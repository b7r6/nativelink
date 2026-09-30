// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// keeper_shim.cpp — C shim over an in-process ClickHouse Keeper.
//
// Design notes (verified against the vendored ClickHouse tree):
//
//  * We drive KeeperDispatcher RAW (registerSession + putRequest) instead of
//    wrapping Coordination::KeeperOverDispatcher, for two reasons:
//      1. ~KeeperOverDispatcher unconditionally calls finishSession
//         (src/Common/ZooKeeper/KeeperOverDispatcher.cpp:77-80), which makes
//         nlk_session_abandon impossible at that layer.
//      2. Its watch parameters throw NOT_IMPLEMENTED
//         (KeeperOverDispatcher.cpp:161-162, 178-179).
//    The request/response plumbing below mirrors KeeperOverDispatcher::pushRequest
//    (KeeperOverDispatcher.cpp:92-103) and KeeperTCPHandler's session callback
//    (src/Server/KeeperTCPHandler.cpp:514-532).
//
//  * Watch events are delivered through the same registered session callback as
//    normal responses, tagged xid == Coordination::WATCH_XID
//    (src/Common/ZooKeeper/ZooKeeperConstants.h:14; see also
//    KeeperTCPHandler.cpp:903 which special-cases that xid). We register a watch
//    by sending an Exists request with has_watch = true
//    (ZooKeeperCommon.h:56) — an exists-watch fires on create, delete and data
//    change, which matches the header's "data + existence" contract.
//
//  * Abandon vs close: KeeperDispatcher::finishSession only unregisters the
//    response callback / live-session entry; ephemeral cleanup happens solely
//    when a Close op commits through raft. The leader's sessionCleanerTask
//    (src/Coordination/KeeperDispatcher.cpp:524-568) detects dead sessions via
//    the state machine's timeout table and pushes the Close itself. So:
//      close   = push ZooKeeperCloseRequest (CLOSE_XID) + finishSession,
//      abandon = stop heartbeats + finishSession only; the server expires the
//                session after timeout_ms and removes its ephemerals.
//
//  * Sessions stay alive only while requests arrive, so each nlk_session runs a
//    heartbeat thread sending ZooKeeperHeartbeatRequest (ZooKeeperCommon.h:104)
//    with xid = PING_XID (ZooKeeperConstants.h:15) at timeout/3, exactly like a
//    live TCP client would.
//
//  * NLK_EV_SESSION_LOST is best-effort: the dispatcher unregisters our callback
//    BEFORE committing the expiry Close (KeeperDispatcher.cpp:556-557), so no
//    ZooKeeperCloseResponse reaches us. We synthesize the event when a heartbeat
//    or operation fails with ZSESSIONEXPIRED / rejected putRequest.
//
//  * Parent creation: nlk_create auto-creates missing intermediate nodes as
//    persistent empty znodes (mkdir -p), retrying the leaf create once.

#include "../include/nativelink-keeper.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <Common/ThreadPool.h>                       // GlobalThreadPool::initialize (programs/keeper/Keeper.cpp:382)
#include <Common/ThreadStatus.h>                     // MainThreadStatus (Keeper.cpp:351)
#include <Common/ZooKeeper/IKeeper.h>                // Error, Stat, Event (IKeeper.h:84,67,796)
#include <Common/ZooKeeper/ZooKeeperCommon.h>        // ZooKeeper*Request/Response
#include <Common/ZooKeeper/ZooKeeperConstants.h>     // WATCH_XID, PING_XID, CLOSE_XID
#include <Coordination/KeeperDispatcher.h>           // putRequest/registerSession/finishSession/getSessionID
#include <Disks/registerDisks.h>                     // registerDisks (Keeper.cpp:441)
#include <Interpreters/Context.h>                    // Context::createShared/createGlobal (Keeper.cpp:430-435)

namespace
{

thread_local std::string g_last_error;

void set_last_error(std::string msg) { g_last_error = std::move(msg); }

nlk_rc map_error(Coordination::Error err)
{
    using E = Coordination::Error;
    switch (err)
    {
        case E::ZOK: return NLK_OK;
        case E::ZNONODE: return NLK_NO_NODE;
        case E::ZNODEEXISTS: return NLK_NODE_EXISTS;
        case E::ZBADVERSION: return NLK_BAD_VERSION;
        case E::ZSESSIONEXPIRED:
        case E::ZSESSIONMOVED: return NLK_SESSION_EXPIRED;
        case E::ZOPERATIONTIMEOUT: return NLK_TIMEOUT;
        default:
            // errorMessage: src/Common/ZooKeeper/IKeeper.h:139
            set_last_error(Coordination::errorMessage(err));
            return NLK_ERR;
    }
}

// Deterministic raft port for single-node operation. KeeperServer always binds
// the raft_configuration port even with one server; port 0 is not interpreted
// as "ephemeral" by nuraft's asio listener, so pick a stable pseudo-random
// port derived from the storage path (collision => nlk_server_start fails and
// the caller retries with a different dir).
uint16_t raft_port_for(const std::string & dir)
{
    return static_cast<uint16_t>(20000 + (std::hash<std::string>{}(dir) % 20000));
}

std::once_flag g_process_init_once;

void process_wide_init()
{
    // Mirrors programs/keeper/Keeper.cpp:351,382,441. GlobalThreadPool must be
    // sized for nuraft + keeper workers.
    DB::MainThreadStatus::getInstance();
    GlobalThreadPool::initialize(/*max_threads=*/1000, /*max_free_threads=*/100, /*queue_size=*/1000);
    DB::registerDisks(/*global_skip_access_check=*/false);
}

} // namespace

struct nlk_server
{
    DB::SharedContextHolder shared_context;
    DB::ContextMutablePtr global_context;
    std::shared_ptr<DB::KeeperDispatcher> dispatcher;
};

struct nlk_watch
{
    nlk_session * session = nullptr;
    std::string path;
    nlk_watch_cb cb = nullptr;
    void * ctx = nullptr;
    std::atomic<bool> cancelled{false};
};

struct nlk_session
{
    nlk_server * server = nullptr;
    int64_t session_id = 0;
    uint32_t timeout_ms = 0;

    // Response routing, modeled on KeeperOverDispatcher::CallbackState
    // (KeeperOverDispatcher.h:139-146): the dispatcher invokes our registered
    // callback from its response thread; we match by xid.
    struct State
    {
        std::mutex mutex;
        std::unordered_map<Coordination::XID, std::function<void(const Coordination::ZooKeeperResponsePtr &)>> callbacks;
        // Live watches keyed by path (one-shot; removed on fire).
        std::unordered_multimap<std::string, std::shared_ptr<nlk_watch>> watches;
        std::atomic<bool> expired{false};
    };
    std::shared_ptr<State> state = std::make_shared<State>();
    std::atomic<Coordination::XID> next_xid{1};

    // Heartbeat machinery.
    std::thread heartbeat_thread;
    std::mutex hb_mutex;
    std::condition_variable hb_cv;
    bool hb_stop = false;

    void fire_session_lost()
    {
        if (state->expired.exchange(true))
            return;
        std::vector<std::shared_ptr<nlk_watch>> to_fire;
        {
            std::lock_guard lock(state->mutex);
            for (auto & [path, w] : state->watches)
                to_fire.push_back(w);
            state->watches.clear();
            state->callbacks.clear();
        }
        for (auto & w : to_fire)
            if (!w->cancelled.load())
                w->cb(w->ctx, NLK_EV_SESSION_LOST, w->path.c_str());
    }

    // Push a request and register a completion callback for its xid.
    // Mirrors KeeperOverDispatcher::pushRequest (KeeperOverDispatcher.cpp:92-103).
    bool push(Coordination::ZooKeeperRequestPtr request,
              std::function<void(const Coordination::ZooKeeperResponsePtr &)> callback)
    {
        request->xid = next_xid++;
        {
            std::lock_guard lock(state->mutex);
            state->callbacks[request->xid] = std::move(callback);
        }
        // putRequest: src/Coordination/KeeperDispatcher.h:206. Returns false if
        // the session is no longer accepting requests.
        if (!server->dispatcher->putRequest(request, session_id, /*use_xid_64=*/false))
        {
            {
                std::lock_guard lock(state->mutex);
                state->callbacks.erase(request->xid);
            }
            fire_session_lost();
            return false;
        }
        return true;
    }

    // Synchronous request helper: block up to the session timeout.
    // Returns NLK_TIMEOUT / NLK_SESSION_EXPIRED on transport failure, else NLK_OK
    // with *out set to the raw response (whose ->error still needs mapping).
    nlk_rc push_sync(Coordination::ZooKeeperRequestPtr request, Coordination::ZooKeeperResponsePtr * out)
    {
        if (state->expired.load())
        {
            set_last_error("session expired");
            return NLK_SESSION_EXPIRED;
        }
        auto promise = std::make_shared<std::promise<Coordination::ZooKeeperResponsePtr>>();
        auto future = promise->get_future();
        if (!push(request, [promise](const Coordination::ZooKeeperResponsePtr & r) { promise->set_value(r); }))
        {
            set_last_error("session disconnected");
            return NLK_SESSION_EXPIRED;
        }
        if (future.wait_for(std::chrono::milliseconds(timeout_ms)) != std::future_status::ready)
        {
            set_last_error("operation timed out");
            return NLK_TIMEOUT;
        }
        *out = future.get();
        return NLK_OK;
    }
};

/* ---- server lifecycle ------------------------------------------------ */

namespace
{

struct EnsembleMember
{
    uint32_t id;
    std::string host;
    uint16_t port;
};

// Parse "id=host:port,id=host:port,...".
bool parse_ensemble(const std::string & spec, std::vector<EnsembleMember> & out)
{
    std::istringstream ss(spec);
    std::string entry;
    while (std::getline(ss, entry, ','))
    {
        if (entry.empty())
            continue;
        auto eq = entry.find('=');
        auto colon = entry.rfind(':');
        if (eq == std::string::npos || colon == std::string::npos || colon < eq)
        {
            set_last_error("malformed ensemble entry: " + entry);
            return false;
        }
        try
        {
            EnsembleMember m;
            m.id = static_cast<uint32_t>(std::stoul(entry.substr(0, eq)));
            m.host = entry.substr(eq + 1, colon - eq - 1);
            m.port = static_cast<uint16_t>(std::stoul(entry.substr(colon + 1)));
            out.push_back(std::move(m));
        }
        catch (const std::exception &)
        {
            set_last_error("malformed ensemble entry: " + entry);
            return false;
        }
    }
    if (out.empty())
        set_last_error("empty ensemble");
    return !out.empty();
}

} // namespace

nlk_server * nlk_server_start(const char * storage_dir, uint32_t tick_ms)
{
    // Single-node special case: an ensemble of one — ourselves — on the
    // deterministic derived port.
    std::string spec = "1=localhost:" + std::to_string(raft_port_for(storage_dir));
    return nlk_server_start_ensemble(storage_dir, tick_ms, 1, spec.c_str());
}

nlk_server * nlk_server_start_ensemble(const char * storage_dir, uint32_t tick_ms,
                                       uint32_t my_id, const char * ensemble)
{
    try
    {
        std::call_once(g_process_init_once, process_wide_init);

        std::string dir(storage_dir);
        std::filesystem::create_directories(dir);

        if (tick_ms == 0)
            tick_ms = 500;

        std::vector<EnsembleMember> members;
        if (!parse_ensemble(ensemble, members))
            return nullptr;
        bool my_id_present = false;
        for (const auto & m : members)
            my_id_present |= (m.id == my_id);
        if (!my_id_present)
        {
            // KeeperStateManager::parseServersConfiguration requires our own id
            // in the member list ("5. Our ID present in hostnames list",
            // vendor/clickhouse/src/Coordination/KeeperStateManager.cpp:158;
            // the entry whose <id> == <server_id> supplies the local bind port).
            set_last_error("my_id not present in ensemble");
            return nullptr;
        }

        // In-memory config; schema mirrors programs/keeper/keeper_config.xml:30-63
        // and is parsed by KeeperStateManager::parseServersConfiguration
        // (vendor/clickhouse/src/Coordination/KeeperStateManager.cpp:162-198:
        // keeper_server.raft_configuration.server -> <id>/<hostname>/<port>).
        // No <tcp_port>: we never start the TCP servers (that is Keeper.cpp's
        // server loop, which we deliberately skip).
        std::ostringstream xml;
        xml << "<clickhouse>"
               "<logger><level>warning</level><console>false</console></logger>"
               "<path>" << dir << "</path>"
               "<keeper_server>"
               "<server_id>" << my_id << "</server_id>"
               "<storage_path>" << dir << "</storage_path>"
               "<log_storage_path>" << dir << "/logs</log_storage_path>"
               "<snapshot_storage_path>" << dir << "/snapshots</snapshot_storage_path>"
               "<coordination_settings>"
               // Setting names: src/Coordination/CoordinationSettings.cpp:28-40.
               "<min_session_timeout_ms>1</min_session_timeout_ms>"
               "<session_timeout_ms>3600000</session_timeout_ms>"
               "<operation_timeout_ms>10000</operation_timeout_ms>"
               "<dead_session_check_period_ms>" << tick_ms << "</dead_session_check_period_ms>"
               "<heart_beat_interval_ms>" << tick_ms << "</heart_beat_interval_ms>"
               "<election_timeout_lower_bound_ms>" << tick_ms * 4 << "</election_timeout_lower_bound_ms>"
               "<election_timeout_upper_bound_ms>" << tick_ms * 8 << "</election_timeout_upper_bound_ms>"
               "<raft_logs_level>warning</raft_logs_level>"
               "</coordination_settings>"
               "<hostname_checks_enabled>false</hostname_checks_enabled>"
               "<raft_configuration>";
        // Multiple sibling <server> elements surface to Poco config keys as
        // "server", "server[1]", ... which is exactly what
        // KeeperStateManager iterates via config.keys(".raft_configuration")
        // (KeeperStateManager.cpp:167).
        for (const auto & m : members)
            xml << "<server><id>" << m.id << "</id><hostname>" << m.host
                << "</hostname><port>" << m.port << "</port></server>";
        xml << "</raft_configuration>"
               "</keeper_server>"
               "</clickhouse>";

        std::istringstream stream(xml.str());
        Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration);
        config->load(stream);

        auto srv = std::make_unique<nlk_server>();

        // Boot sequence: programs/keeper/Keeper.cpp:430-435 then
        // global_context->initializeKeeperDispatcher(false)
        // (Keeper.cpp:476; impl at src/Interpreters/Context.cpp:6436 —
        // standalone_keeper is inferred from ApplicationType::KEEPER).
        srv->shared_context = DB::Context::createShared();
        srv->global_context = DB::Context::createGlobal(srv->shared_context.get());
        srv->global_context->makeGlobalContext();
        srv->global_context->setApplicationType(DB::Context::ApplicationType::KEEPER);
        srv->global_context->setPath(dir + "/");
        srv->global_context->setConfig(config); // Context.h:918

        srv->global_context->initializeKeeperDispatcher(/*start_async=*/false);
        srv->dispatcher = srv->global_context->getKeeperDispatcher(); // Context.h:1582

        return srv.release();
    }
    catch (const std::exception & e)
    {
        set_last_error(e.what());
        return nullptr;
    }
    catch (...)
    {
        set_last_error("unknown error starting keeper");
        return nullptr;
    }
}

void nlk_server_shutdown(nlk_server * srv)
{
    if (!srv)
        return;
    try
    {
        // Shutdown ordering per programs/keeper/Keeper.cpp:714-741 (no TCP
        // handlers exist, so "all connections closed" holds trivially).
        srv->dispatcher.reset();
        srv->global_context->signalKeeperDispatcherShutdown();
        srv->global_context->shutdownKeeperDispatcherBeforeConnectionsFinish();
        srv->global_context->shutdownKeeperDispatcherAfterConnectionsFinish(/*closed_all_connections=*/true);
        srv->global_context->shutdown();
    }
    catch (...)
    {
        // Best effort; nothing actionable for the caller during shutdown.
    }
    delete srv;
}

/* ---- sessions -------------------------------------------------------- */

nlk_session * nlk_session_create(nlk_server * srv, uint32_t timeout_ms)
{
    try
    {
        auto sess = std::make_unique<nlk_session>();
        sess->server = srv;
        sess->timeout_ms = timeout_ms;
        // getSessionID: KeeperDispatcher.h:209 (same call KeeperOverDispatcher's
        // ctor makes, KeeperOverDispatcher.cpp:29).
        sess->session_id = srv->dispatcher->getSessionID(timeout_ms);

        // Session response callback; shape per KeeperOverDispatcher.cpp:38-67.
        // Captures State by shared_ptr so it stays valid after session teardown.
        auto state = sess->state;
        auto session_cb = [state](const Coordination::ZooKeeperResponsePtr & response,
                                  Coordination::ZooKeeperRequestPtr /*request*/) -> bool
        {
            if (response->xid == Coordination::WATCH_XID)
            {
                // Watch delivery path; xid tag per ZooKeeperConstants.h:14 and
                // KeeperTCPHandler.cpp:903.
                const auto & watch = dynamic_cast<const Coordination::ZooKeeperWatchResponse &>(*response);
                nlk_event ev;
                switch (watch.type) // Coordination::Event, IKeeper.h:796-804
                {
                    case Coordination::Event::CREATED: ev = NLK_EV_CREATED; break;
                    case Coordination::Event::DELETED: ev = NLK_EV_DELETED; break;
                    case Coordination::Event::CHANGED: ev = NLK_EV_CHANGED; break;
                    case Coordination::Event::SESSION: ev = NLK_EV_SESSION_LOST; break;
                    default: return false;
                }
                std::vector<std::shared_ptr<nlk_watch>> fired;
                {
                    std::lock_guard lock(state->mutex);
                    auto [begin, end] = state->watches.equal_range(watch.path);
                    for (auto it = begin; it != end; ++it)
                        fired.push_back(it->second);
                    state->watches.erase(begin, end); // one-shot
                }
                for (auto & w : fired)
                    if (!w->cancelled.load())
                        w->cb(w->ctx, ev, watch.path.c_str());
                return false;
            }

            if (dynamic_cast<const Coordination::ZooKeeperCloseResponse *>(response.get()))
            {
                state->expired = true;
                return false;
            }

            std::function<void(const Coordination::ZooKeeperResponsePtr &)> cb;
            {
                std::lock_guard lock(state->mutex);
                auto it = state->callbacks.find(response->xid);
                if (it != state->callbacks.end())
                {
                    cb = std::move(it->second);
                    state->callbacks.erase(it);
                }
            }
            if (cb)
                cb(response);
            return false;
        };
        // registerSession: KeeperDispatcher.h:213.
        srv->dispatcher->registerSession(sess->session_id, session_cb);

        // Heartbeat thread: keeps the server-side session alive (state machine
        // expiry is driven by request activity; a TCP client would ping).
        nlk_session * raw = sess.get();
        sess->heartbeat_thread = std::thread([raw]
        {
            const auto interval = std::chrono::milliseconds(std::max<uint32_t>(1, raw->timeout_ms / 3));
            std::unique_lock lock(raw->hb_mutex);
            while (!raw->hb_cv.wait_for(lock, interval, [raw] { return raw->hb_stop; }))
            {
                auto ping = std::make_shared<Coordination::ZooKeeperHeartbeatRequest>();
                ping->xid = Coordination::PING_XID; // ZooKeeperConstants.h:15
                if (!raw->server->dispatcher->putRequest(ping, raw->session_id, false))
                {
                    raw->fire_session_lost();
                    return;
                }
            }
        });

        return sess.release();
    }
    catch (const std::exception & e)
    {
        set_last_error(e.what());
        return nullptr;
    }
}

namespace
{

void stop_heartbeat(nlk_session * sess)
{
    {
        std::lock_guard lock(sess->hb_mutex);
        sess->hb_stop = true;
    }
    sess->hb_cv.notify_all();
    if (sess->heartbeat_thread.joinable())
        sess->heartbeat_thread.join();
}

} // namespace

void nlk_session_close(nlk_session * sess)
{
    if (!sess)
        return;
    stop_heartbeat(sess);
    try
    {
        if (!sess->state->expired.load())
        {
            // Graceful close: same op the session cleaner issues
            // (KeeperDispatcher.cpp:545-557) — Close commits through raft and
            // removes the session's ephemerals.
            auto close = std::make_shared<Coordination::ZooKeeperCloseRequest>(); // ZooKeeperCommon.h:223
            close->xid = Coordination::CLOSE_XID; // ZooKeeperConstants.h:17
            sess->server->dispatcher->putRequest(close, sess->session_id, false);
        }
        sess->server->dispatcher->finishSession(sess->session_id); // KeeperDispatcher.h:216
    }
    catch (...) {}
    delete sess;
}

void nlk_session_abandon(nlk_session * sess)
{
    if (!sess)
        return;
    stop_heartbeat(sess);
    try
    {
        // NO Close request: ephemerals survive until the leader's
        // sessionCleanerTask (KeeperDispatcher.cpp:524-568) sees the session
        // exceed its timeout and commits the Close itself. finishSession only
        // unregisters our response callback; it does not touch ephemerals.
        sess->server->dispatcher->finishSession(sess->session_id);
    }
    catch (...) {}
    delete sess;
}

int64_t nlk_session_id(const nlk_session * sess)
{
    return sess ? sess->session_id : 0;
}

/* ---- znodes ---------------------------------------------------------- */

namespace
{

nlk_rc do_create(nlk_session * sess, const std::string & path,
                 const uint8_t * data, size_t len, bool ephemeral)
{
    // Request shape per KeeperOverDispatcher::create (KeeperOverDispatcher.cpp:105-124).
    // Empty acls = open access on the in-process keeper (same as that path).
    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request->path = path;
    request->data.assign(reinterpret_cast<const char *>(data), len);
    request->is_ephemeral = ephemeral;
    request->is_sequential = false;

    Coordination::ZooKeeperResponsePtr response;
    if (nlk_rc rc = sess->push_sync(request, &response); rc != NLK_OK)
        return rc;
    return map_error(response->error);
}

} // namespace

nlk_rc nlk_create(nlk_session * sess, const char * path,
                  const uint8_t * data, size_t len, bool ephemeral)
{
    std::string p(path);
    nlk_rc rc = do_create(sess, p, data, len, ephemeral);
    if (rc != NLK_NO_NODE)
        return rc;

    // Missing parent: mkdir -p intermediate persistent empty nodes, then retry.
    for (size_t pos = p.find('/', 1); pos != std::string::npos; pos = p.find('/', pos + 1))
    {
        nlk_rc prc = do_create(sess, p.substr(0, pos), nullptr, 0, /*ephemeral=*/false);
        if (prc != NLK_OK && prc != NLK_NODE_EXISTS)
            return prc;
    }
    return do_create(sess, p, data, len, ephemeral);
}

nlk_rc nlk_delete(nlk_session * sess, const char * path, int32_t version)
{
    // KeeperOverDispatcher::remove (KeeperOverDispatcher.cpp:126-139).
    auto request = std::make_shared<Coordination::ZooKeeperRemoveRequest>();
    request->path = path;
    request->version = version;

    Coordination::ZooKeeperResponsePtr response;
    if (nlk_rc rc = sess->push_sync(request, &response); rc != NLK_OK)
        return rc;
    return map_error(response->error);
}

nlk_rc nlk_get(nlk_session * sess, const char * path,
               uint8_t ** out_data, size_t * out_len, int32_t * out_version)
{
    // KeeperOverDispatcher::get (KeeperOverDispatcher.cpp:173-188).
    auto request = std::make_shared<Coordination::ZooKeeperGetRequest>();
    request->path = path;

    Coordination::ZooKeeperResponsePtr response;
    if (nlk_rc rc = sess->push_sync(request, &response); rc != NLK_OK)
        return rc;
    if (nlk_rc rc = map_error(response->error); rc != NLK_OK)
        return rc;

    // GetResponse: data + stat (IKeeper.h:535-541); Stat::version (IKeeper.h:73).
    const auto & get = dynamic_cast<const Coordination::GetResponse &>(*response);
    *out_len = get.data.size();
    *out_data = static_cast<uint8_t *>(std::malloc(get.data.size() ? get.data.size() : 1));
    if (!*out_data)
    {
        set_last_error("out of memory");
        return NLK_ERR;
    }
    std::memcpy(*out_data, get.data.data(), get.data.size());
    *out_version = get.stat.version;
    return NLK_OK;
}

nlk_rc nlk_set(nlk_session * sess, const char * path,
               const uint8_t * data, size_t len, int32_t expected_version,
               int32_t * out_new_version)
{
    // KeeperOverDispatcher::set (KeeperOverDispatcher.cpp:238-253); version -1
    // is ZK's "unconditional", matching the header contract.
    auto request = std::make_shared<Coordination::ZooKeeperSetRequest>();
    request->path = path;
    request->data.assign(reinterpret_cast<const char *>(data), len);
    request->version = expected_version;

    Coordination::ZooKeeperResponsePtr response;
    if (nlk_rc rc = sess->push_sync(request, &response); rc != NLK_OK)
        return rc;
    if (nlk_rc rc = map_error(response->error); rc != NLK_OK)
        return rc;
    if (out_new_version)
        *out_new_version = dynamic_cast<const Coordination::SetResponse &>(*response).stat.version;
    return NLK_OK;
}

nlk_rc nlk_exists(nlk_session * sess, const char * path, int32_t * out_version)
{
    // KeeperOverDispatcher::exists (KeeperOverDispatcher.cpp:156-171).
    auto request = std::make_shared<Coordination::ZooKeeperExistsRequest>();
    request->path = path;

    Coordination::ZooKeeperResponsePtr response;
    if (nlk_rc rc = sess->push_sync(request, &response); rc != NLK_OK)
        return rc;
    if (nlk_rc rc = map_error(response->error); rc != NLK_OK)
        return rc;
    if (out_version)
        *out_version = dynamic_cast<const Coordination::ExistsResponse &>(*response).stat.version;
    return NLK_OK;
}

/* ---- watches --------------------------------------------------------- */

nlk_watch * nlk_watch_subscribe(nlk_session * sess, const char * path,
                                nlk_watch_cb cb, void * ctx)
{
    auto watch = std::make_shared<nlk_watch>();
    watch->session = sess;
    watch->path = path;
    watch->cb = cb;
    watch->ctx = ctx;

    {
        std::lock_guard lock(sess->state->mutex);
        sess->state->watches.emplace(watch->path, watch);
    }

    // Register server-side via Exists with has_watch (ZooKeeperCommon.h:56).
    // An exists-watch covers creation, deletion and data change of `path`.
    auto request = std::make_shared<Coordination::ZooKeeperExistsRequest>();
    request->path = path;
    request->has_watch = true;

    Coordination::ZooKeeperResponsePtr response;
    nlk_rc rc = sess->push_sync(request, &response);
    // ZNONODE is fine: the watch is still armed for creation. Transport-level
    // failure means no watch got registered.
    if (rc != NLK_OK)
    {
        std::lock_guard lock(sess->state->mutex);
        auto [begin, end] = sess->state->watches.equal_range(watch->path);
        for (auto it = begin; it != end; ++it)
            if (it->second.get() == watch.get()) { sess->state->watches.erase(it); break; }
        return nullptr;
    }

    // The opaque handle owns one extra shared_ptr ref; released in cancel.
    return reinterpret_cast<nlk_watch *>(new std::shared_ptr<nlk_watch>(watch));
}

void nlk_watch_cancel(nlk_watch * handle)
{
    if (!handle)
        return;
    auto * sp = reinterpret_cast<std::shared_ptr<nlk_watch> *>(handle);
    nlk_watch * w = sp->get();
    w->cancelled = true;
    // Remove from the session registry; the server-side watch may still fire
    // but the callback is suppressed via `cancelled`.
    auto & state = *w->session->state;
    {
        std::lock_guard lock(state.mutex);
        auto [begin, end] = state.watches.equal_range(w->path);
        for (auto it = begin; it != end; ++it)
            if (it->second.get() == w) { state.watches.erase(it); break; }
    }
    delete sp;
}

/* ---- misc ------------------------------------------------------------ */

void nlk_free(void * p)
{
    std::free(p);
}

const char * nlk_last_error(void)
{
    return g_last_error.c_str();
}

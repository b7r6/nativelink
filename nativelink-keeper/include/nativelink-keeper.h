/* Copyright 2026 The NativeLink Authors. All rights reserved.
 *
 * nativelink-keeper.h — minimal C surface over an in-process ClickHouse
 * Keeper (NuRaft) instance. Deliberately small; iterate later.
 *
 * Threading: all calls are thread-safe. Callbacks fire on keeper's
 * dispatcher thread — do not block in them.
 * Ownership: char* out-params are malloc'd by the shim; free with
 * nlk_free(). All handles are opaque.
 */
#ifndef NATIVELINK_KEEPER_H
#define NATIVELINK_KEEPER_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct nlk_server nlk_server;      /* the embedded keeper        */
typedef struct nlk_session nlk_session;    /* one client session         */
typedef struct nlk_watch nlk_watch;        /* a registered watch         */

typedef enum nlk_rc {
  NLK_OK = 0,
  NLK_ERR = 1,            /* generic failure; see nlk_last_error()      */
  NLK_NO_NODE = 2,
  NLK_NODE_EXISTS = 3,
  NLK_BAD_VERSION = 4,    /* CAS conflict                               */
  NLK_SESSION_EXPIRED = 5,
  NLK_TIMEOUT = 6,
} nlk_rc;

typedef enum nlk_event {
  NLK_EV_CREATED = 1,
  NLK_EV_DELETED = 2,
  NLK_EV_CHANGED = 3,
  NLK_EV_SESSION_LOST = 4,
} nlk_event;

/* ---- server lifecycle ------------------------------------------------ */

/* Single-node raft on local storage. `storage_dir` must exist.
 * `tick_ms` is the coordination heartbeat (session expiry granularity). */
nlk_server *nlk_server_start(const char *storage_dir, uint32_t tick_ms);

/* Multi-node raft. `my_id` is this member's server id; `ensemble` is a
 * comma-separated list "id=host:port,id=host:port,..." naming EVERY
 * member including this one (this member's entry determines the local
 * raft bind port). A quorum of members must be reachable before the
 * server reports started. */
nlk_server *nlk_server_start_ensemble(const char *storage_dir, uint32_t tick_ms,
                                      uint32_t my_id, const char *ensemble);
void nlk_server_shutdown(nlk_server *);

/* ---- sessions -------------------------------------------------------- */

/* `timeout_ms`: session expires this long after last heartbeat/op.     */
nlk_session *nlk_session_create(nlk_server *, uint32_t timeout_ms);
/* Graceful close: ephemerals owned by the session are removed.          */
void nlk_session_close(nlk_session *);
/* Simulated death for tests: drop the session WITHOUT graceful close;
 * ephemerals must disappear after the server expires it.                */
void nlk_session_abandon(nlk_session *);
int64_t nlk_session_id(const nlk_session *);

/* ---- znodes ---------------------------------------------------------- */

nlk_rc nlk_create(nlk_session *, const char *path,
                  const uint8_t *data, size_t len, bool ephemeral);
nlk_rc nlk_delete(nlk_session *, const char *path, int32_t version /* -1 = any */);

/* On NLK_OK: *out_data/*out_len malloc'd (nlk_free), *out_version set.  */
nlk_rc nlk_get(nlk_session *, const char *path,
               uint8_t **out_data, size_t *out_len, int32_t *out_version);

/* CAS write: succeeds iff current version == expected_version.
 * expected_version == -1 means unconditional.                           */
nlk_rc nlk_set(nlk_session *, const char *path,
               const uint8_t *data, size_t len, int32_t expected_version,
               int32_t *out_new_version);

/* Existence probe that does not allocate.                               */
nlk_rc nlk_exists(nlk_session *, const char *path, int32_t *out_version);

/* ---- watches --------------------------------------------------------- */

typedef void (*nlk_watch_cb)(void *ctx, nlk_event ev, const char *path);

/* One-shot ZK-style watch on `path` (data + existence). Re-arm by
 * re-subscribing from the callback if desired.                          */
nlk_watch *nlk_watch_subscribe(nlk_session *, const char *path,
                               nlk_watch_cb cb, void *ctx);
void nlk_watch_cancel(nlk_watch *);

/* ---- misc ------------------------------------------------------------ */

void nlk_free(void *);
/* Thread-local description of the last NLK_ERR.                         */
const char *nlk_last_error(void);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* NATIVELINK_KEEPER_H */

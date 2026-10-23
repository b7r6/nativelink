# Copyright 2026 The NativeLink Authors. All rights reserved.
# Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future
# License. See LICENSE for details.

"""Reproduce the private publication snapshot bound; not a state generator.

Shapes deliberately combine incompatible maxima. Field inventory checks detect
added/removed struct fields, but do not prove their Rust types or invariants.
Review those premises in publication-snapshot-bound.md when source changes.
"""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AUTH = ROOT / "src/authority"
CANDIDATE = ROOT / "tests/publication_candidate"
RETENTION = ROOT / "tests/retention"
U64 = 2**64 - 1
PAYLOAD = 4096


def encoded(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def nested(value):
    # Every inner byte becomes decimal digits followed by a comma in Vec<u8>.
    # The enclosing array needs one extra byte after this sum, in header slack.
    return sum(1 + len(str(byte)) for byte in encoded(value))


def fields(path, name, expected):
    source = path.read_text()
    declaration = re.search(r"struct\s+" + name + r"(?:<[^>]*>)?\s*\{([^}]+)\}", source)
    assert declaration is not None, (path, name)
    actual = set(
        re.findall(
            r"^\s*(?:pub(?:\([^)]*\))?\s+)?(\w+)\s*:", declaration[1], re.MULTILINE
        )
    )
    assert actual == set(expected), (path, name, actual ^ set(expected))


for constant, value in {
    "MAX_STAGES": 8,
    "MAX_OPERATIONS": 16,
    "MAX_PHYSICAL": 64,
    "MAX_MEMBERS": 8,
    "MAX_GRANTS": 16,
    "MAX_PAYLOAD": PAYLOAD,
    "LEASE_TICKS": 100,
    "STAGE_TICKS": 10,
}.items():
    assert re.search(
        r"const " + constant + r": \w+ = " + str(value) + r";",
        (CANDIDATE / "types.rs").read_text(),
    )
assert "pub const MAX_ATTEMPTS: u32 = 3;" in (AUTH / "types.rs").read_text()
assert (
    "const MAX_SNAPSHOT: usize = 2 * 1024 * 1024;"
    in (CANDIDATE / "types.rs").read_text()
)

resources = {"cpu_millis": U64, "memory_bytes": U64, "slots": U64}
terminal = {"ResourceFailed": {"result": None, "reason": "AttemptBudgetExhausted"}}
deadline = {"at": U64, "generation": U64}
scheduler = {
    "operation": {
        "view": {
            "id": U64,
            "execution": U64,
            "priority": -(2**31),
            "interested": False,
            "terminal": terminal,
            "version": U64,
            "interest_deadline": deadline,
        },
        "original_request": None,
    },
    "execution": {
        "id": U64,
        "request": None,
        "reservation_target": resources,
        "phase": "Dispatching",
        "effective_priority": -(2**31),
        "enqueue_ordinal": U64,
        "enqueue_at": U64,
        "hold_cooldown_until": U64,
        "attempt": U64,
        "attempts_started": 3,
        "terminal": terminal,
        "terminal_at": U64,
    },
    "worker": {
        "id": U64,
        "incarnation": U64,
        "session": U64,
        "properties": None,
        "capacity": resources,
        "available": resources,
        "draining": False,
        "last_heartbeat": U64,
        "charged": resources,
        "quarantined": False,
    },
    "hold": {
        "execution": U64,
        "worker": U64,
        "incarnation": U64,
        "started_at": U64,
        "until": U64,
    },
    "attempt": {
        "id": U64,
        "dispatch_effect": U64,
        "execution": U64,
        "worker": U64,
        "incarnation": U64,
        "reservation": U64,
        "authorized": False,
        "phase": "Dispatching",
        "progress": U64,
        "deadline": deadline,
        "running_deadline": U64,
    },
    "reservation": {
        "id": U64,
        "attempt": U64,
        "worker": U64,
        "incarnation": U64,
        "charged": resources,
        "released": False,
    },
    "effect": {
        "id": U64,
        "epoch": U64,
        "kind": {
            "Dispatch": {
                "execution": U64,
                "attempt": U64,
                "worker": U64,
                "incarnation": U64,
                "reservation": U64,
                "charged": resources,
                "request": None,
            }
        },
    },
}
counts = {
    "operation": 16,
    "execution": 8,
    "worker": 8,
    "hold": 8,
    "attempt": 24,
    "reservation": 24,
    "effect": 24,
}
placeholders = {"operation": 2, "execution": 2, "worker": 1, "effect": 1}
fields(AUTH / "mod.rs", "Operation", scheduler["operation"])
fields(AUTH / "types.rs", "OperationView", scheduler["operation"]["view"])
for row, name in {
    "execution": "ExecutionView",
    "worker": "WorkerView",
    "hold": "HoldView",
    "attempt": "AttemptView",
    "reservation": "ReservationView",
    "effect": "Effect",
}.items():
    fields(AUTH / "types.rs", name, scheduler[row])
fields(AUTH / "types.rs", "Resources", resources)
fields(AUTH / "types.rs", "Deadline", deadline)
scheduler_rows = {
    name: {
        "count": counts[name],
        "fixed_encoded_bytes": nested({str(U64): row})
        - placeholders.get(name, 0) * nested(None)
        + 4,
    }
    for name, row in scheduler.items()
}
# 24 original requests; 8 dispatch/lookup OR execution-result payloads;
# 16 operation results; 8 worker property maps. No PublishResult escapes.
scheduler_variables = (24 + 8 + 16 + 8) * PAYLOAD * 4
scheduler_fixed = (
    sum(row["count"] * row["fixed_encoded_bytes"] for row in scheduler_rows.values())
    + 4096
)
scheduler_total = scheduler_fixed + scheduler_variables

key = {"incarnation": 64, "generation": U64}
cohort = {
    "phase": "Allocating",
    "fence": U64,
    "captured": [],
    "cursor": 112,
    "marked": [],
}
generation = {
    "logical_object": 64,
    "cohort": 64,
    "ready": False,
    "retired": False,
    "write_revision": 128,
    "current_delete": None,
    "delete_confirmed_revision": None,
}
root = lambda members, kind: [
    112,
    {"lease": 1, "kind": kind, "closure": [key] * members, "until": U64},
]
write = [128, {"key": key, "outcome": False}]
for name, shape in {
    "Key": key,
    "Cohort": cohort,
    "Generation": generation,
    "Root": root(8, "Publication")[1],
    "Write": write[1],
}.items():
    fields(RETENTION / "protocol.rs", name, shape)
retention_parts = {
    "cohorts": 64 * (nested([64, cohort]) + 4),
    "generations": 64 * (nested([key, generation]) + 4),
    "roots": 64 * (nested(root(1, "Staging")) + 4)
    + 8 * (nested(root(8, "Staging")) + 4)
    + 8 * (nested(root(8, "Publication")) + 4)
    + 16 * (nested(root(8, "Operation")) + 4)
    + 16 * (nested(root(8, "Serving")) + 4),
    "writes": 128 * (nested(write) + 4),
    "header": 4096,
}
retention_total = sum(retention_parts.values())

label = "\x00" * 256
object_id = {"algorithm": 9, "hash": [255] * 32, "bytes": 4096}
physical_key = {"namespace": [255] * 16, "incarnation": [255] * 16, "generation": U64}
member = {"object": object_id, "key": physical_key, "role": "Directory"}
origin = {
    "epoch": U64,
    "action": label,
    "execution": U64,
    "attempt": U64,
    "worker": U64,
    "incarnation": U64,
}
metadata = {
    "namespace": {"namespace": [255] * 16, "incarnation": [255] * 16, "handle": 64},
    "physical": {
        "key": physical_key,
        "object": object_id,
        "logical": 64,
        "bootstrap": [112, 128],
    },
    "credit": {
        "closed": True,
        "physical_remaining": 8,
        "stage": U64,
        "prior_stages": [U64] * 2,
    },
    "stage": {
        "candidate": {
            "action": label,
            "publisher": {
                "Worker": {
                    "execution": U64,
                    "attempt": U64,
                    "worker": U64,
                    "incarnation": U64,
                }
            },
            "result": object_id,
            "result_bytes": None,
            "response": None,
            "closure": [member] * 8,
        },
        "authorized_at": U64,
        "until": U64,
        "root": 112,
        "writes": {str(U64 - i): physical_key for i in range(8)},
        "internal_writes": {str(U64 - i): 128 for i in range(8)},
        "outcomes": {str(U64 - i): False for i in range(8)},
        "preparation": {
            "stage": U64,
            "catalogue_revision": U64,
            "closure": [member] * 8,
            "fences": {str(64 - i): U64 for i in range(8)},
            "until": U64,
        },
        "state": {"Accepted": U64},
    },
    "accepted": {
        "stage": U64,
        "commit": U64,
        "at": U64,
        "eligible": False,
        "root": 112,
        "until": U64,
    },
    "operation": {
        "execution": U64,
        "action": label,
        "until": U64,
        "publication": U64,
        "root": 112,
        "cached": False,
        "payload": None,
    },
    "grant": {"publication": U64, "root": 112, "until": U64},
    "authorization": {
        "label": label,
        "binding": None,
        "issued_at": U64,
        "revoked_at": U64,
    },
    "report": {
        "report": {
            "origin": origin,
            "kind": {
                "MemoryFailure": {
                    "charged": resources,
                    "peak_memory_bytes": U64,
                    "quiescent": False,
                }
            },
            "result": None,
        },
        "position": U64,
        "at": U64,
        "disposition": {"Terminal": terminal},
    },
    "no_graph": {
        "report_id": U64,
        "origin": origin,
        "terminal": terminal,
        "position": U64,
        "at": U64,
    },
}
metadata_counts = {
    "namespace": 64,
    "physical": 64,
    "credit": 8,
    "stage": 8,
    "accepted": 8,
    "operation": 16,
    "grant": 16,
    "authorization": 8,
    "report": 24,
    "no_graph": 8,
}
metadata_placeholders = {
    "stage": 2,
    "operation": 1,
    "authorization": 1,
    "report": 2,
    "no_graph": 1,
}
for row, name in {
    "namespace": "NamespaceBinding",
    "physical": "Physical",
    "credit": "Credit",
}.items():
    fields(CANDIDATE / "engine.rs", name, metadata[row])
for name, shape in {
    "Object": object_id,
    "Key": physical_key,
    "Member": member,
    "Candidate": metadata["stage"]["candidate"],
    "Evidence": metadata["stage"]["preparation"],
    "Stage": metadata["stage"],
    "Accepted": metadata["accepted"],
    "Operation": metadata["operation"],
    "Grant": metadata["grant"],
}.items():
    fields(CANDIDATE / "types.rs", name, shape)
fields(CANDIDATE / "direct_client.rs", "UpdateAuthorization", metadata["authorization"])
for name, shape in {
    "WorkerOrigin": origin,
    "NoGraphReport": metadata["report"]["report"],
    "ClassifiedReport": metadata["report"],
    "NoGraphAcceptance": metadata["no_graph"],
}.items():
    fields(CANDIDATE / "terminal_types.rs", name, shape)
metadata_rows = {
    name: {
        "count": metadata_counts[name],
        "fixed_bytes": len(encoded({str(U64): row}))
        - 4 * metadata_placeholders.get(name, 0)
        + 1,
    }
    for name, row in metadata.items()
}
metadata_fixed = sum(
    row["count"] * row["fixed_bytes"] for row in metadata_rows.values()
)
# 24 escaped label keys (actions, heads, real.actions), 8 complete real bindings,
# root/header slack, trusted context strings, terminal-operation links, clients.
metadata_extra = 24 * (1538 + 24) + 8 * PAYLOAD + 4096 + 2 * 1538 + 16 * 44 + 8 * 21
# 8 stage responses, 16 operation results, 8 copied grant bindings,
# 24 report results, 8 terminal report results, 8 terminal acceptance results.
metadata_payloads = (8 + 16 + 8 + 24 + 8 + 8) * PAYLOAD
metadata_raw_results = 8 * (4 * PAYLOAD + 1)
# A retry's target wrapper is 44 bytes larger than the terminal placeholder.
# Charge 256 per report instead, including branches not jointly reachable.
metadata_retry_slack = 24 * 256
metadata_total = (
    metadata_fixed
    + metadata_extra
    + metadata_payloads
    + metadata_raw_results
    + metadata_retry_slack
)

# Header inventories also cover fields accounted outside the row shapes.
fields(
    AUTH / "mod.rs",
    "State",
    [
        "epoch",
        "limits",
        "now",
        "next_execution",
        "operation_watermark",
        "next_attempt",
        "next_reservation",
        "next_effect",
        "operations",
        "executions",
        "workers",
        "holds",
        "attempts",
        "reservations",
        "effects",
        "receipt_retirement",
        "accepted_history",
    ],
)
fields(AUTH / "mod.rs", "Snapshot", ["version", "state"])
fields(
    RETENTION / "coarse.rs",
    "Coarse",
    [
        "limits",
        "now",
        "next_delete",
        "cohorts",
        "generations",
        "roots",
        "writes",
        "deletes",
    ],
)
fields(RETENTION / "coarse.rs", "Snapshot", ["format", "state"])
fields(
    CANDIDATE / "engine.rs",
    "Metadata",
    [
        "commit",
        "revision",
        "next_root",
        "next_write",
        "actions",
        "real",
        "credits",
        "clients",
        "update_authorizations",
        "terminal",
        "namespaces",
        "physical",
        "stages",
        "accepted",
        "heads",
        "operations",
        "grants",
    ],
)
fields(
    CANDIDATE / "engine.rs",
    "Snapshot",
    ["version", "scheduler", "retention", "metadata"],
)
fields(
    CANDIDATE / "real_request.rs", "TrustedContext", ["principal", "execution_config"]
)
fields(CANDIDATE / "real_request.rs", "RealBindings", ["context", "actions"])
fields(
    CANDIDATE / "terminal_types.rs",
    "TerminalState",
    ["enabled_at", "reports", "accepted", "operations"],
)

total = scheduler_total + retention_total + metadata_total + 1024
assert (scheduler_total, retention_total, metadata_total, total) == (
    1058032,
    214096,
    749652,
    2022804,
)
assert total < 2 * 1024 * 1024
print(
    json.dumps(
        {
            "scheduler_rows": scheduler_rows,
            "scheduler_fixed_with_header": scheduler_fixed,
            "scheduler_variables": scheduler_variables,
            "scheduler": scheduler_total,
            "retention_parts": retention_parts,
            "retention": retention_total,
            "metadata_rows": metadata_rows,
            "metadata_fixed": metadata_fixed,
            "metadata_extra": metadata_extra,
            "metadata_payloads": metadata_payloads,
            "metadata_raw_results": metadata_raw_results,
            "metadata_retry_slack": metadata_retry_slack,
            "metadata": metadata_total,
            "outer_envelope_allowance": 1024,
            "total": total,
            "limit": 2 * 1024 * 1024,
            "headroom": 2 * 1024 * 1024 - total,
        },
        indent=2,
    )
)

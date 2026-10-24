# Scheduler incident coverage

Status: partial M1 evidence, 2026-10-22. This map records the behavior inspected
in checkout `283ff776264fd4f7afe672d209995df088fc347d` and the new deterministic
fixtures. The plan's pinned `a7bb376e6f66284eb37a77b22d2f258c03ba9a7f` object was
not available locally. Inspection of the current tests is therefore evidence
about this checkout, not proof that the two revisions have identical fixtures.

The new authority remains disconnected from production scheduling. The pure
model has no actual worker process, RPC channel, automatic timer service, or
matcher wakeup loop. Its recipient simulator supplies explicit receipt and
closure rules; it does not qualify worker-side persistence or process launch.
Passing these histories does not complete M1 or M2.

## Incident families

Fixture names below identify tests in
[authority_test.rs](../../tests/authority_test.rs),
[adversarial.rs](../../tests/authority/adversarial.rs), and the independent
[reference.rs](../../tests/authority/reference.rs). All ordinary authority
histories compare implementation and reference replies, visible state, and
ordered effect identities after every applied command.

| Existing seed | Behavior represented now | Remaining transfer or qualification |
| --- | --- | --- |
| [dispatch_ack_test.rs](../../tests/dispatch_ack_test.rs) | `uncertain_dispatch_survives_snapshot_and_sender_loss` retains dispatch identity after sender loss; `stale_attempt_cannot_complete_successor_or_refund_unknown_work` fences late results; `late_decline_after_acceptance_cannot_refund_a_running_attempt` rejects contradictory release; `confirmed_decline_pauses_until_fresh_capacity_without_spinning_retries` prevents immediate repeat dispatch. | A full real dispatch channel, lost acknowledgment through actual transport, immediate cancellation delivery after late acceptance, and runtime wakeup are M2/M4 integration obligations. Confirmed decline currently consumes an attempt; the legacy test's separate untried-decline budget is not preserved. |
| [reservation_hold_test.rs](../../tests/reservation_hold_test.rs) | Seven independent histories cover aged creation, blocked lower-ranked backfill, current joined priority, reconnect, expiry/cooldown, decline, worker loss/drain, capacity veto, action departure, placement elsewhere, and fleet caps; their fixed policy and names are recorded below. | The repeated-small-job fixture reaches large-job placement under its explicit fair completion schedule. Runtime matching/timer wakeup and a supported-load starvation bound remain unqualified. The old replica-hash ownership rule is retired with independently acting schedulers. |
| [liveness_clock_step_test.rs](../../tests/liveness_clock_step_test.rs) | Generated histories submit backward `AdvanceTime`; the core rejects them without rewinding logical time. `stale_attempt_cannot_complete_successor_or_refund_unknown_work` and `worker_replacement_quarantines_uncertain_capacity` cover late results after reassignment and incarnation replacement. | The legacy best-effort wall-clock liveness refresh is not an oracle rule. Mapping real clocks, leader changes, worker observations, and timer scheduling to monotonic recorded time remains an adapter obligation. |
| [backpressure_test.rs](../../tests/backpressure_test.rs) | `confirmed_decline_pauses_until_fresh_capacity_without_spinning_retries` verifies that repeated matching cannot spin through attempts on a paused worker. Ordinary heartbeat does not reopen admission; a fresh capacity advertisement or newer registration does. | The old next-keepalive policy is replaced deliberately. Runtime capacity messages must actually wake matching; a pure explicit `Match` input does not prove that wakeup. |
| [escalation_test.rs](../../tests/escalation_test.rs) | `finite_attempt_budget_finishes_despite_three_uncertain_old_processes` covers finite replacement attempts and retained charges. Eight independent memory-failure histories below cover fixed capped growth, original grants, typed evidence, finite total attempts, and complete final responses. | The pure event assumes authenticated typed memory failure and physical quiescence. Production classification, exact worker receipt mapping, client-visible diagnostic policy, and supported-load qualification remain open. The legacy separate configurable retry budgets and class ladder are deliberately replaced by one three-attempt bound. |
| [live_memory_veto_test.rs](../../tests/live_memory_veto_test.rs) | `one_live_capacity_observation_cannot_fund_two_dispatches` independently checks aggregate live capacity. `draining_and_live_capacity_require_fresh_nonstale_advertisements` covers capacity shrink and fresh restoration. | The new policy always applies the conservative live ceiling; the old optional bypass and idle-admission exemption are not retained. Worker units, missing observations, freshness, and actual wakeup require the protocol mapping and integration fixtures. |

The distinction between a confirmed decline and lost delivery matters: a
decline while still dispatching is positive evidence of no launch and can
release its reservation. A lost acknowledgment, revoked attempt, disconnected
observer, or new process incarnation is not that evidence. The uncertain charge
stays until cleanup or reconciliation proves it can be released.

## Fixed held-worker policy and witnesses

An execution that has waited 30 recorded ticks can hold one busy compatible
worker for at most 300 ticks. A valid hold prevents work with a worse current
queue rank from using that worker; higher-ranked work can still run. The fleet
cap is one hold when there are fewer than 20 usable workers and otherwise one
per ten usable workers, rounded down. A fleet with no usable workers has no
holds. Expiry records a 300-tick cooldown. These are fixed experimental policy
constants, not calibrated production durations.

Holds do not create reservations, release old charges, preempt running work, or
renew on heartbeat. Their compatibility check subtracts every immovable charge
from an unauthorized attempt. Actual placement continues to account for every
unreleased reservation, including authorized work expected to finish. An
expired hold is reconciled by the recorded clock transition without requiring
an observer or a matching call. Changed worker facts, departure, or a smaller fleet cap invalidate and remove
the hold without an expiry cooldown.

The tempting rule "skip a hold if one running job's completion would suffice"
is deliberately absent. With four slots, three running one-slot jobs, an aged
two-slot request, and a stream of one-slot jobs, each matching call can refill
the spare slot. Every completion makes the large request one completion away
again. Healthy completions alone therefore do not ensure that it ever runs.
The fixed hold protects the available remainder once the request ages, and the
next fair completion allows the large request to be placed.

| Fixture | Checkable claim |
| --- | --- |
| `aged_one_completion_wait_cannot_be_starved_by_repeated_small_backfill` | Repeated small-job placement and completion cannot continue consuming the remaining capacity after the large request ages; a fair completion dispatches it. |
| `held_worker_accumulates_capacity_without_minting_or_refunding_reservations` | Lower-ranked arrivals stay queued while original reservations remain charged; their actual completion makes the held request fit. |
| `hold_uses_current_effective_priority_and_keeps_deadline_across_reconnect` | Joining and departing interests change the rank used for backfill; same-incarnation reconnect and heartbeat preserve the original hold deadline. |
| `hold_expiry_records_cooldown_without_observer_or_match_and_does_not_extend` | Recorded time alone expires a valid hold and stores its bounded cooldown through snapshot recovery. |
| `hold_invalidations_never_treat_worker_loss_or_capacity_veto_as_cleanup` | Cancellation, drain, capacity shrink, replacement, fenced worker loss, and confirmed decline invalidate holds while preserving the correct original accounting. |
| `uncertain_capacity_is_an_immovable_floor_and_cleanup_can_place_holder_elsewhere` | An uncertain reservation cannot be assumed to finish when choosing a held worker; confirmed cleanup on another worker can place the action and clear its previous hold. |
| `fleet_hold_cap_trims_by_current_rank_and_excludes_zero_admission_workers` | A smaller usable fleet trims excess holds by current queue rank; zero admission slots do not increase the cap. |

These finite histories do not promise progress against an infinite stream of
higher-priority requests, uncertain processes that never close, or missing
clock and matching service. They also do not qualify a wall-clock wait bound.

## Fixed memory-failure policy and witnesses

The [resource failure contract](resource-failure-policy.md) separates immutable
accepted requirements, the current execution reservation target, and every
attempt's original grant. Only a current attempt's typed memory-failure event
with confirmed quiescence, the exact original charge, and a valid unsuccessful
response can raise the target. It cannot use a worker-supplied resource echo as
a smaller growth base or infer memory exhaustion from exit status 137.

Growth doubles the original positive memory charge, or a positive measured
peak when the original charge is zero, capped by one compatible worker's
declared ceiling. CPU, slots, properties, and immovable old charges must fit
that same worker. Transient available capacity does not choose the estimate;
placement still checks every live admission limit and unreleased charge.
Dispatch carries the new grant separately from the unchanged accepted request.
The total identity budget remains three attempts across all retry causes.
Final failure retains the complete accepted response and an internal bounded
diagnostic. This model does not establish how a production client receives that
additional diagnostic.

| Fixture | Checkable claim |
| --- | --- |
| `memory_growth_preserves_accepted_request_age_join_and_each_original_grant` | A retry preserves captured ingress, operation handles, coalescing, and queue age while its new dispatch grants the recorded larger target. |
| `memory_growth_requires_one_compatible_shape_and_ignores_transient_free_memory` | Incompatible properties, insufficient CPU, and zero slots cannot contribute a fictitious memory ceiling; stale free memory cannot shrink a valid target. |
| `memory_growth_subtracts_immovable_cpu_and_slots_before_choosing_a_ceiling` | A large-memory worker whose CPU or slots remain held by an uncertain old process cannot supply the growth ceiling. |
| `memory_failure_rejects_uncertain_or_contradictory_evidence_without_mutation` | Uncertain closure, a conflicting charge, successful response, forged normalized facts, or wrong worker identity leaves authority unchanged. |
| `memory_zero_evidence_and_capped_doubling_have_no_overflow_or_invented_minimum` | Missing and measured-zero evidence do not invent a resource class; positive evidence and integer-boundary doubling cap without overflow. |
| `memory_retry_budget_is_three_total_attempts_and_preserves_full_final_failure` | Three accepted memory failures exhaust the same finite attempt budget and preserve full final response bytes without publication. |
| `memory_growth_never_changes_an_uncertain_predecessors_cleanup_charge` | A later larger target cannot refund or rewrite an earlier uncertain reservation; cleanup releases its original vector only. |
| `held_worker_uses_grown_target_while_immutable_request_keeps_original_minimum` | Hold selection, backfill blocking, and eventual placement use the raised target while the accepted request remains unchanged. |

The causal reduction regression also retains the memory-failure event that
produced a later dispatch grant. No historical estimator, resource classifier,
terminal absent-fleet policy, or queue-wait deadline follows from these tests.

## Admission pays for future bytes

The [byte headroom contract](byte-headroom.md) reserves a maximum terminal
payload for every accepted operation, execution, and possible publication,
along with original ingress copies, worker properties, and bounded future
rows. The fixed pure-state ceiling is 14 MiB; the configured complete request
or result bound cannot exceed 512 KiB. Count limits remain upper bounds: byte
admission can refuse before all count limits are reached.

The independent model sums prepaid obligations per relation, without calling
the implementation's accounting helpers. The harness compares the real
canonical snapshot length with that independent sum after every applied input
and recovery. Restore checks the same future obligation, including identity,
ownership, and payload-copy bounds; small present-day state cannot bypass the
cost of its future terminal copies.

| Fixture | Checkable claim |
| --- | --- |
| `byte_credit_counts_distinct_coalesced_ingress_and_prepays_complete_terminal_copies` | Distinct captured ingress with quote-heavy principal text remains separately charged for every coalesced handle. At exactly 14 MiB reserved credit, a near-maximum captured response can complete, publish, survive restart, and retire; the previously refused operation ID can then be admitted. |
| `byte_credit_worker_replacement_refuses_atomically_and_cancellation_cleanup_stay_available` | A larger property map cannot revoke attempts before byte refusal. Stale identity retains its rejection precedence; cancellation, physical cleanup, and completion remain possible at the ceiling. |
| `byte_credit_accepts_exact_limit_and_rejects_one_more_encoded_byte` | A property replacement costing one additional encoded byte is refused; reducing and restoring one byte reaches the exact accepted boundary. |
| `byte_credit_limits_and_restore_cannot_admit_uncommittable_or_unpaid_state` | Oversized configured payloads, unpaid restored operations, and oversized raw snapshots fail; zero payload capacity and large count ceilings do not overflow initialization. |
| `causal_reduction_keeps_terminal_retirement_that_funds_later_byte_admission` | Shrinking preserves the terminal transition, elapsed retention time, and retirement that free credit for a later accepted request. |

These checks establish the bounded pure-model obligation. Journal membership,
snapshot metadata, and framing require their separate envelope checks; allocator
RSS, total log disk use, throughput, and production response-size suitability
are outside this claim.

## Mandatory histories

| Plan history | Executable fixture or remaining gap |
| --- | --- |
| Commit dispatch, lose sender before delivery | `uncertain_dispatch_survives_snapshot_and_sender_loss`; the harness also rejects effects before commit/application in `proposals_and_commits_cannot_dispatch_before_apply`. |
| Deliver dispatch, lose acknowledgment, retry identical intent | `uncertain_dispatch_survives_snapshot_and_sender_loss` and `recipient_deduplicates_launch_and_late_cancel_cannot_kill_successor`. Receipt behavior is simulated. |
| Revoke A, create B, deliver A's late completion/cancellation | `stale_attempt_cannot_complete_successor_or_refund_unknown_work` and `recipient_deduplicates_launch_and_late_cancel_cannot_kill_successor`. |
| Connected worker stops input progress | `progress_generations_fence_timers_and_heartbeats_do_not_mask_stalls`. Heartbeat cannot renew action progress. |
| Healthy work survives process-preserving reconnect | `reconnect_and_changed_advertisements_do_not_recreate_capacity` and oracle-only `transport_reconnect_and_late_decline_do_not_create_a_second_slot`. |
| Replace incarnation before old work is known stopped | `worker_replacement_quarantines_uncertain_capacity` and oracle-only `replacement_requires_cleanup_and_retains_original_charge`. |
| Duplicate completion after reservation release | `stale_attempt_cannot_complete_successor_or_refund_unknown_work`. Terminal state and released accounting remain unchanged. |
| Snapshot with charged reservation and uncertain effect | `uncertain_dispatch_survives_snapshot_and_sender_loss`, `retained_snapshot_plus_suffix_matches_uninterrupted_execution`, and oracle-only `snapshot_keeps_uncertain_effects_and_stale_timer_cannot_expire_progress`. |
| Observer disappears while cleanup remains necessary | `last_interest_cancellation_requires_cleanup_even_without_observers`; observation itself is read-only and separate from interest renewal/expiry. |
| Progress invalidates an old inactivity timer | `progress_generations_fence_timers_and_heartbeats_do_not_mask_stalls`. `nonrenewable_execution_timer_survives_continuous_progress_and_old_stall_timers` and oracle-only `absolute_execution_expiry_is_independent_of_renewed_progress` exercise the separate immutable execution deadline, whose timer survives progress renewal. |
| Two noncacheable requests remain independent | `noncacheable_work_is_independent_and_has_no_publication_intent`. |
| Skip lookup bypasses a hit but joins actual work and can publish | `skip_lookup_never_joins_lookup_but_can_join_actual_work_and_publish`. |
| Urgent join promotes queued work; departure restores priority and age | `coalesced_handles_cancel_independently_and_rekey_priority`, `numeric_priority_and_fifo_age_survive_joins`, and the independent priority fixture. |
| Final slot cannot be recreated by reconnect or advertisements | `reconnect_and_changed_advertisements_do_not_recreate_capacity`, `draining_and_live_capacity_require_fresh_nonstale_advertisements`, and independent aggregate live-capacity fixture. |

## Compatibility facts inspected

| Source | Disposition |
| --- | --- |
| [worker_capability_index_test.rs](../../tests/worker_capability_index_test.rs) | Exact-property presence and equality remain requirements. Legacy `Minimum`, `Priority`, `Ignore`, and missing-key behavior are not imported as configurable rules. The fixed typed-resource/property mapping still needs complete protocol and migration fixtures. |
| [unsatisfiable_action_test.rs](../../tests/unsatisfiable_action_test.rs) | Distinguish a busy capable fleet from an incapable or absent fleet; repeated identical-worker churn must not indefinitely restart an eventual failure obligation. The prototype has interest expiry but no separately qualified unsatisfiable/no-worker policy or diagnostic projection. Interest expiry is not evidence that these incidents have been transferred. |
| [historical_resource_scheduler_test.rs](../../tests/historical_resource_scheduler_test.rs) | Explicit client requirements must not be silently reduced. Hint key precedence, reserved dimensions, and cold-start choices are compatibility facts for the eventual fixed controller. No hint loader, optional historical wrapper, or configurable class ladder is carried into the new core. |
| [store_awaited_action_db_test.rs](../../tests/store_awaited_action_db_test.rs) | Stable client handles, independent interest, snapshot recovery, deterministic ordering, and bounded retirement replace store/index coupling. Redis search visibility retries, orphan backstop guesses, and scheduler-peer lookup are not new authority mechanisms. Upgrade compatibility tests here do not certify the new journal's format transitions. |
| [redis_store_awaited_action_db_test.rs](../../tests/redis_store_awaited_action_db_test.rs) | Multiple handles, invocation separation, stale updates, orphan handles, and finite retention motivate `coalesced_handles_cancel_independently_and_rekey_priority`, `buck2_scope_and_digest_size_are_distinct_inflight_identities`, and `terminal_retirement_waits_for_effects_and_never_reuses_handles`. Redis TTL/index mechanics are retired; the new persisted operation watermark prevents handle reuse after retirement. |

## Exploration and limits

[campaign.rs](../../tests/authority/campaign.rs) runs 64 deterministic seeds of
256 generated application inputs each, in addition to two worker registrations
per seed. An explicitly declared lifecycle prefix reaches the major execution
stages; the remaining inputs vary identities, progress, capacity, deadlines,
incarnation changes, cancellations, and external outcomes. Transport scheduling
separately varies proposal loss, commit-before-crash, application, duplicate
delivery, checkpoint, and recovery. Reports separate generated command counts
from successfully applied commands, so rejected traffic is not presented as
successful transition coverage.

Every fourth generated seed uses the maximum 512 KiB complete-payload bound;
the other seeds use 64 KiB. This varies the prepaid finalization cost even when
the generated commands themselves are small. The report includes observed
credit and canonical-snapshot high-water marks; the separate byte-boundary
fixtures exercise large captured payloads.

The exhaustive campaign explores a declared finite event alphabet through depth
five using full snapshot identity for visited states. Its state and transition
counts describe only that alphabet and depth. The fair-drain fixture proves a
finite 16-job workload finishes under its explicit responsive-worker and
successful-publication schedule. The separate repeated-small-job hold fixture
exercises accumulation under a finite adversarial arrival sequence. The memory
fixtures exercise fixed retries and termination. None establishes a starvation
bound under general sustained demand.

Failure bundles contain the original scenario and transport schedule, minimized
replays, seed, observed state/replies, source content, and dependency lockfile.
The causal reducer keeps prerequisite producers; the schedule reducer preserves
the failing race while removing irrelevant delivery/checkpoint/crash steps.
Malformed transport traces are not accepted as substitute failures. Source
content matters because an uncommitted working tree is not identified by Git
HEAD alone.

Use the checked-in evidence receipts for measured counts and mutation outcomes.
Those receipts must be regenerated after semantic changes; this map intentionally
does not duplicate a possibly stale pass count. M1 still requires the outstanding
incident dispositions, broader supported-load liveness evidence, complete
mutation coverage, and review of the fixed policies above before it can be
declared complete.

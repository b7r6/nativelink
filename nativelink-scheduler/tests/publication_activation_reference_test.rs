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

#[path = "publication_activation/reference.rs"]
mod reference;
#[path = "publication_activation/types.rs"]
mod types;

use std::collections::{BTreeMap, BTreeSet};

use reference::Model;
use sha2::{Digest, Sha256};
use types::*;

const OLD: Namespace = Namespace([1; 16]);
const NEW: Namespace = Namespace([2; 16]);
const CAS: Namespace = Namespace([3; 16]);
const HISTORY: Namespace = Namespace([4; 16]);

fn descriptor(bytes: &[u8], algorithm: Algorithm) -> Object {
    Object {
        algorithm,
        hash: match algorithm {
            Algorithm::Sha256 => Sha256::digest(bytes).into(),
            Algorithm::Blake3 => *blake3::hash(bytes).as_bytes(),
        },
        bytes: bytes.len() as u64,
    }
}
const fn generation(namespace: Namespace, number: u64) -> Generation {
    Generation {
        namespace,
        incarnation: [9; 16],
        number,
    }
}
const fn request(effect: u64, id: u64) -> RequestRef {
    RequestRef {
        kind: EffectKind::Sink,
        effect,
        request: id,
    }
}
const fn history_request() -> RequestRef {
    RequestRef {
        kind: EffectKind::History,
        effect: 13,
        request: 301,
    }
}

fn fixture(algorithm: Algorithm) -> (Legacy, Facts, Activation) {
    let a = b"immutable ActionResult A".to_vec();
    let b = b"immutable ActionResult B".to_vec();
    let response = b"original terminal A; message never rewritten".to_vec();
    let history = [
        b"opaque historical framing:".as_slice(),
        response.as_slice(),
    ]
    .concat();
    let origin = Origin {
        epoch: 0,
        accepted_commit: 10,
        execution: 1,
        attempt: 1,
        worker: 0,
        incarnation: 0,
    };
    let terminal_a = Terminal {
        id: 1,
        action: "qualified/action".into(),
        origin,
        operations: BTreeSet::from([1, 2]),
        response,
        result: descriptor(&a, algorithm),
        result_bytes: a.clone(),
        ac_eligible: true,
    };
    let terminal_b = Terminal {
        id: 2,
        action: "qualified/action".into(),
        origin: Origin {
            accepted_commit: 20,
            execution: 2,
            attempt: 2,
            worker: 9,
            incarnation: 9,
            ..origin
        },
        operations: BTreeSet::from([3]),
        response: b"terminal B".to_vec(),
        result: descriptor(&b, algorithm),
        result_bytes: b.clone(),
        ac_eligible: true,
    };
    let result_a = Member {
        object: terminal_a.result.clone(),
        generation: generation(CAS, 1),
        role: Role::Result,
    };
    let blob = Member {
        object: descriptor(b"shared file and stdout", algorithm),
        generation: generation(CAS, 2),
        role: Role::Blob,
    };
    let tree = Member {
        object: descriptor(b"Tree with embedded Directory, file edge", algorithm),
        generation: generation(CAS, 3),
        role: Role::Tree,
    };
    let result_b = Member {
        object: terminal_b.result.clone(),
        generation: generation(CAS, 4),
        role: Role::Result,
    };
    let history_member = Member {
        object: descriptor(&history, algorithm),
        generation: generation(HISTORY, 5),
        role: Role::History,
    };
    let history_effect = HistoryEffect {
        id: 13,
        terminal: 1,
        origin,
        destination: HISTORY,
        exposure: Exposure::Opaque,
        object: history_member.object.clone(),
        payload: history,
        requests: BTreeMap::from([(301, Delivery::Unknown)]),
        materialized: false,
    };
    let roots = BTreeMap::from([
        (
            1,
            Root {
                id: 1,
                subject: Subject::Result(1),
                object: result_a.object.clone(),
                required: BTreeSet::from([result_a.clone(), blob.clone(), tree.clone()]),
            },
        ),
        (
            2,
            Root {
                id: 2,
                subject: Subject::Result(2),
                object: result_b.object.clone(),
                required: BTreeSet::from([result_b.clone(), blob.clone()]),
            },
        ),
        (
            3,
            Root {
                id: 3,
                subject: Subject::History(13),
                object: history_member.object.clone(),
                required: BTreeSet::from([history_member.clone()]),
            },
        ),
    ]);
    let protections = roots
        .iter()
        .map(|(id, root)| {
            (
                *id + 100,
                Protection {
                    id: *id + 100,
                    root: *id,
                    catalogue_revision: 7,
                    complete: true,
                    verified: true,
                    typed: root.required.clone(),
                    protected: root
                        .required
                        .iter()
                        .map(|member| member.generation)
                        .collect(),
                    fences: BTreeSet::from([Fence {
                        namespace: if *id == 3 { HISTORY } else { CAS },
                        revision: 7,
                    }]),
                    until: 100,
                },
            )
        })
        .collect();
    let facts = Facts {
        now: 30,
        catalogue_revision: 7,
        catalogue: [result_a, blob, tree, result_b, history_member]
            .into_iter()
            .map(|member| CatalogueRow {
                key: member.generation,
                object: member.object,
                state: Readiness::Ready,
            })
            .collect(),
        fences: BTreeSet::from([
            Fence {
                namespace: CAS,
                revision: 7,
            },
            Fence {
                namespace: HISTORY,
                revision: 7,
            },
        ]),
        protections,
        bindings: [OLD, NEW, CAS, HISTORY]
            .into_iter()
            .map(|namespace| Binding {
                namespace,
                physical_domain: [namespace.0[0]; 32],
                old_requests_confined: namespace == OLD,
            })
            .collect(),
        cold: BTreeSet::from([NEW]),
        migration_publishers: BTreeSet::from([77]),
        quiescent: BTreeSet::from([request(11, 102), request(12, 201)]),
        quiescent_effects: BTreeSet::from([EffectRef {
            kind: EffectKind::Sink,
            effect: 12,
        }]),
        quiescent_domains: BTreeSet::new(),
        current_workers: BTreeSet::from([(99, 99)]),
    };
    let legacy = Legacy {
        old_sink: OLD,
        domain_exposure: Exposure::Opaque,
        terminals: BTreeMap::from([(1, terminal_a), (2, terminal_b)]),
        effects: BTreeMap::from([
            (
                11,
                SinkEffect {
                    id: 11,
                    terminal: 1,
                    destination: OLD,
                    exposure: Exposure::Opaque,
                    payload: a,
                    requests: BTreeMap::from([(101, Delivery::Unknown), (102, Delivery::Stored)]),
                },
            ),
            (
                12,
                SinkEffect {
                    id: 12,
                    terminal: 2,
                    destination: OLD,
                    exposure: Exposure::Opaque,
                    payload: b,
                    requests: BTreeMap::from([(201, Delivery::Stored)]),
                },
            ),
        ]),
        histories: BTreeMap::from([(13, history_effect)]),
        roots,
        late_outcomes: BTreeSet::from([Observation {
            request: request(11, 102),
            outcome: Delivery::Stored,
        }]),
        request_quiescence: BTreeSet::from([request(11, 102)]),
    };
    let activation = Activation {
        commit: 30,
        new_namespace: NEW,
        dispositions: BTreeMap::from([(11, Disposition::Retired), (12, Disposition::Settled)]),
        imports: vec![],
        protections: BTreeMap::from([(1, 101), (2, 102), (3, 103)]),
    };
    (legacy, facts, activation)
}

fn rejected(model: &mut Model, event: &Event, facts: &Facts) -> Reject {
    let before = model.snapshot().unwrap();
    let error = model.apply(event, facts).unwrap_err();
    assert_eq!(
        model.snapshot().unwrap(),
        before,
        "refused transition changed authority"
    );
    error
}
fn observe(legacy: &Legacy, request: RequestRef, outcome: Delivery) -> Event {
    let (destination, object) = match request.kind {
        EffectKind::Sink => {
            let effect = &legacy.effects[&request.effect];
            (
                effect.destination,
                legacy.terminals[&effect.terminal].result.clone(),
            )
        }
        EffectKind::History => {
            let effect = &legacy.histories[&request.effect];
            (effect.destination, effect.object.clone())
        }
    };
    Event::Observe {
        request,
        destination,
        object,
        outcome,
    }
}

#[test]
fn cold_activation_preserves_all_accepted_bytes_ids_and_uncertain_requests() {
    for algorithm in [Algorithm::Sha256, Algorithm::Blake3] {
        let (legacy, facts, activation) = fixture(algorithm);
        let mut model = Model::new(legacy.clone()).unwrap();
        assert_eq!(
            model.apply(&Event::Activate(activation.clone()), &facts),
            Ok(Outcome::Applied)
        );
        assert_eq!(model.heads, Vec::<Head>::new());
        assert_eq!(model.legacy, legacy);
        assert_eq!(model.activation, Some(activation));
        assert_eq!(
            model.unresolved_requests(),
            BTreeSet::from([request(11, 101), history_request()])
        );
        assert_eq!(model.pending_history(), BTreeSet::from([13]));
        assert_eq!(
            model.unresolved_effects(),
            BTreeSet::from([
                EffectRef {
                    kind: EffectKind::Sink,
                    effect: 11,
                },
                EffectRef {
                    kind: EffectKind::History,
                    effect: 13,
                },
            ])
        );
        assert!(model.unresolved_legacy_domain());
        assert!(!model.may_collect(NEW, facts.now));
        assert_eq!(
            Model::restore(&model.snapshot().unwrap(), &legacy).unwrap(),
            model
        );
    }
}

#[test]
fn missing_or_extra_old_intents_and_retained_roots_refuse_atomically() {
    let (legacy, facts, activation) = fixture(Algorithm::Sha256);
    let mut rejected_count = 0;
    for effect_mask in 0..4 {
        for root_mask in 0..8 {
            let mut plan = activation.clone();
            plan.dispositions
                .retain(|id, _| effect_mask & (1 << (*id - 11)) != 0);
            plan.protections
                .retain(|id, _| root_mask & (1 << (*id - 1)) != 0);
            let mut model = Model::new(legacy.clone()).unwrap();
            if effect_mask == 3 && root_mask == 7 {
                model.apply(&Event::Activate(plan), &facts).unwrap();
            } else {
                assert_eq!(
                    rejected(&mut model, &Event::Activate(plan), &facts),
                    Reject::Incomplete
                );
                rejected_count += 1;
            }
        }
    }
    assert_eq!(rejected_count, 31);
    let mut extra = activation;
    extra.dispositions.insert(99, Disposition::Retired);
    assert_eq!(
        rejected(
            &mut Model::new(legacy).unwrap(),
            &Event::Activate(extra),
            &facts
        ),
        Reject::Incomplete
    );
}

#[test]
fn successful_retry_or_retirement_does_not_settle_an_unknown_request() {
    let (legacy, mut facts, mut activation) = fixture(Algorithm::Sha256);
    activation.dispositions.insert(11, Disposition::Settled);
    let mut model = Model::new(legacy.clone()).unwrap();
    assert_eq!(
        rejected(&mut model, &Event::Activate(activation.clone()), &facts),
        Reject::Uncertain
    );
    activation.dispositions.insert(11, Disposition::Retired);
    model.apply(&Event::Activate(activation), &facts).unwrap();
    model
        .apply(
            &observe(&legacy, request(11, 101), Delivery::Stored),
            &facts,
        )
        .unwrap();
    assert!(model.unresolved_requests().contains(&request(11, 101)));
    assert_eq!(
        rejected(&mut model, &Event::Quiesce(request(11, 101)), &facts),
        Reject::Uncertain
    );
    facts.quiescent.insert(request(11, 101));
    model
        .apply(&Event::Quiesce(request(11, 101)), &facts)
        .unwrap();
    assert!(!model.unresolved_requests().contains(&request(11, 101)));
    assert_eq!(model.legacy.effects[&11].requests[&101], Delivery::Unknown);
    assert!(model.observations.contains(&Observation {
        request: request(11, 101),
        outcome: Delivery::Stored
    }));
    assert_eq!(model.legacy, legacy);
}

#[test]
fn explicit_import_keeps_revoked_acceptance_and_late_a_cannot_replace_b() {
    let (legacy, facts, mut activation) = fixture(Algorithm::Blake3);
    activation.imports.push(Import {
        publication: 502,
        terminal: 2,
        publisher: 77,
    });
    activation
        .dispositions
        .insert(12, Disposition::Converted { publication: 502 });
    let mut model = Model::new(legacy.clone()).unwrap();
    model.apply(&Event::Activate(activation), &facts).unwrap();
    assert_eq!(model.heads[0].terminal, 2);
    assert_eq!(model.heads[0].origin, legacy.terminals[&2].origin);
    assert!(!facts.current_workers.contains(&(
        model.heads[0].origin.worker,
        model.heads[0].origin.incarnation
    )));
    let heads = model.heads.clone();
    for outcome in [Delivery::Unknown, Delivery::Stored] {
        model
            .apply(&observe(&legacy, request(11, 101), outcome), &facts)
            .unwrap();
    }
    assert_eq!(model.heads, heads);
    assert_eq!(model.legacy, legacy);
    assert_eq!(
        model.apply(
            &observe(&legacy, request(11, 101), Delivery::Stored),
            &facts
        ),
        Ok(Outcome::Ignored)
    );
    assert_eq!(
        rejected(
            &mut model,
            &observe(&legacy, request(11, 101), Delivery::Failed),
            &facts
        ),
        Reject::Conflict
    );
}

#[test]
fn imports_need_explicit_publisher_eligibility_and_matching_conversion() {
    let (legacy, facts, activation) = fixture(Algorithm::Sha256);
    for variant in 0..5 {
        let mut plan = activation.clone();
        plan.imports.push(Import {
            publication: 502,
            terminal: 2,
            publisher: 77,
        });
        match variant {
            0 => plan.imports[0].publisher = 999,
            1 => plan
                .dispositions
                .insert(11, Disposition::Converted { publication: 502 })
                .map(|_| ())
                .unwrap(),
            2 => plan
                .dispositions
                .insert(12, Disposition::Converted { publication: 999 })
                .map(|_| ())
                .unwrap(),
            3 => plan.imports.push(Import {
                publication: 501,
                terminal: 1,
                publisher: 77,
            }),
            4 => plan.commit = 20,
            _ => unreachable!(),
        }
        rejected(
            &mut Model::new(legacy.clone()).unwrap(),
            &Event::Activate(plan),
            &facts,
        );
    }
    let mut no_cache = legacy;
    no_cache.effects.remove(&12);
    no_cache.terminals.get_mut(&2).unwrap().ac_eligible = false;
    let mut plan = activation;
    plan.dispositions.remove(&12);
    let mut model = Model::new(no_cache.clone()).unwrap();
    plan.imports.push(Import {
        publication: 502,
        terminal: 2,
        publisher: 77,
    });
    assert_eq!(
        rejected(&mut model, &Event::Activate(plan.clone()), &facts),
        Reject::Invalid
    );
    plan.imports.clear();
    model.apply(&Event::Activate(plan), &facts).unwrap();
    assert!(
        model.coverage.contains_key(&2),
        "noncacheable operation result still needs protection"
    );
}

#[test]
fn namespace_identity_alias_coldness_and_old_request_confinement_are_mandatory() {
    let (legacy, facts, activation) = fixture(Algorithm::Sha256);
    for variant in 0..6 {
        let mut changed = facts.clone();
        let mut plan = activation.clone();
        match variant {
            0 => plan.new_namespace = OLD,
            1 => {
                changed
                    .bindings
                    .iter_mut()
                    .find(|row| row.namespace == NEW)
                    .unwrap()
                    .physical_domain = [1; 32];
            }
            2 => changed.cold.clear(),
            3 => {
                changed
                    .bindings
                    .iter_mut()
                    .find(|row| row.namespace == OLD)
                    .unwrap()
                    .old_requests_confined = false;
            }
            4 => changed.bindings.retain(|row| row.namespace != NEW),
            5 => {
                changed
                    .bindings
                    .iter_mut()
                    .find(|row| row.namespace == CAS)
                    .unwrap()
                    .physical_domain = [1; 32];
            }
            _ => unreachable!(),
        }
        assert_eq!(
            rejected(
                &mut Model::new(legacy.clone()).unwrap(),
                &Event::Activate(plan),
                &changed
            ),
            Reject::Namespace
        );
    }
}

#[test]
fn complete_typed_closure_current_fences_and_ready_exact_generations_are_required() {
    let (legacy, facts, activation) = fixture(Algorithm::Sha256);
    for variant in 0..11 {
        let mut changed = facts.clone();
        let proof = changed.protections.get_mut(&101).unwrap();
        match variant {
            0 => {
                proof.typed.retain(|member| member.role != Role::Blob);
            }
            1 => {
                let mut tree = proof
                    .typed
                    .iter()
                    .find(|member| member.role == Role::Tree)
                    .unwrap()
                    .clone();
                proof.typed.remove(&tree);
                tree.role = Role::Directory;
                proof.typed.insert(tree);
            }
            2 => {
                proof.protected.remove(&generation(CAS, 2));
            }
            3 => proof.complete = false,
            4 => proof.verified = false,
            5 => proof.catalogue_revision -= 1,
            6 => {
                proof.fences.clear();
                proof.fences.insert(Fence {
                    namespace: CAS,
                    revision: 6,
                });
            }
            7 => proof.until = changed.now,
            8 => {
                changed
                    .catalogue
                    .iter_mut()
                    .find(|row| row.key == generation(CAS, 2))
                    .unwrap()
                    .state = Readiness::Retired;
            }
            9 => {
                changed
                    .catalogue
                    .iter_mut()
                    .find(|row| row.key == generation(CAS, 2))
                    .unwrap()
                    .object
                    .hash[0] ^= 1;
            }
            10 => {
                changed
                    .catalogue
                    .iter_mut()
                    .find(|row| row.key == generation(CAS, 2))
                    .unwrap()
                    .key
                    .incarnation[15] ^= 1;
            }
            _ => unreachable!(),
        }
        rejected(
            &mut Model::new(legacy.clone()).unwrap(),
            &Event::Activate(activation.clone()),
            &changed,
        );
    }
}

#[test]
fn pending_history_keeps_exact_bytes_identity_origin_and_its_own_uncertainty() {
    let (legacy, mut facts, activation) = fixture(Algorithm::Blake3);
    let mut model = Model::new(legacy.clone()).unwrap();
    facts
        .catalogue
        .iter_mut()
        .find(|row| row.key == generation(HISTORY, 5))
        .unwrap()
        .state = Readiness::Writing;
    assert_eq!(
        rejected(&mut model, &Event::Activate(activation.clone()), &facts),
        Reject::Unprotected
    );
    facts
        .catalogue
        .iter_mut()
        .find(|row| row.key == generation(HISTORY, 5))
        .unwrap()
        .state = Readiness::Ready;
    model.apply(&Event::Activate(activation), &facts).unwrap();
    assert!(model.coverage.contains_key(&3));
    model
        .apply(
            &observe(&legacy, history_request(), Delivery::Stored),
            &facts,
        )
        .unwrap();
    assert!(model.pending_history().is_empty());
    assert!(model.unresolved_requests().contains(&history_request()));
    assert_eq!(model.legacy.histories[&13], legacy.histories[&13]);
    assert_eq!(model.legacy.terminals, legacy.terminals);
    assert_eq!(model.heads, Vec::<Head>::new());
}

#[test]
fn collection_is_explicit_covers_every_root_and_never_owns_old_sink() {
    let (legacy, mut facts, activation) = fixture(Algorithm::Sha256);
    let mut model = Model::new(legacy).unwrap();
    assert_eq!(
        rejected(
            &mut model,
            &Event::EnableCollection {
                namespace: NEW,
                through: 90
            },
            &facts
        ),
        Reject::WrongPhase
    );
    model.apply(&Event::Activate(activation), &facts).unwrap();
    assert_eq!(
        rejected(
            &mut model,
            &Event::EnableCollection {
                namespace: OLD,
                through: 90
            },
            &facts
        ),
        Reject::Namespace
    );
    assert_eq!(
        rejected(
            &mut model,
            &Event::EnableCollection {
                namespace: NEW,
                through: 101
            },
            &facts
        ),
        Reject::Unprotected
    );
    model
        .apply(
            &Event::EnableCollection {
                namespace: NEW,
                through: 90,
            },
            &facts,
        )
        .unwrap();
    assert!(model.may_collect(NEW, 89));
    assert!(!model.may_collect(NEW, 90));
    assert!(!model.may_collect(OLD, 40));
    assert!(model.unresolved_requests().contains(&request(11, 101)));
    facts.now = 101;
    assert_eq!(
        rejected(
            &mut model,
            &Event::EnableCollection {
                namespace: NEW,
                through: 102
            },
            &facts
        ),
        Reject::Unprotected
    );
    facts.catalogue_revision += 1;
    facts.fences = facts
        .fences
        .iter()
        .map(|fence| Fence {
            revision: fence.revision + 1,
            ..*fence
        })
        .collect();
    for proof in facts.protections.values_mut() {
        proof.catalogue_revision = facts.catalogue_revision;
        proof.until = 200;
        proof.fences = proof
            .fences
            .iter()
            .map(|fence| Fence {
                revision: fence.revision + 1,
                ..*fence
            })
            .collect();
    }
    for root in 1..=3 {
        model
            .apply(
                &Event::Protect {
                    root,
                    evidence: root + 100,
                },
                &facts,
            )
            .unwrap();
    }
    model
        .apply(
            &Event::EnableCollection {
                namespace: NEW,
                through: 190,
            },
            &facts,
        )
        .unwrap();
    assert!(model.may_collect(NEW, 189));
}

#[test]
fn wrong_late_outcomes_cannot_invent_requests_destinations_or_objects() {
    let (legacy, facts, activation) = fixture(Algorithm::Sha256);
    let mut model = Model::new(legacy.clone()).unwrap();
    model.apply(&Event::Activate(activation), &facts).unwrap();
    for variant in 0..4 {
        let mut event = observe(&legacy, request(11, 101), Delivery::Stored);
        if let Event::Observe {
            request,
            destination,
            object,
            outcome,
        } = &mut event
        {
            match variant {
                0 => request.request = 999,
                1 => *destination = NEW,
                2 => object.hash[0] ^= 1,
                3 => *outcome = Delivery::Issued,
                _ => unreachable!(),
            }
        }
        rejected(&mut model, &event, &facts);
    }
}

#[test]
fn every_activation_snapshot_cut_and_outcome_order_replays_identically() {
    let (legacy, mut facts, activation) = fixture(Algorithm::Blake3);
    facts.quiescent.insert(request(11, 101));
    for reverse in [false, true] {
        let mut suffix = vec![
            observe(&legacy, request(11, 101), Delivery::Stored),
            observe(&legacy, history_request(), Delivery::Stored),
        ];
        if reverse {
            suffix.reverse();
        }
        let mut events = vec![Event::Activate(activation.clone())];
        events.extend(suffix);
        events.push(Event::Quiesce(request(11, 101)));
        events.push(Event::EnableCollection {
            namespace: NEW,
            through: 90,
        });
        let mut baseline = Model::new(legacy.clone()).unwrap();
        for event in &events {
            baseline.apply(event, &facts).unwrap();
        }
        for cut in 0..=events.len() {
            let mut prefix = Model::new(legacy.clone()).unwrap();
            for event in &events[..cut] {
                prefix.apply(event, &facts).unwrap();
            }
            let snapshot = prefix.snapshot().unwrap();
            let mut restored = Model::restore(&snapshot, &legacy).unwrap();
            assert_eq!(restored.snapshot().unwrap(), snapshot);
            for event in &events[cut..] {
                restored.apply(event, &facts).unwrap();
            }
            assert_eq!(restored, baseline);
            assert_eq!(restored.snapshot().unwrap(), baseline.snapshot().unwrap());
        }
    }
}

#[test]
fn snapshots_refuse_dropped_obligations_changed_terminals_and_forged_heads() {
    let (legacy, facts, mut activation) = fixture(Algorithm::Sha256);
    activation.imports.push(Import {
        publication: 502,
        terminal: 2,
        publisher: 77,
    });
    let mut model = Model::new(legacy.clone()).unwrap();
    model.apply(&Event::Activate(activation), &facts).unwrap();
    for variant in 0..7 {
        let mut bad = model.clone();
        match variant {
            0 => {
                bad.activation.as_mut().unwrap().dispositions.remove(&11);
            }
            1 => {
                bad.coverage.remove(&3);
            }
            2 => bad.legacy.terminals.get_mut(&1).unwrap().response.push(7),
            3 => bad.heads[0].terminal = 1,
            4 => bad.legacy.histories.get_mut(&13).unwrap().payload.push(7),
            5 => bad.collection_until = Some(101),
            6 => {
                bad.observations.insert(Observation {
                    request: request(99, 999),
                    outcome: Delivery::Stored,
                });
            }
            _ => unreachable!(),
        }
        let bytes = serde_json::to_vec(&bad).unwrap();
        assert!(Model::restore(&bytes, &legacy).is_err());
    }
    let mut bytes = model.snapshot().unwrap();
    bytes.push(b' ');
    assert!(Model::restore(&bytes, &legacy).is_err());
    assert_eq!(
        Model::restore(&vec![b' '; MAX_SNAPSHOT_BYTES + 1], &legacy),
        Err(Reject::Bounds)
    );
}

#[test]
fn legacy_inventory_is_complete_bounded_and_hash_bound_before_activation() {
    let (legacy, facts, activation) = fixture(Algorithm::Sha256);
    for variant in 0..6 {
        let mut bad = legacy.clone();
        match variant {
            0 => {
                bad.roots.remove(&3);
            }
            1 => bad.histories.get_mut(&13).unwrap().origin.attempt = 999,
            2 => bad.effects.get_mut(&11).unwrap().payload.push(1),
            3 => bad.terminals.get_mut(&1).unwrap().result.hash[0] ^= 1,
            4 => bad.terminals.get_mut(&1).unwrap().origin.attempt = 0,
            5 => {
                bad.effects
                    .get_mut(&11)
                    .unwrap()
                    .requests
                    .insert(0, Delivery::Unknown);
            }
            _ => unreachable!(),
        }
        assert!(Model::new(bad).is_err());
    }
    let mut model = Model::new(legacy).unwrap();
    model
        .apply(&Event::Activate(activation.clone()), &facts)
        .unwrap();
    assert_eq!(
        rejected(&mut model, &Event::Activate(activation), &facts),
        Reject::WrongPhase
    );
}

#[test]
fn opaque_exposure_survives_empty_or_partial_send_inventory_and_late_records() {
    let (mut legacy, mut facts, mut activation) = fixture(Algorithm::Sha256);
    legacy.effects.get_mut(&11).unwrap().requests.clear();
    legacy.late_outcomes.clear();
    legacy.request_quiescence.clear();
    let mut model = Model::new(legacy.clone()).unwrap();
    activation.dispositions.insert(11, Disposition::Settled);
    assert_eq!(
        rejected(&mut model, &Event::Activate(activation.clone()), &facts),
        Reject::Uncertain
    );
    activation.dispositions.insert(11, Disposition::Retired);
    model.apply(&Event::Activate(activation), &facts).unwrap();
    let effect = EffectRef {
        kind: EffectKind::Sink,
        effect: 11,
    };
    assert!(model.unresolved_effects().contains(&effect));
    assert_eq!(model.legacy.effects[&11].exposure, Exposure::Opaque);
    assert_eq!(
        rejected(&mut model, &Event::QuiesceEffect(effect), &facts),
        Reject::Uncertain
    );
    facts.quiescent_effects.insert(effect);
    model.apply(&Event::QuiesceEffect(effect), &facts).unwrap();
    assert!(!model.unresolved_effects().contains(&effect));
    assert_eq!(model.legacy.effects[&11].exposure, Exposure::Opaque);
    assert_eq!(
        Model::restore(&model.snapshot().unwrap(), &legacy).unwrap(),
        model
    );

    let (mut legacy, facts, mut activation) = fixture(Algorithm::Sha256);
    legacy.effects.get_mut(&11).unwrap().exposure = Exposure::ProvenQuiescent;
    legacy.late_outcomes.insert(Observation {
        request: request(11, 101),
        outcome: Delivery::Stored,
    });
    activation.dispositions.insert(11, Disposition::Settled);
    let mut model = Model::new(legacy.clone()).unwrap();
    model.apply(&Event::Activate(activation), &facts).unwrap();
    assert_eq!(model.observations, legacy.late_outcomes);
    assert_eq!(model.legacy.effects[&11].requests[&101], Delivery::Unknown);
    assert!(!model.unresolved_effects().contains(&effect));
}

#[test]
fn empty_retained_outbox_never_proves_forgotten_legacy_requests_stopped() {
    let (mut legacy, mut facts, mut activation) = fixture(Algorithm::Sha256);
    legacy.effects.clear();
    legacy.late_outcomes.clear();
    legacy.request_quiescence.clear();
    activation.dispositions.clear();
    let mut model = Model::new(legacy.clone()).unwrap();
    model.apply(&Event::Activate(activation), &facts).unwrap();
    assert!(model.unresolved_legacy_domain());
    model
        .apply(
            &Event::EnableCollection {
                namespace: NEW,
                through: 90,
            },
            &facts,
        )
        .unwrap();
    assert!(!model.may_collect(OLD, 40));
    assert_eq!(
        rejected(&mut model, &Event::QuiesceDomain(OLD), &facts),
        Reject::Uncertain
    );
    assert_eq!(
        Model::restore(&model.snapshot().unwrap(), &legacy).unwrap(),
        model
    );
    facts.quiescent_domains.insert(OLD);
    model.apply(&Event::QuiesceDomain(OLD), &facts).unwrap();
    assert!(!model.unresolved_legacy_domain());
    assert_eq!(model.legacy.domain_exposure, Exposure::Opaque);
    assert!(!model.may_collect(OLD, 40));
    assert!(
        model.unresolved_effects().contains(&EffectRef {
            kind: EffectKind::History,
            effect: 13
        }),
        "old sink quiescence does not settle a different history destination"
    );
}

// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! A per-scrape collector that walks a set of [`RootMetricsComponent`] trees
//! and renders their metrics as Prometheus text.
//!
//! The `nativelink_metric` publishing machinery emits its data as `tracing`
//! events (values) and spans (group nesting) on the `nativelink_metric`
//! target. Rather than run an always-on global `tracing` layer, we build a
//! throwaway collecting subscriber, install it with
//! [`tracing::subscriber::with_default`] for the duration of a single publish
//! pass (scoped to the collecting thread), and drain the buffer. This keeps
//! the hot path free of any metrics-collection overhead when nobody is
//! scraping.

use core::fmt::Write as _;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

pub use nativelink_metric::RootMetricsComponent;
use nativelink_metric::{MetricKind, publish};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt as _};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};

/// The `tracing` target that all `nativelink_metric` events and spans use.
const METRIC_TARGET: &str = "nativelink_metric";

/// Serializes concurrent [`collect`] calls. `with_default` is thread-local so
/// concurrent collects would not corrupt each other's buffers, but publishing
/// walks shared component state and there is no value in doing it in parallel.
static COLLECT_LOCK: Mutex<()> = Mutex::new(());

/// A single flattened metric gathered from a component tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectedMetric {
    /// Fully-qualified, sanitized metric name (without the `nativelink_`
    /// prefix, which is added at render time).
    pub name: String,
    /// The value. Numeric metrics carry a `u64`; string metrics carry text.
    pub value: CollectedValue,
    /// Help text as published by the component.
    pub help: String,
}

/// The value of a [`CollectedMetric`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CollectedValue {
    Counter(u64),
    Str(String),
}

/// Sanitizes a metric-name segment to the Prometheus-legal character set
/// `[a-zA-Z0-9_:]`; every other byte becomes `_`.
fn sanitize(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Extracts the `__name`/`__value`/`__type`/`__help` fields from an event or
/// span record.
#[derive(Default)]
struct FieldVisitor {
    name: Option<String>,
    help: String,
    counter: Option<u64>,
    string: Option<String>,
    kind: u64,
}

impl Visit for FieldVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "__value" => self.counter = Some(value),
            "__type" => self.kind = value,
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "__name" => self.name = Some(value.to_string()),
            "__value" => self.string = Some(value.to_string()),
            "__help" => self.help = value.to_string(),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn core::fmt::Debug) {
        // `__name`/`__help` arrive as `String` (recorded via `record_str` in
        // practice), but be defensive against debug-formatted fallbacks.
        match field.name() {
            "__name" if self.name.is_none() => self.name = Some(format!("{value:?}")),
            "__help" if self.help.is_empty() => self.help = format!("{value:?}"),
            _ => {}
        }
    }
}

/// The group name attached to a `nativelink_metric` span, stored in the span's
/// extensions so descendant events can reconstruct the full metric path.
#[derive(Clone)]
struct GroupName(String);

/// A `tracing` layer that flattens `nativelink_metric` events into a shared
/// buffer.
struct CollectingLayer {
    buffer: Arc<Mutex<Vec<CollectedMetric>>>,
}

impl<S> Layer<S> for CollectingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if attrs.metadata().target() != METRIC_TARGET {
            return;
        }
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        if let Some(name) = visitor.name
            && let Some(span) = ctx.span(id)
        {
            span.extensions_mut().insert(GroupName(name));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if event.metadata().target() != METRIC_TARGET {
            return;
        }
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let Some(leaf) = visitor.name else {
            return;
        };

        // Walk the enclosing span scope (nearest-first) to gather the group
        // prefix, then reverse to get root-to-leaf order.
        let mut path: Vec<String> = Vec::new();
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                if let Some(group) = span.extensions().get::<GroupName>() {
                    path.push(group.0.clone());
                }
            }
        }
        path.reverse();
        path.push(leaf);
        let name = sanitize(&path.join("_"));

        let value = match MetricKind::from(visitor.kind) {
            MetricKind::String => CollectedValue::Str(visitor.string.unwrap_or_default()),
            // Counter / Default / Component all carry numeric data here.
            _ => CollectedValue::Counter(visitor.counter.unwrap_or_default()),
        };

        if let Ok(mut buffer) = self.buffer.lock() {
            buffer.push(CollectedMetric {
                name,
                value,
                help: visitor.help,
            });
        }
    }
}

/// Walks each `(name, root)` component tree and returns the flattened list of
/// metrics. Collection is serialized across callers.
#[must_use]
pub fn collect(roots: &[(String, Arc<dyn RootMetricsComponent>)]) -> Vec<CollectedMetric> {
    let _guard = COLLECT_LOCK.lock().unwrap_or_else(PoisonError::into_inner);

    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = Registry::default().with(CollectingLayer {
        buffer: Arc::clone(&buffer),
    });

    tracing::subscriber::with_default(subscriber, || {
        for (name, root) in roots {
            // `publish!` returns via `?` on failure; wrap in a closure so a
            // single failing component does not abort the whole scrape.
            let published = (|| -> Result<(), nativelink_metric::Error> {
                // Pass `name` as the group (5th arg) so every metric emitted by
                // the root tree is prefixed with the root's name (e.g.
                // `stores_*`, `schedulers_<n>_*`).
                publish!(
                    name,
                    root,
                    MetricKind::Component,
                    "Root metrics component.".to_string(),
                    name.as_str()
                );
                Ok(())
            })();
            if let Err(err) = published {
                tracing::warn!(?err, %name, "Failed to publish metrics root");
            }
        }
    });

    Arc::try_unwrap(buffer)
        .map(|m| m.into_inner().unwrap_or_else(PoisonError::into_inner))
        .unwrap_or_default()
}

/// Renders collected metrics as Prometheus text-exposition v0.0.4. Every
/// metric name is prefixed with `nativelink_`. Numeric metrics are emitted as
/// gauges; string metrics are dropped from the numeric output (they carry no
/// meaningful Prometheus value). Duplicate names are summed so the output
/// stays valid.
#[must_use]
pub fn render_prometheus(metrics: &[CollectedMetric]) -> String {
    // Aggregate by final name, summing duplicate numeric samples and keeping
    // the first help string.
    struct Series {
        help: String,
        total: u64,
    }
    let mut series: BTreeMap<String, Series> = BTreeMap::new();

    for metric in metrics {
        let CollectedValue::Counter(value) = metric.value else {
            // Skip string-valued metrics; they are not numerically meaningful.
            continue;
        };
        let full = format!("nativelink_{}", sanitize(&metric.name));
        series
            .entry(full)
            .and_modify(|s| s.total = s.total.saturating_add(value))
            .or_insert_with(|| Series {
                help: metric.help.clone(),
                total: value,
            });
    }

    let mut out = String::new();
    for (name, s) in series {
        // Help text must not contain newlines; collapse them.
        let help = s.help.replace(['\n', '\r'], " ");
        let _ = writeln!(out, "# HELP {name} {help}");
        // Everything numeric is reported as a gauge: values published here are
        // point-in-time snapshots (store sizes, queue depths) rather than
        // guaranteed-monotonic counters.
        let _ = writeln!(out, "# TYPE {name} gauge");
        let _ = writeln!(out, "{name} {}", s.total);
    }
    out
}

#[cfg(test)]
mod tests {
    use nativelink_metric::{
        MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
        RootMetricsComponent, group,
    };

    use super::*;

    #[derive(Default)]
    struct Leaf {
        hits: core::sync::atomic::AtomicU64,
        label: String,
    }

    impl MetricsComponent for Leaf {
        fn publish(
            &self,
            _kind: MetricKind,
            field_metadata: MetricFieldData,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            let _enter = group!(field_metadata.name).entered();
            publish!(
                "hits",
                &self.hits,
                MetricKind::Counter,
                "Number of hits.".to_string()
            );
            publish!(
                "label",
                &self.label,
                MetricKind::String,
                "A label.".to_string()
            );
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    #[derive(MetricsComponent, Default)]
    struct Root {
        #[metric(group = "leaf_a")]
        leaf_a: Leaf,
        #[metric(group = "leaf_b")]
        leaf_b: Leaf,
    }

    impl RootMetricsComponent for Root {}

    #[test]
    fn collects_nested_tree_and_renders() {
        let root = Root::default();
        root.leaf_a
            .hits
            .store(7, core::sync::atomic::Ordering::Relaxed);
        root.leaf_b
            .hits
            .store(3, core::sync::atomic::Ordering::Relaxed);

        let roots: Vec<(String, Arc<dyn RootMetricsComponent>)> =
            vec![("stores".to_string(), Arc::new(root))];
        let metrics = collect(&roots);

        // The root name ("stores") is the outermost group; the derive
        // `#[metric(group = ...)]` opens a group span AND the manual
        // `Leaf::publish` re-groups on `field_metadata.name`, so the leaf group
        // appears twice — this mirrors how real hand-written components (e.g.
        // `AsyncCounterWrapper`) publish.
        let a = metrics
            .iter()
            .find(|m| m.name == "stores_leaf_a_leaf_a_hits")
            .expect("leaf_a hits present");
        assert_eq!(a.value, CollectedValue::Counter(7));
        let b = metrics
            .iter()
            .find(|m| m.name == "stores_leaf_b_leaf_b_hits")
            .expect("leaf_b hits present");
        assert_eq!(b.value, CollectedValue::Counter(3));

        let rendered = render_prometheus(&metrics);
        assert!(
            rendered.contains("nativelink_stores_leaf_a_leaf_a_hits 7"),
            "rendered output:\n{rendered}"
        );
        assert!(rendered.contains("# TYPE nativelink_stores_leaf_a_leaf_a_hits gauge"));
        assert!(rendered.contains("nativelink_stores_leaf_b_leaf_b_hits 3"));
        // String metrics are dropped from numeric output.
        assert!(!rendered.contains("label"));
    }

    #[test]
    fn sanitize_replaces_illegal_chars() {
        assert_eq!(sanitize("a.b-c/d"), "a_b_c_d");
        assert_eq!(sanitize("ok_name:1"), "ok_name:1");
    }

    #[test]
    fn duplicate_names_are_summed() {
        let metrics = vec![
            CollectedMetric {
                name: "dup".to_string(),
                value: CollectedValue::Counter(2),
                help: "h".to_string(),
            },
            CollectedMetric {
                name: "dup".to_string(),
                value: CollectedValue::Counter(5),
                help: "h".to_string(),
            },
        ];
        let rendered = render_prometheus(&metrics);
        assert!(rendered.contains("nativelink_dup 7"), "{rendered}");
    }
}

use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use std::env;
use std::time::Duration;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::prelude::*;

const DEFAULT_MAX_QUEUE_SIZE: usize = 131_072;
const DEFAULT_MAX_EXPORT_BATCH_SIZE: usize = 4_096;
const DEFAULT_SCHEDULE_DELAY_MS: u64 = 200;

pub struct BenchTracing {
    provider: SdkTracerProvider,
}

impl Drop for BenchTracing {
    fn drop(&mut self) {
        let _ = self.provider.shutdown();
    }
}

pub fn init(service_name: &'static str) -> Option<BenchTracing> {
    if !bool_env("OZONE_BENCH_OTEL", false) {
        return None;
    }

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("build OTLP span exporter");
    let batch_processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_max_queue_size(usize_env("OTEL_BSP_MAX_QUEUE_SIZE", DEFAULT_MAX_QUEUE_SIZE))
                .with_max_export_batch_size(usize_env(
                    "OTEL_BSP_MAX_EXPORT_BATCH_SIZE",
                    DEFAULT_MAX_EXPORT_BATCH_SIZE,
                ))
                .with_scheduled_delay(Duration::from_millis(u64_env(
                    "OTEL_BSP_SCHEDULE_DELAY",
                    DEFAULT_SCHEDULE_DELAY_MS,
                )))
                .build(),
        )
        .build();
    let provider = SdkTracerProvider::builder()
        .with_resource(Resource::builder().with_service_name(service_name).build())
        .with_span_processor(batch_processor)
        .build();
    let tracer = provider.tracer(service_name);
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(targets_filter()),
    );

    tracing::subscriber::set_global_default(subscriber).expect("install benchmark tracer");
    global::set_tracer_provider(provider.clone());

    Some(BenchTracing { provider })
}

fn bool_env(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|value| match value.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" => Some(true),
            "0" | "false" | "FALSE" | "no" | "NO" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn usize_env(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn u64_env(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn targets_filter() -> Targets {
    if let Some(targets) = env::var("OZONE_BENCH_OTEL_TARGETS")
        .ok()
        .and_then(|value| value.parse().ok())
    {
        return targets;
    }

    Targets::new()
        .with_default(LevelFilter::OFF)
        .with_target("ozone_rust", LevelFilter::INFO)
        .with_target("io", LevelFilter::INFO)
        .with_target("rpc", LevelFilter::INFO)
}

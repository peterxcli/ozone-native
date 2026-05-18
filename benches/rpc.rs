use criterion::{criterion_group, criterion_main, BatchSize, Criterion, SamplingMode};
use futures::future::join_all;
use ozone_rust::{Client, ClientBuilder, ClientConfig, OzoneClient};
use std::env;
use tokio::runtime::Runtime;
use tracing::Instrument;
use uuid::Uuid;

#[path = "common/bench_cluster.rs"]
mod bench_cluster;
#[path = "common/bench_tracing.rs"]
mod bench_tracing;
#[path = "common/java_bench.rs"]
mod java_bench;

use bench_cluster::BenchCluster;
use java_bench::JavaBenchClient;

const DEFAULT_PARALLELISM: usize = 100;

struct BenchEnv {
    parallelism: usize,
}

impl BenchEnv {
    fn from_env() -> Self {
        Self {
            parallelism: usize_env("OZONE_BENCH_RPC_PARALLELISM", DEFAULT_PARALLELISM),
        }
    }
}

fn usize_env(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

async fn build_clients(cluster: &BenchCluster) -> (OzoneClient, Client) {
    let config = ClientConfig {
        host_override: cluster.host_override.clone(),
        ..ClientConfig::default()
    };
    let admin = OzoneClient::connect_with_config(&cluster.endpoint, config)
        .await
        .expect("connect admin client");
    let mut builder = ClientBuilder::new().with_url(&cluster.endpoint);
    if let Some(host_override) = &cluster.host_override {
        builder = builder.with_config(vec![("ozone.host.override", host_override.as_str())]);
    }
    let client = builder.build().await.expect("connect compatibility client");
    (admin, client)
}

async fn create_namespace(admin: &OzoneClient) -> (String, String) {
    let volume = format!("bench-{}", Uuid::new_v4().simple());
    let bucket = format!("bucket-{}", Uuid::new_v4().simple());
    admin
        .create_volume(&volume, "ozone", "ozone")
        .await
        .expect("create benchmark volume");
    admin
        .create_bucket(&volume, &bucket)
        .await
        .expect("create benchmark bucket");
    (volume, bucket)
}

async fn create_directory(client: &Client, volume: &str, bucket: &str, key: &str) {
    client
        .mkdirs(volume, bucket, key, 0o755, true)
        .await
        .expect("create benchmark directory");
}

async fn cleanup(
    admin: &OzoneClient,
    client: &Client,
    volume: &str,
    bucket: &str,
    directories: &[&str],
) {
    for directory in directories {
        let _ = client.delete(volume, bucket, directory, true).await;
    }
    let _ = admin.delete_bucket(volume, bucket).await;
    let _ = admin.delete_volume(volume).await;
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn bench(c: &mut Criterion) {
    let env = BenchEnv::from_env();
    let rt = runtime();
    let _tracing = {
        let _enter = rt.enter();
        bench_tracing::init("ozone-rust-rpc-bench")
    };
    let cluster = BenchCluster::start();
    let (admin, client) = rt.block_on(build_clients(&cluster));
    let (volume, bucket) = rt.block_on(create_namespace(&admin));
    let key = "bench-rpc-dir";
    let mut java = JavaBenchClient::start(&cluster, 1, env.parallelism);

    rt.block_on(create_directory(&client, &volume, &bucket, key));

    let mut group = c.benchmark_group("rpc");
    group.bench_function("getFileInfo-native", |b| {
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            let volume = volume.clone();
            let bucket = bucket.clone();
            async move {
                client
                    .get_file_info(&volume, &bucket, key)
                    .await
                    .expect("get benchmark file info")
            }
            .instrument(tracing::info_span!("bench.rpc.get_file_info"))
        })
    });
    group.bench_function("getFileInfo-java", |b| {
        b.iter_custom(|iterations| java.measure("getFileStatus", iterations))
    });

    let parallelism = env.parallelism;
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("getFileInfo-parallel", |b| {
        b.to_async(&rt).iter_batched(
            || (0..parallelism).collect::<Vec<_>>(),
            |request_indexes| {
                let client = client.clone();
                let volume = volume.clone();
                let bucket = bucket.clone();
                async move {
                    async move {
                        let requests = request_indexes
                            .into_iter()
                            .map(|request_index| {
                                let client = client.clone();
                                let volume = volume.clone();
                                let bucket = bucket.clone();
                                async move { client.get_file_info(&volume, &bucket, key).await }
                                    .instrument(tracing::info_span!(
                                        "bench.rpc.get_file_info_parallel",
                                        request_index
                                    ))
                            })
                            .collect::<Vec<_>>();
                        for result in join_all(requests).await {
                            result.expect("get benchmark file info");
                        }
                    }
                    .instrument(tracing::info_span!(
                        "bench.rpc.get_file_info_parallel_batch",
                        parallelism
                    ))
                    .await;
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("getFileInfo-parallel-java", |b| {
        b.iter_custom(|iterations| java.measure("getFileStatusParallel", iterations))
    });
    group.finish();

    rt.block_on(cleanup(&admin, &client, &volume, &bucket, &[key]));
}

criterion_group!(benches, bench);
criterion_main!(benches);

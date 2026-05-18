use bytes::{BufMut, Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use ozone_rust::{Client, ClientBuilder, ClientConfig, OzoneClient, WriteOptions};
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

const DEFAULT_FILE_MIB: usize = 128;
const DEFAULT_READ_SAMPLES: usize = 50;
const DEFAULT_WRITE_SAMPLES: usize = 10;
const DEFAULT_NATIVE_MAX_WRITE_RETRIES: usize = 50;

struct BenchEnv {
    file_mib: usize,
    read_samples: usize,
    write_samples: usize,
    native_chunk_size: usize,
    watch_for_commit: bool,
    native_max_write_retries: usize,
}

impl BenchEnv {
    fn from_env() -> Self {
        Self {
            file_mib: usize_env("OZONE_BENCH_FILE_MIB", DEFAULT_FILE_MIB),
            read_samples: usize_env("OZONE_BENCH_READ_SAMPLES", DEFAULT_READ_SAMPLES),
            write_samples: usize_env("OZONE_BENCH_WRITE_SAMPLES", DEFAULT_WRITE_SAMPLES),
            native_chunk_size: usize_env(
                "OZONE_BENCH_NATIVE_CHUNK_SIZE",
                ClientConfig::default().chunk_size,
            ),
            watch_for_commit: bool_env("OZONE_BENCH_WATCH_FOR_COMMIT", false),
            native_max_write_retries: usize_env(
                "OZONE_BENCH_NATIVE_MAX_WRITE_RETRIES",
                DEFAULT_NATIVE_MAX_WRITE_RETRIES,
            ),
        }
    }
}

fn usize_env(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
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

async fn build_clients(cluster: &BenchCluster, env: &BenchEnv) -> (OzoneClient, Client) {
    let config = ClientConfig {
        host_override: cluster.host_override.clone(),
        chunk_size: env.native_chunk_size,
        watch_for_commit: env.watch_for_commit,
        max_write_retries: env.native_max_write_retries,
        ..ClientConfig::default()
    };
    let admin = OzoneClient::connect_with_config(&cluster.endpoint, config)
        .await
        .expect("connect admin client");
    let builder = ClientBuilder::new().with_url(&cluster.endpoint);
    let watch_for_commit = env.watch_for_commit.to_string();
    let max_write_retries = env.native_max_write_retries.to_string();
    let native_chunk_size = env.native_chunk_size.to_string();
    let mut configs = vec![
        ("ozone.chunk.size", native_chunk_size.as_str()),
        ("ozone.watch.for.commit", watch_for_commit.as_str()),
        ("ozone.max.write.retries", max_write_retries.as_str()),
    ];
    if let Some(host_override) = &cluster.host_override {
        configs.push(("ozone.host.override", host_override.as_str()));
    }
    let client = builder
        .with_config(configs)
        .build()
        .await
        .expect("connect compatibility client");
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

async fn write_file(client: &Client, volume: &str, bucket: &str, key: &str, data: Bytes) {
    let mut writer = client
        .create(
            volume,
            bucket,
            key,
            WriteOptions::default().overwrite(true).create_parent(true),
        )
        .await
        .expect("create benchmark file");
    writer.write(data).await.expect("write benchmark data");
    writer.close().await.expect("close benchmark writer");
}

async fn cleanup(admin: &OzoneClient, volume: &str, bucket: &str, keys: &[&str]) {
    for key in keys {
        let _ = admin.delete_key(volume, bucket, key).await;
    }
    let _ = admin.delete_bucket(volume, bucket).await;
    let _ = admin.delete_volume(volume).await;
}

fn bench_data(file_mib: usize) -> Bytes {
    let byte_len = file_mib * 1024 * 1024;
    let mut data = BytesMut::with_capacity(byte_len);
    for value in 0..byte_len / 4 {
        data.put_u32(value as u32);
    }
    data.freeze()
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
        bench_tracing::init("ozone-rust-io-bench")
    };
    let cluster = BenchCluster::start();
    let (admin, client) = rt.block_on(build_clients(&cluster, &env));
    let (volume, bucket) = rt.block_on(create_namespace(&admin));
    let read_key = "bench-read";
    let write_key = "bench-write";
    let data = bench_data(env.file_mib);
    let mut java = JavaBenchClient::start(&cluster, env.file_mib, 1);

    rt.block_on(write_file(
        &client,
        &volume,
        &bucket,
        read_key,
        data.clone(),
    ));

    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.sample_size(env.read_samples);
    group.bench_function("read-native", |b| {
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            let volume = volume.clone();
            let bucket = bucket.clone();
            async move {
                let reader = client.read(&volume, &bucket, read_key).await.unwrap();
                reader.read_range(0, reader.file_length()).await.unwrap()
            }
            .instrument(tracing::info_span!("bench.io.read"))
        })
    });
    group.bench_function("read-java", |b| {
        b.iter_custom(|iterations| java.measure("read", iterations))
    });
    group.finish();

    let mut group = c.benchmark_group("write");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.sample_size(env.write_samples);
    group.bench_function("write-native", |b| {
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            let volume = volume.clone();
            let bucket = bucket.clone();
            let data = data.clone();
            async move {
                write_file(&client, &volume, &bucket, write_key, data).await;
            }
            .instrument(tracing::info_span!("bench.io.write"))
        })
    });
    group.bench_function("write-java", |b| {
        b.iter_custom(|iterations| java.measure("write", iterations))
    });
    group.finish();

    rt.block_on(cleanup(&admin, &volume, &bucket, &[read_key, write_key]));
}

criterion_group!(benches, bench);
criterion_main!(benches);

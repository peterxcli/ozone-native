use std::fs;

#[test]
fn criterion_benchmarks_are_wired_into_cargo() {
    let manifest = fs::read_to_string("Cargo.toml").expect("Cargo.toml");

    assert!(manifest.contains("criterion"));
    assert!(manifest.contains("name = \"io\""));
    assert!(manifest.contains("name = \"rpc\""));
    assert!(manifest.contains("harness = false"));
    assert!(fs::metadata("benches/io.rs").is_ok());
    assert!(fs::metadata("benches/rpc.rs").is_ok());
    assert!(fs::metadata("benches/common/bench_cluster.rs").is_ok());
    assert!(fs::metadata("benches/common/java_bench.rs").is_ok());
    assert!(fs::metadata("benches/common/bench_tracing.rs").is_ok());
    assert!(fs::metadata("bench/miniozone/pom.xml").is_ok());
    assert!(fs::metadata(
        "bench/miniozone/src/main/java/dev/ozonerust/bench/MiniOzoneClusterLauncher.java"
    )
    .is_ok());
    assert!(fs::metadata(
        "bench/miniozone/src/main/java/dev/ozonerust/bench/JavaOzoneClientBenchmark.java"
    )
    .is_ok());

    let io_bench = fs::read_to_string("benches/io.rs").expect("benches/io.rs");
    let rpc_bench = fs::read_to_string("benches/rpc.rs").expect("benches/rpc.rs");
    let bench_cluster =
        fs::read_to_string("benches/common/bench_cluster.rs").expect("bench_cluster.rs");
    let bench_tracing =
        fs::read_to_string("benches/common/bench_tracing.rs").expect("bench_tracing.rs");
    let miniozone_pom = fs::read_to_string("bench/miniozone/pom.xml").expect("miniozone pom");
    let java_bench = fs::read_to_string(
        "bench/miniozone/src/main/java/dev/ozonerust/bench/JavaOzoneClientBenchmark.java",
    )
    .expect("JavaOzoneClientBenchmark.java");
    let miniozone_launcher = fs::read_to_string(
        "bench/miniozone/src/main/java/dev/ozonerust/bench/MiniOzoneClusterLauncher.java",
    )
    .expect("MiniOzoneClusterLauncher.java");
    assert!(io_bench.contains("BenchCluster"));
    assert!(io_bench.contains("read-java"));
    assert!(io_bench.contains("write-java"));
    assert!(io_bench.contains("OZONE_BENCH_NATIVE_MAX_WRITE_RETRIES"));
    assert!(rpc_bench.contains("BenchCluster"));
    assert!(rpc_bench.contains("getFileInfo-java"));
    assert!(bench_cluster.contains("OZONE_BENCH_MINIOZONE_LOG_DIR"));
    assert!(bench_cluster.contains("OZONE_BENCH_MINIOZONE_LOG_LEVEL"));
    assert!(bench_cluster.contains("MiniOzoneCluster log:"));
    assert!(bench_cluster.contains("org.slf4j.simpleLogger.defaultLogLevel"));
    assert!(bench_tracing.contains("BatchSpanProcessor"));
    assert!(bench_tracing.contains("OZONE_BENCH_OTEL_TARGETS"));
    assert!(miniozone_pom.contains("slf4j-simple"));
    assert!(miniozone_launcher.contains("OZONE_BENCH_OTEL"));
    assert!(miniozone_launcher.contains("ozone.tracing.enabled"));
    assert!(java_bench.contains("ozone.client.stream.readblock.enable"));
}

# ozone-rust

`ozone-rust` is an experimental native Rust client for Apache Ozone. It talks
to the Ozone Manager over gRPC and reads/writes container data through the
DataNode and RATIS protocols.

## Supported Ozone features

This checklist tracks what is implemented today and what is still open. Checked
items mean the feature has a public API or concrete implementation in this
crate, not that every edge case has production-grade coverage yet.

### Ozone operations

- [x] Connect to a single Ozone Manager endpoint over gRPC
- [x] Create, inspect, list, and delete volumes
- [x] Create, inspect, list, and delete buckets
- [x] Create, write, read, inspect, list, and delete keys
- [x] Create files with parent-directory creation and overwrite controls
- [x] Read files through Ozone file lookup
- [x] Get file status
- [x] List file status recursively and page through large listings
- [x] Create directories
- [x] Delete keys/files
- [x] Set file modification and access times
- [x] Get, add, remove, replace, and clear ACLs on bucket/key objects
- [x] Traverse FileSystemOptimized buckets for key listings
- [ ] Rename keys or files
- [ ] Append to existing files
- [ ] Multipart upload APIs
- [ ] Object tag APIs
- [ ] Snapshot
- [ ] trash
- [ ] lease recovery
- [ ] tenant
- [ ] quota repair APIs

### Data transfer

- [x] Read key blocks from DataNodes
- [x] Write blocks through RATIS `WriteChunk` and `PutBlock`
- [x] Commit keys after block writes
- [x] Watch RATIS log indexes for majority commit
- [x] Configurable chunk, flush, and stream-window sizes
- [x] Optional `PutBlock` piggybacking on the last chunk in a flush
- [x] Retry unacknowledged write tails
- [x] Reallocate blocks with an exclude list after write failures
- [x] RATIS replication factor `ONE` or `THREE` at file creation time
- [ ] Secure block-token encoding
- [ ] Checksum generation and validation
- [ ] Erasure-coded reads and writes
- [ ] True streaming file reads from the public `FileReader`
- [x] True streaming file writes from the public `FileWriter`
- [ ] Pooled block writes

### HDFS-compatible client surface

- [x] `ClientBuilder::with_url`
- [x] Runtime config map via `ClientBuilder::with_config`
- [x] `fs.defaultFS` endpoint resolution, including `o3fs://` authorities
- [x] `WriteOptions` for block size, replication, permissions, overwrite, and parent creation
- [x] `read`, `create`, `get_file_info`, `list_status`, and `list_status_iter`
- [x] `mkdirs`, `delete`, and `set_times`
- [x] ACL operations: modify, remove entries, remove default ACLs, set ACLs, clear ACLs, get ACL status
- [x] Range reads over already-loaded file data
- [ ] `append`
- [ ] `rename`
- [ ] `set_owner`
- [ ] `set_permission`
- [ ] `set_replication`
- [ ] `get_content_summary`
- [ ] `glob_status`
- [ ] Hadoop XML config discovery through `ClientBuilder::with_config_dir`
- [ ] External I/O runtime integration

### Client settings

- [x] `ozone.chunk.size`
- [x] `ozone.stream.flush.size`
- [x] `ozone.stream.window.size`
- [x] `ozone.read.response.size`
- [x] `ozone.watch.for.commit`
- [x] `ozone.max.write.retries`
- [x] `ozone.enable.put.block.piggybacking`
- [x] `ozone.host.override`
- [ ] Ozone Manager HA and failover discovery
- [ ] Kerberos, delegation tokens, and SASL authentication
- [ ] TLS/auth configuration beyond endpoint transport support

### Benchmarks and test infrastructure

- [x] Criterion RPC and I/O benchmarks
- [x] Automatic benchmark cluster startup with Ozone `MiniOzoneCluster`
- [x] External benchmark cluster override with `OZONE_OM_ENDPOINT`
- [x] Java Ozone client baseline in the same Criterion benchmark run
- [x] Benchmark tuning through environment variables
- [ ] CI benchmark baselines
- [ ] HDFS comparison harness

## Building

```sh
cargo build
```

## Running tests

Unit tests run without an Ozone cluster:

```sh
cargo test
```

Integration tests are ignored by default and require a local Ozone cluster plus
`OZONE_OM_ENDPOINT`:

```sh
cargo test --test ozone_cluster -- --ignored
```

## Running benchmarks

The Criterion benchmarks mirror the I/O and RPC benchmark shape from
`hdfs-native`. By default they start an in-process Ozone
`MiniOzoneCluster` through a small Java launcher, run both the native Rust
client and Apache Ozone's Java client against it, and tear the cluster down
when the benchmark process exits:

```sh
cargo bench
```

`cargo bench --bench io` runs `read-native`, `read-java`, `write-native`, and
`write-java`. `cargo bench --bench rpc` runs `getFileInfo-native`,
`getFileInfo-java`, `getFileInfo-parallel`, and
`getFileInfo-parallel-java`.

The first run may take a while because Maven builds the Ozone mini-cluster
modules from the vendored `ozone/` source tree. To benchmark against an
already-running cluster instead, set `OZONE_OM_ENDPOINT` for the Rust gRPC
endpoint and `OZONE_OM_RPC_ENDPOINT` for the Java client RPC endpoint.

Optional tuning variables:

- `OZONE_OM_ENDPOINT`: use an external OM gRPC endpoint instead of starting `MiniOzoneCluster`.
- `OZONE_OM_RPC_ENDPOINT`: use an external OM Hadoop RPC endpoint for the Java Ozone client baseline. The mini-cluster path discovers this automatically.
- `OZONE_HOST_OVERRIDE`: force DataNode hostnames, useful for Docker clusters exposed on localhost. The mini-cluster path defaults this to `127.0.0.1`.
- `OZONE_BENCH_DATANODES`: datanode count for the mini-cluster, default `1`.
- `OZONE_BENCH_REPLICATION`: mini-cluster replication factor, `1` or `3`, default `1`.
- `OZONE_BENCH_SKIP_MINIOZONE_BUILD`: set to `1` to skip Maven's Ozone module build when the snapshot dependencies are already installed locally.
- `OZONE_BENCH_MINIOZONE_LOG_DIR`: directory for MiniOzoneCluster Java logs, default `bench/miniozone/target/logs`.
- `OZONE_BENCH_MINIOZONE_LOG_LEVEL`: MiniOzoneCluster Java log level, default `info`; use `debug` or `trace` when chasing protocol failures.
- `OZONE_BENCH_FILE_MIB`: file size for I/O benchmarks, default `128`.
- `OZONE_BENCH_READ_SAMPLES`: Criterion sample count for reads, default `50`.
- `OZONE_BENCH_WRITE_SAMPLES`: Criterion sample count for writes, default `10`.
- `OZONE_BENCH_WATCH_FOR_COMMIT`: set to `1` to enable RATIS commit-watch during I/O benchmark writes. The benchmark default is `0`.
- `OZONE_BENCH_NATIVE_MAX_WRITE_RETRIES`: retry budget for native Rust benchmark setup and write samples, default `50`.
- `OZONE_BENCH_JAVA_STREAM_READBLOCK_ENABLE`: set to `0` to disable the Java Ozone client's stream read-block API. The benchmark default is `1`.
- `OZONE_BENCH_RPC_PARALLELISM`: concurrent `get_file_info` calls in the parallel RPC benchmark, default `100`.

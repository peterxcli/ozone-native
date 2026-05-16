# HDFS-Native-Compatible Ozone API Design

Date: 2026-05-16

## Summary

Add a public compatibility layer that exposes a Rust API very close to the
`hdfs-native` crate while keeping Ozone's native volume, bucket, and key model.

The new layer will provide familiar types such as `ClientBuilder`, `Client`,
`WriteOptions`, `FileReader`, `FileWriter`, `FileStatus`, `ContentSummary`, and
ACL types. Methods will use explicit Ozone coordinates instead of path strings:

```rust
let client = ClientBuilder::new()
    .with_url("http://127.0.0.1:9874")
    .build()
    .await?;

let mut reader = client.read("volume", "bucket", "key").await?;
let data = reader.read(1024).await?;

let mut writer = client
    .create("volume", "bucket", "key", WriteOptions::default())
    .await?;
writer.write(bytes::Bytes::from_static(b"hello")).await?;
writer.close().await?;
```

The compatibility layer will expose the full comparable `hdfs-native` method
surface. Operations with direct Ozone support will be implemented. Operations
without a true Ozone equivalent will return explicit `Error::Unsupported`
messages rather than silently pretending to provide HDFS semantics.

## Goals

- Make the crate approachable for users familiar with `hdfs-native`.
- Preserve Ozone's explicit `volume`, `bucket`, `key` API shape.
- Keep the existing `OzoneClient` API working unchanged.
- Expose the full comparable `hdfs-native` surface for migration stability.
- Implement capability-backed behavior where Ozone supports it.
- Return method-specific `Unsupported` errors for semantic gaps.
- Add tests that lock down the public API, reader/writer behavior, and OM
  request mapping.

## Non-Goals

- Do not introduce HDFS path parsing or `viewfs` behavior.
- Do not load Hadoop XML configuration files in this implementation.
- Do not emulate HDFS-only semantics when Ozone has no equivalent operation.
- Do not remove or rename the existing `OzoneClient` methods.
- Do not implement append, rename, POSIX owner/group changes, POSIX permission
  mutation, replication mutation, content summary, or globbing in this
  implementation.

## Public API

### Exports

`src/lib.rs` will re-export:

- `Client`
- `ClientBuilder`
- `WriteOptions`
- `FileReader`
- `FileWriter`
- `FileStatus`
- `ContentSummary`
- ACL compatibility types
- existing `OzoneClient`, `ClientConfig`, `Error`, and `Result`

This keeps current users whole while allowing new code to use the compatibility
names.

### ClientBuilder

`ClientBuilder` will be an async builder for `Client`.

Supported builder methods:

- `new()`
- `with_url(url)`
- `with_config(config)`
- `with_io_runtime(runtime)`
- `build().await`

`with_url` maps to the Ozone Manager endpoint used today by
`OzoneClient::connect`. `with_config` accepts string key/value overrides for API
parity and for Ozone-specific settings. The supported keys are:

- `ozone.chunk.size`
- `ozone.stream.flush.size`
- `ozone.stream.window.size`
- `ozone.read.response.size`
- `ozone.watch.for.commit`
- `ozone.max.write.retries`
- `ozone.enable.put.block.piggybacking`
- `ozone.enable.incremental.chunk.list`
- `ozone.host.override`

`with_config_dir` will be exposed for parity but will return
`Error::Unsupported` from `build` if it is set, because this crate does not yet
load Hadoop configuration files.

### Client

`Client` wraps the existing `OzoneClient` and delegates all supported work to
the existing OM, datanode, and Ratis layers.

Supported coordinate shape:

```rust
client.read(volume, bucket, key).await?;
client.create(volume, bucket, key, WriteOptions::default()).await?;
client.list_status(volume, bucket, key, recursive).await?;
client.delete(volume, bucket, key, recursive).await?;
```

Volume and bucket administration remains available through the existing
`OzoneClient` methods. `Client` will not add duplicate volume or bucket
administration methods in this implementation.

### WriteOptions

`WriteOptions` mirrors `hdfs-native`:

- `block_size: Option<u64>`
- `replication: Option<u32>`
- `permission: u32`
- `overwrite: bool`
- `create_parent: bool`

Initial behavior:

- `overwrite` maps to Ozone `CreateFileRequest.isOverwrite`.
- `create_parent` maps to Ozone `CreateFileRequest.isRecursive`.
- `block_size` is accepted but ignored in this implementation.
- `replication` is accepted but ignored in this implementation.
- `permission` is accepted but ignored because Ozone file create does not expose
  a POSIX permission field in the OM request.

Ignoring accepted options must be documented on the type and covered by tests
where practical.

## Operation Mapping

### Implemented Operations

`get_file_info(volume, bucket, key)`:

- Use OM `GetFileStatus`.
- Convert `OzoneFileStatusProto` into compatibility `FileStatus`.
- Return a not-found-flavored Ozone error when OM reports missing data.

`list_status(volume, bucket, key, recursive)`:

- Use OM `ListStatus`.
- Provide both vector-returning and iterator/stream-like variants.
- Convert Ozone status records into compatibility `FileStatus`.

`read(volume, bucket, key)`:

- Use OM `LookupFile` where filesystem semantics are needed, or existing
  key lookup as fallback where equivalent.
- Return `FileReader`.
- The reader will eagerly fetch bytes with the existing datanode read path,
  then provide the `hdfs-native` cursor API on top.

`create(volume, bucket, key, options)`:

- Use OM `CreateFile` to open the file.
- Return `FileWriter`.
- The writer will buffer data and commit through the existing `put_key_bytes`
  machinery on `close`.
- Streaming writes are out of scope for this implementation.

`mkdirs(volume, bucket, key, permission, create_parent)`:

- Use OM `CreateDirectory`.
- Accept `permission` for API parity, but ignore it unless Ozone gains a
  compatible request field in a future design.
- `create_parent` maps to the recursive intent when available.

`delete(volume, bucket, key, recursive)`:

- Use the Ozone key/file delete path.
- `recursive` is accepted for API parity. This implementation delegates to the
  existing key delete path and returns `Error::Unsupported` for recursive
  directory deletion if the target cannot be deleted as a key.

`set_times(volume, bucket, key, mtime, atime)`:

- Use OM `SetTimes`.

ACL operations:

- `get_acl_status`
- `modify_acl_entries`
- `remove_acl_entries`
- `remove_default_acl`
- `remove_acl`
- `set_acl`

These map to OM `GetAcl`, `AddAcl`, `RemoveAcl`, and `SetAcl` using Ozone ACL
objects for the target volume, bucket, or key. The compatibility ACL types will
be close to `hdfs-native`, with conversion functions that make the Ozone
permission model explicit.

### Explicit Unsupported Operations

These methods will exist for API parity and return `Error::Unsupported` with
method-specific messages in this implementation:

- `append(volume, bucket, key)`
- `rename(src_volume, src_bucket, src_key, dst_volume, dst_bucket, dst_key, overwrite)`
- `set_owner(volume, bucket, key, owner, group)`
- `set_permission(volume, bucket, key, permission)`
- `set_replication(volume, bucket, key, replication)`
- `get_content_summary(volume, bucket, key)`
- `glob_status(volume, bucket, pattern)`

This keeps migration code compiling while making unsupported behavior visible at
runtime.

## Types

### FileReader

`FileReader` will expose:

- `file_length()`
- `remaining()`
- `seek(pos)`
- `tell()`
- `read(len).await`
- `read_buf(buf).await`
- `read_range(offset, len).await`
- `read_range_buf(buf, offset).await`
- `read_range_stream(offset, len)`

`FileReader` owns a `bytes::Bytes` buffer. Cursor operations are local and
deterministic. Range methods must panic on out-of-range access to match
`hdfs-native` behavior.

### FileWriter

`FileWriter` will expose:

- `write(bytes).await`
- `close().await`

`FileWriter` buffers bytes and writes once during `close`. Dropped, unclosed
writers do not commit partial data.

### FileStatus

`FileStatus` will mirror `hdfs-native` fields:

- `volume`
- `bucket`
- `key`
- `length`
- `isdir`
- `permission`
- `owner`
- `group`
- `modification_time`
- `access_time`
- `replication`
- `blocksize`

Ozone does not always provide every HDFS field. Missing values use these
defaults:

- `permission`: `0`
- `group`: empty string
- `access_time`: `0`
- `replication`: `None`
- `blocksize`: value from Ozone status when present, otherwise `None`

### ContentSummary

`ContentSummary` will be exposed for parity, but
`Client::get_content_summary` returns `Error::Unsupported` in this
implementation.

## Error Handling

Keep the existing `Error` enum and add only variants that serve this API:

- unsupported method messages continue to use `Error::Unsupported`.
- invalid builder configuration uses a new `Error::InvalidArgument` variant.
- status conversion failures return `Error::InvalidState`; only
  `FileReader` out-of-range range reads intentionally panic to match
  `hdfs-native`.

Unsupported errors must name the method and the missing Ozone capability.

## Module Layout

Add focused compatibility modules:

- `src/api.rs`: `ClientBuilder`, `Client`, `WriteOptions`, operation methods
- `src/file.rs`: `FileReader`, `FileWriter`
- `src/status.rs`: `FileStatus`, `ContentSummary`, status conversion helpers
- `src/acl.rs`: compatibility ACL types and Ozone ACL conversion helpers

Modify existing modules:

- `src/lib.rs`: re-export compatibility API
- `src/client.rs`: expose helper methods as needed without breaking
  `OzoneClient`
- `src/om.rs`: add OM methods for filesystem operations and ACLs
- `src/error.rs`: add targeted argument errors only if necessary

## Testing Strategy

Follow test-first implementation for each phase.

API compile tests:

- `ClientBuilder::new().with_url(...).build().await` returns `Client`.
- `WriteOptions` builder methods mirror `hdfs-native`.
- Every compatibility method is callable with explicit Ozone coordinates.

Reader tests:

- `file_length`, `remaining`, `tell`, and `seek` update correctly.
- sequential `read` advances the cursor.
- `read_buf` fills only the readable range.
- range reads do not modify the cursor.
- out-of-range reads panic.

Writer tests:

- `write` appends to the pending buffer.
- `close` commits once.
- repeated `close` is idempotent.
- dropped unclosed writer does not commit.

Status and ACL conversion tests:

- Ozone file status converts to the compatibility `FileStatus`.
- Ozone directory status sets `isdir`.
- ACL type/scope/action conversions are deterministic.

OM request tests:

- new OM methods populate the expected request oneof fields.
- unsupported compatibility methods return `Error::Unsupported` with clear
  messages.

Integration tests:

- create a directory in an FSO bucket.
- create, write, close, read, and delete a file through `Client`.
- list file and directory status through `Client`.
- set times and read back metadata where Ozone exposes it.
- ACL roundtrip where the local cluster supports ACL mutation.

## Implementation Phases

1. Add the public compatibility module, exported types, and unsupported method
   shells behind tests.
2. Implement in-memory `FileReader` and buffered `FileWriter` over existing
   key byte operations.
3. Add OM filesystem methods: `GetFileStatus`, `ListStatus`, `CreateFile`,
   `CreateDirectory`, `LookupFile`, and `SetTimes`.
4. Add ACL compatibility types and OM ACL request methods.
5. Document ignored `WriteOptions` fields in rustdoc and tests.
6. Add integration tests against the local Ozone cluster.

## Trade-Offs

- Exposing all methods early helps migration, but some methods fail at runtime
  with explicit unsupported errors in this implementation.
- Buffering `FileWriter` is less memory-efficient than a streaming writer, but
  it gives the correct lifecycle API quickly and leaves streaming as a future
  replacement behind the same public type.
- Keeping explicit volume, bucket, and key arguments avoids ambiguous path
  parsing and matches the user's chosen API shape, but it intentionally differs
  from `hdfs-native` path-string calls.

# Ozone Rust Put-Key Pipeline Design

Date: 2026-04-10

## Summary

Redesign `OzoneClient::put_key_bytes` to follow the upstream Ozone write model more closely:

- keep key-level progression sequential across blocks
- make the active block fully pipelined at the chunk level
- maintain a sliding window of unacknowledged chunk data
- use a persistent bidirectional Ratis `unordered` request stream instead of opening a new gRPC stream for every request
- add a retry request manager modeled after Apache Ozone PR `#9195` so retries replay only unacknowledged chunks and at most one terminal `putBlock`
- support `putBlock` piggybacking and incremental chunk-list behavior when the server path supports them

This design targets both throughput and recovery behavior. The current Rust client writes one block end-to-end in strict serial order. The new design pipelines chunk writes inside a block, overlaps request/response handling, and retains enough replay state to recover from mid-stream failures without re-sending already committed data.

## Context

Relevant upstream references:

- `hadoop-ozone/client/.../KeyOutputStream.java`
- `hadoop-ozone/client/.../BlockOutputStreamEntryPool.java`
- `hadoop-ozone/client/.../BlockOutputStreamEntry.java`
- `hadoop-hdds/client/.../BlockOutputStream.java`
- `hadoop-hdds/client/.../RatisBlockOutputStream.java`
- `hadoop-hdds/client/.../AbstractCommitWatcher.java`
- Apache Ozone PR `#9195`, which introduced `RetryRequestBatcher`

Upstream behavior is layered:

- key layer chooses and rolls blocks
- block layer pipelines chunk writes and flushes
- commit watcher layer advances an acknowledged frontier and releases buffered data only when contiguous commit progress is observed

The Rust client currently collapses all three layers into a single synchronous loop in `put_key_bytes`, which leaves most of the available performance on the table and makes failure recovery coarse.

## Goals

- Match the upstream layering closely enough that failure handling is understandable from Ozone’s design.
- Increase upload throughput by pipelining chunk writes within a block.
- Avoid per-chunk stream creation cost by reusing a persistent bidirectional Ratis request stream.
- Keep bounded memory by enforcing a byte-based sliding window for unacknowledged data.
- Retry only unacknowledged chunk data after failures.
- Preserve key commit correctness by committing only fully acknowledged block locations.
- Keep the first implementation scoped to RATIS key writes.

## Non-Goals

- Parallel writes to multiple blocks of the same key in the first implementation.
- Erasure-coded write-path support.
- Full parity with every upstream optimization flag and server-version edge case.
- Multipart upload support.

## High-Level Design

### Key Layer

`OzoneClient::put_key_bytes` becomes orchestration only.

For each block:

1. Obtain the block location from preallocated locations or OM `allocate_block`.
2. Create a `BlockWriter` bound to that block and pipeline.
3. Feed block-sized data into the writer.
4. Wait for the block to close cleanly and return its committed location.
5. Move to the next block until all key data is written.
6. Commit the key with OM using the acknowledged block list.

Only one block is active at a time. This matches upstream more closely and keeps replay semantics manageable.

### Block Layer

Introduce a `BlockWriter` responsible for:

- splitting block data into chunk-sized requests
- buffering in-flight chunk data until commit is acknowledged
- issuing `writeChunk` requests asynchronously
- emitting `putBlock` requests at flush boundaries
- waiting for `watch` advancement when the unacknowledged byte window is full
- replaying only the unacknowledged tail after failures

The block writer maintains three byte counters:

- `written_len`: bytes accepted into the block writer
- `flush_len`: highest byte offset covered by a sent `putBlock`
- `acked_len`: highest byte offset durably acknowledged by a watched flush

The block is considered committed only when `acked_len == written_len` and the closing flush succeeds.

### Transport Layer

Replace the current one-request-per-stream code path in `src/ratis.rs` with a persistent unordered request manager.

The manager owns:

- one live gRPC `unordered` request stream for a specific pipeline leader
- a background reply task
- a `call_id -> oneshot` reply map
- stream lifecycle state

Supported operations:

- send `writeChunk`
- send `putBlock`
- send `watch`
- fail all pending waiters if the stream breaks

The manager is bound to the active pipeline. When the manager sees leader redirection, broken transport, or a terminal Ratis/state machine failure, the block writer treats the block as failed and enters retry handling.

## Sliding Window

Windowing is byte-based, not request-count-based.

New config values:

- `chunk_size`: existing field, reused as write chunk size
- `stream_flush_size`: bytes after which a flush boundary is created
- `stream_window_size`: maximum unacknowledged bytes allowed before the writer must wait
- `max_write_retries`: retry attempts for a failed block handoff
- `enable_put_block_piggybacking`
- `enable_incremental_chunk_list`

Proposed defaults:

- `chunk_size = 1 MiB`
- `stream_flush_size = 16 MiB`
- `stream_window_size = 32 MiB`
- `max_write_retries = 5`
- piggybacking and incremental chunk list enabled when capability detection says the server path supports them

Behavior:

- each full chunk is sent immediately
- every time `written_len - flush_len >= stream_flush_size`, send a new flush boundary
- every time `written_len - acked_len >= stream_window_size`, block and wait until `acked_len` advances

This is the Rust equivalent of the upstream `streamBufferSize`, `streamBufferFlushSize`, and `streamBufferMaxSize` behavior.

## Flush, Watch, and Ack Model

Each flush boundary produces:

- a `putBlock` operation that covers data up to `flush_len`
- a log index returned by the Ratis write
- a `watch` request tied to that log index

When a watch succeeds:

- the block writer advances `acked_len` to the flush boundary covered by that log index
- buffered chunk data up to that offset can be released
- the retry window trims replay state up to that offset

Out-of-order watch completion is allowed. The writer must only advance the durable contiguous `acked_len` frontier. This mirrors the upstream commit watcher behavior.

## Retry Request Manager

Add a Rust module analogous to PR `#9195`’s `RetryRequestBatcher`.

Tracked state:

- pending chunk requests ordered by end offset
- the latest outstanding `putBlock` offset
- acknowledged offset
- sent offset

Rules:

- every outbound `writeChunk` registers `(buffer_ref, start_offset, end_offset)`
- every outbound `putBlock` replaces any lower pending `putBlock` offset
- `acknowledge_up_to(offset)` removes or logically skips all chunk requests ending at or before `offset` and clears any pending `putBlock` up to that point
- `optimize_for_retry()` returns:
  - the remaining chunk buffers in order
  - whether a terminal `putBlock` is still needed

The planner does not decide block allocation. It only decides what part of the unacknowledged tail must be replayed.

## Piggybacking and Incremental Chunk List

### Piggybacking

When enabled and supported, the final chunk of a flush or retry batch carries the terminal `putBlock` metadata. This removes one standalone RPC for that boundary.

Fallback behavior:

- if piggybacking is disabled or unsupported, emit normal `writeChunk` requests and one standalone `putBlock`

### Incremental Chunk List

The block writer will maintain:

- `all_chunks` only when full-list mode is required
- `pending_chunks_since_last_put_block` for incremental mode

When incremental mode is enabled, each `putBlock` includes only newly added chunks since the previous successful `putBlock`. After success, the pending incremental list is cleared. On retry, the replay planner reconstructs the correct terminal `putBlock` from the remaining unacknowledged tail.

If capability detection is uncertain, full-list mode wins over optimization.

## Failure Handling

### Failure Classes

1. transport failure on the persistent request stream
2. Ratis leader redirect or leader not ready
3. state machine or datanode container error during `writeChunk` or `putBlock`
4. watch failure after a flush was sent

### Recovery Strategy

For any failure with unacknowledged data:

1. stop accepting new writes into the current block writer
2. wait for any already-returned replies to settle if useful, but do not trust in-flight writes beyond `acked_len`
3. build a retry plan from the retry request manager for the range `(acked_len, written_len]`
4. allocate a replacement block from OM using an exclude list derived from the failed pipeline and any failed nodes if available
5. create a new block writer on the replacement block
6. replay only the unacknowledged chunks in order
7. emit at most one terminal `putBlock` for the replay batch
8. continue streaming the remaining user data

If retries exceed `max_write_retries`, fail the key write and do not call `commit_key`.

### Exclude List Support

Extend `OmClient::allocate_block` to accept an optional exclude list so retries can avoid the failed pipeline or nodes. The first implementation can prioritize pipeline exclusion. Node and container exclusion can be added where the available error surface permits it.

## Capability Detection

The Rust client must not assume piggyback or incremental chunk list support blindly.

Capability detection order:

1. explicit config override
2. pipeline member version metadata if reliable in current protos
3. conservative fallback to disabled

If support cannot be determined, disable the optimization and continue in correct but less efficient mode.

## Module Layout

Planned Rust modules:

- `src/block_writer.rs`
- `src/retry_window.rs`

Existing modules to change:

- `src/client.rs`: key-level orchestration only
- `src/ratis.rs`: persistent unordered request manager and reply demux
- `src/om.rs`: exclude-list-aware block allocation
- `src/util.rs`: helper utilities for flush sizing, capability checks, and block sizing

## Testing Strategy

### Unit Tests

- retry window bookkeeping
- duplicate `putBlock` collapse
- `acknowledge_up_to` trimming
- out-of-order watch completion preserving contiguous `acked_len`
- retry plan generation with and without piggyback
- incremental chunk-list bookkeeping
- request manager reply demultiplexing by `call_id`
- request manager failure propagation to all pending waiters

### Integration Tests

Extend the existing cluster test coverage to include:

- multi-chunk single-block write
- multi-block key write
- flush/window behavior on payloads larger than the ack window
- induced failure during unacknowledged tail replay
- successful recovery onto a replacement block

## Trade-Offs

- This design is more complex than naive block-parallel uploads, but it matches upstream semantics better and gives a cleaner recovery model.
- One active block at a time is a deliberate scope limit. It sacrifices some theoretical parallelism in exchange for easier correctness and alignment with Ozone’s current client behavior.
- Piggybacking and incremental chunk-list support improve performance but are optional optimizations layered over a correct base design.

## Decision

Implement the first version as a sequential-block, chunk-pipelined RATIS writer with:

- persistent unordered request streaming
- byte-based sliding window
- flush/watch/ack frontiers
- retry window optimization inspired by PR `#9195`
- piggybacked terminal `putBlock` and incremental chunk-list support when capability detection allows it

This is the best balance of performance, failure handling, and closeness to upstream Ozone behavior for the Rust client.

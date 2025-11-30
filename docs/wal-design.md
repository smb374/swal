# Write-Ahead Log (WAL) Design

## Overview

This document defines a generic Write-Ahead Log (WAL) system. The WAL provides crash recovery and durability guarantees for storage systems that modify persistent resources (pages, objects, records, etc.). The design is intentionally generic and can be adapted to various use cases including databases, key-value stores, object storage, and file systems.

**Implementation Note:** This library is implemented in Zig and exposed as a shared library with C-compatible interfaces. All documented structures and functions (except opaque handles like `WAL`, `WALTxn`, `WALIterator`) are ABI-compatible with C and follow the C calling convention.

## Design Principles

1. **Generic & Extensible:** No assumptions about resource types (pages, objects, etc.)
2. **User-Defined Record Types:** Applications define their own record type taxonomy
3. **LSN-Based Ordering:** Globally unique Log Sequence Numbers ensure correct ordering
4. **Idempotent Recovery:** Redo operations can be safely applied multiple times
5. **Independent System:** Self-contained with no external dependencies
6. **Lock-Free Append:** Double-buffered design with active writer tracking for safe concurrent appends
7. **Segment-Based Storage:** Log rotation enables bounded segment sizes and easy retirement
8. **Flexible Resource Identification:** Generic 64-bit resource IDs support any addressing scheme
9. **ACID Transactions:** Full transaction support with undo/redo recovery via prev_lsn chains
10. **Production-Grade Durability:** Directory fsync, torn write protection, sector-aligned flushes
11. **Memory-Efficient Recovery:** Two-pass protocol with on-disk link following avoids OOM on large logs

---

## WAL Record Format

### Core Record Structure

```c
enum WALPayloadHashType {
    WAL_PCHK_XXH64 = 0,      // xxHash64 (default, fastest)
    WAL_PCHK_BLAKE2S = 1,    // BLAKE2s 8-byte (cryptographic)
};

struct WALRecordHeader {
    u64 lsn;                  // Log Sequence Number (globally unique, monotonic)
    u64 prev_lsn;             // Previous LSN (for transaction chains, 0 if none)
    u64 resource_id;          // Generic resource identifier (page, object, key hash, etc.)
    u64 txn_id;               // Transaction ID (0 = no transaction context)
    u32 record_len;           // Total record length (header + payload, max 4GB)
    u16 record_type;          // User-defined record type
    u8 payload_hash_type;     // Payload hash algorithm (see WALPayloadHashType)
    u8 _pad[5];               // Padding, initialized to 0
    u32 header_checksum;      // CRC-32C of header (this field is 0 during computation)
    u64 payload_checksum;     // XXH64 or BLAKE2s 8-byte checksum of payload
    // Followed by user-defined payload (variable length)
};
// Size: 56 bytes (naturally aligned to 8-byte boundary)
```

**Endianness:** All multi-byte integers (LSN, txn_id, lengths, checksums) are stored in **Little Endian** format for cross-platform compatibility.

**Checksum Coverage:**

- **header_checksum:** CRC-32C of first 44 bytes (header excluding this field and payload_checksum)
- **payload_checksum:** 64-bit (8-byte) hash of payload data (algorithm specified by `payload_hash_type`)
- Recovery discards records with invalid checksums (protects against partial writes and corruption)

**Payload Hash Algorithm Selection:**

The WAL supports two payload hash algorithms, selectable per-record via the `payload_hash_type` field:

1. **xxHash64 (WAL_PCHK_XXH64 = 0) - Default:**
   - **Performance:** ~15-20 GB/s in software
   - **Collision resistance:** 2^64 (1 in 18 quintillion)
   - **Use case:** General-purpose, high-performance workloads
   - **Trade-off:** Non-cryptographic (vulnerable to intentional collision attacks)
   - **Default choice:** Optimal for most WAL use cases where corruption is random

2. **BLAKE2s 8-byte (WAL_PCHK_BLAKE2S = 1):**
   - **Performance:** ~3-5 GB/s in software
   - **Collision resistance:** Cryptographically secure (2^64 with preimage resistance)
   - **Reference:** RFC 7693 (<https://tools.ietf.org/html/rfc7693>)
   - **Use case:** Security-sensitive applications requiring tamper detection
   - **Trade-off:** 3-4x slower than xxHash64
   - **Production usage:** Linux kernel, WireGuard VPN

**Header Checksum (CRC-32C):**

- Fixed algorithm for all records (no selection needed)
- Hardware-accelerated on x86 (SSE4.2 `_mm_crc32_u*` intrinsics)
- Performance: ~10-20 GB/s with hardware support
- Sufficient for 56-byte header integrity validation

**Selection Guidelines:**

- **Default (xxHash64):** Use for typical database, KV, or object store workloads where performance is critical
- **BLAKE2s:** Use when cryptographic integrity guarantees are required (e.g., security-critical logs, audit trails)

**Field Semantics:**

- **lsn:** Uniquely identifies this log record, assigned sequentially by WAL
- **prev_lsn:** Links records within same transaction (forms chain for undo/rollback)
- **resource_id:** Generic 64-bit identifier for the resource being modified
  - Page-based systems: page number (32-bit) with upper bits for partition/table ID
  - Object stores: object ID
  - Key-value stores: hash of key
  - File systems: inode number
  - User defines the meaning
- **txn_id:** Transaction context (0 = auto-commit or no transactions)
- **record_type:** Application-defined type code (see User-Defined Types section)
- **record_len:** Total size including header and payload (max 4GB)
- **payload_hash_type:** Hash algorithm for payload (0=xxHash64, 1=BLAKE2s)
- **_pad:** Reserved padding bytes, must be initialized to 0
- **header_checksum:** CRC-32C validates header integrity
- **payload_checksum:** xxHash64 or BLAKE2s 8-byte hash validates payload integrity

---

## User-Defined Record Types

Applications define their own record type taxonomy using a `u16` type code. The WAL library treats this as an opaque value.

### Example: Page-Based Database

```c
// Application defines record types
enum DBRecordType {
    // System operations
    DB_CHECKPOINT = 1,
    DB_SHUTDOWN = 2,

    // Transaction control
    DB_TXN_BEGIN = 10,
    DB_TXN_COMMIT = 11,
    DB_TXN_ABORT = 12,

    // BTree operations
    DB_BTREE_SPLIT_BEGIN = 100,
    DB_BTREE_SPLIT_COMPLETE = 101,
    DB_BTREE_INSERT = 102,
    DB_BTREE_DELETE = 103,
    DB_BTREE_UPDATE = 104,
};

// Application-specific payload for insert
struct DBBTreeInsertPayload {
    u16 slot;
    u16 key_len;
    u16 value_len;
    u8 data[];  // [key_bytes][value_bytes]
};
```

### Example: Key-Value Store

```c
enum KVRecordType {
    KV_PUT = 1,
    KV_DELETE = 2,
    KV_CHECKPOINT = 3,
};

struct KVPutPayload {
    u32 key_len;
    u32 value_len;
    u8 data[];  // [key][value]
};
```

### Example: Object Store

```c
enum ObjRecordType {
    OBJ_CREATE = 1,
    OBJ_UPDATE = 2,
    OBJ_DELETE = 3,
    OBJ_METADATA_UPDATE = 4,
};

struct ObjUpdatePayload {
    u64 offset;
    u32 length;
    u8 data[];
};
```

---

## WAL File Format

### Segment Naming

WAL uses multiple segment files with sequential numbering:

- `wal.0001` - First segment
- `wal.0002` - Second segment (created after first segment reaches size limit)
- `wal.0003` - Third segment, etc.

**Benefits:**

- Bounded file sizes (easier filesystem management)
- Simple retirement of old segments after checkpoint
- Sequential numbering simplifies recovery ordering

### Segment Header

Each segment file begins with a 4KB header:

```c
struct WALSegmentHeader {
    u32 magic;                   // 0x57534547 ("WSEG")
    u32 version;                 // WAL format version (1)
    u32 segment_num;             // Sequential segment number (1, 2, 3, ...)
    u8 payload_hash_type;        // Default payload hash (0=xxHash64, 1=BLAKE2s)
    u8 record_alignment;         // Record alignment (8 bytes)
    u8 _reserved[2];             // Reserved for future use

    u64 min_lsn;                 // First LSN in this segment
    u64 max_lsn;                 // Last LSN (updated on rotate/close)
    u64 segment_size;            // Current segment size in bytes (including header)

    // WAL-wide configuration (enables recovery without state file)
    u64 segment_max_size;        // Max segment size before rotation (e.g., 1GB)
    u32 buffer_size;             // Deprecated (use WAL_RECORDS = 8192)
    u32 retention_mode;          // WALRetentionMode enum
    u64 retention_param;         // LSN range or segment count

    u32 checksum;                // CRC-32C of header (excluding this field)
    u8 _pad[4028];               // Pad to 4096 bytes
};
_Static_assert(sizeof(struct WALSegmentHeader) == 4096, "Header must be 4096 bytes");
```

**Key Fields:**

- **min_lsn:** First LSN written to this segment (set on creation)
- **max_lsn:** Last LSN written (updated on rotation or close)
- **segment_size:** Current segment size including header (file offset)
- **payload_hash_type:** Default hash algorithm for records in this segment
  - Records can override this per-record using their own `payload_hash_type` field
  - Segment-level setting provides a default for WAL creation
  - Allows migration between hash algorithms across segment boundaries

**WAL Configuration Fields (for stateless recovery):**

- **segment_max_size:** Copied from WAL struct, used to validate consistency across segments
- **buffer_size:** Deprecated field (always use `WAL_RECORDS = 8192`)
- **retention_mode:** Segment retention policy (enum `WALRetentionMode`)
- **retention_param:** Policy parameter (LSN range or segment count)

**Recovery without state file:**

- Scan directory for all `wal.%08d` files
- Read latest segment header to extract WAL configuration
- Validate all segments have compatible config
- Compute `next_lsn` = max(`max_lsn`) across all segments + 1

### Segment Layout

```
┌─────────────────────────────────────┐
│ WAL Segment Header (4KB)            │  ← Offset 0
├─────────────────────────────────────┤
│ WAL Record 1                        │  ← Offset 4096
│   [WALRecordHeader][Payload]        │
├─────────────────────────────────────┤
│ WAL Record 2 (8-byte aligned)       │
│   [WALRecordHeader][Payload]        │
├─────────────────────────────────────┤
│ ...                                 │
├─────────────────────────────────────┤
│ WAL Record N                        │
└─────────────────────────────────────┘
```

**Alignment:** All records are 8-byte aligned for efficient I/O.

---

## WAL API

### Opaque Handles

**Note:** The following handles are **opaque to C clients**. Users only interact via pointers and should never access internal fields directly.

```c
// OPAQUE: WAL handle - main entry point for all WAL operations
// Fields documented for implementation reference only, not accessible from C API
struct WAL {
    // Segment management
    i32 active_fd;                      // File descriptor for active segment
    i32 dirfd;                          // Directory FD for openat()
    u32 segment_num;                    // Current segment number
    u64 segment_max_size;               // Max segment size before rotation (soft limit, see note)
    u8 payload_hash_type;               // Default payload hash (0=xxHash64, 1=BLAKE2s)
    char *wal_dir;                      // Directory containing segment files
    struct WALSegmentHeader *active_shdr; // Cached active segment header (heap-allocated)

    // Double-buffered write buffers (arena + queue pairs)
    // Each WriteBuffer contains:
    //   - ArenaAllocator: Bulk allocates records for a flush batch
    //   - MPSCQueue: Holds pointers into the arena
    // Benefits: Eliminates malloc lock contention, bulk-free on flush
    struct WriteBuffer {
        void *arena;                    // ArenaAllocator (Zig std.heap.ArenaAllocator)
        void *queue;                    // MPSC queue storing arena pointers
    } buffers[2];
    atomic_u32 active_idx;              // Which buffer is active (0 or 1)
    atomic_u32 active_writers[2];       // Count of threads writing to each buffer
    u32 buffer_size;                    // Deprecated (use WAL_RECORDS instead)

    // Ring buffer for aligned writes (serial, flusher-only)
    void *rb;                           // Accumulates bytes, handles 512-byte alignment

    // LSN management
    atomic_u64 next_lsn;                // Next LSN to assign (monotonic)
    atomic_u64 flushed_lsn;             // Last LSN guaranteed on disk
    u64 last_checkpoint_lsn;            // Last checkpoint LSN

    // Flush coordination
    atomic_u32 flush_in_progress;       // Prevents concurrent flushes
    pthread_mutex_t flush_mutex;        // Serializes flush operations
    pthread_cond_t flush_cond;          // Notifies threads waiting for flush

    // Retention policy
    enum WALRetentionMode retention_mode;
    union {
        u64 retention_lsn_range;        // For WAL_RETAIN_LSN
        u32 retention_count;            // For WAL_RETAIN_COUNT
    };
};

// Note: segment_max_size is a "soft limit" - a single flush batch may exceed this
// limit, but rotation occurs afterward. This ensures records are never split across files.

// OPAQUE: Transaction handle - used for transactional operations
struct WALTxn;

// OPAQUE: Iterator handle - used for sequential WAL traversal
struct WALIterator;
```

**Implementation Architecture:**

The WAL implementation uses several key design patterns (implemented in Zig):

- **Double-buffered Arena + MPSC queues:** Enable lock-free append while flushing
  - Arena allocator (`std.heap.ArenaAllocator`) eliminates malloc contention
  - Queue stores pointers into arena (not heap-allocated records)
  - Capacity: 8192 slots, swap at ~90% to prevent write failures
  - Bulk-free on flush (arena reset), zero fragmentation
  - Atomic operations for multi-producer, single-consumer flusher

- **Ring buffer:** Handles 512-byte sector alignment for durability
  - Power-of-2 sizing with mask-based wraparound
  - Serial access (flusher only), no concurrency overhead
  - Zero-padding to sector boundaries with overwrite strategy

- **Lock-free append:** Maximizes throughput for concurrent writers
  - Atomic LSN assignment + arena allocation + MPSC enqueue (no locks)
  - All-or-nothing enqueue prevents partial record corruption

- **Stateless recovery:** No separate state files needed
  - All configuration embedded in segment headers
  - Scan directory to rebuild WAL state on startup
  - Absolute segment numbering simplifies ordering

- **Soft-limit rotation:** Triggered after flush when segment exceeds size
  - Records never split across files (simplifies recovery)
  - Keeps files manageable for filesystem operations
  - Enables clean retirement after checkpoints

- **Zig-specific details:**
  - Little-endian encoding: `std.mem.writeInt(..., .little)` for portability
  - Error handling: Map Zig errors to C integer codes (0=success, -1/-2/-3=errors)
  - Opaque handles: `export const WAL = opaque {}` hides implementation from C

### Public Core Operations

```c
// Error codes returned by WAL functions (C ABI compatible)
#define WAL_OK              0   // Success
#define WAL_ERR_GENERIC    -1   // Generic error
#define WAL_ERR_LSN_GAP    -2   // LSN gap detected during iteration
#define WAL_ERR_CHECKSUM   -3   // Checksum failure (corrupted record)

// Scatter-gather vector for vectorized append (C ABI compatible)
struct WALIoVec {
    const void *base;       // Pointer to data buffer
    size_t len;             // Length of data in bytes
};

// Create new WAL directory with initial segment
// wal_dir: directory to store segment files (e.g., "/data/wal")
// buffer_size: deprecated (uses WAL_RECORDS = 8192 internally)
// segment_max_size: max segment size before rotation (e.g., 1GB)
// payload_hash_type: default payload hash (0=xxHash64, 1=BLAKE2s)
// Returns: WAL handle, or NULL on error
// Note: Queue capacity is WAL_RECORDS (8192), swap threshold is WAL_SWAP_THRES (~90%)
WAL* wal_create(const char *wal_dir, u32 buffer_size, u64 segment_max_size,
                u8 payload_hash_type);

// Open existing WAL directory (finds latest segment)
WAL* wal_open(const char *wal_dir);

// Append record to WAL (lock-free, non-blocking)
// For transactional appends, use wal_txn_append() instead
// Returns LSN assigned to this record
// NOTE: Record is in-memory only until wal_flush() is called
u64 wal_append(WAL *wal, u64 resource_id, u16 record_type,
               const void *payload, u16 payload_len);

// Vectorized append - accepts scatter-gather buffers (lock-free, non-blocking)
// Flattens iovecs into a single contiguous record in the arena
// The WAL does NOT retain pointers to iovec data after this call returns
// Use this to avoid memcpy when key/value are in separate buffers
// For transactional appends, use wal_txn_append_v() instead
// Returns LSN assigned to this record
u64 wal_append_v(WAL *wal, u64 resource_id, u16 record_type,
                 const struct WALIoVec *iovs, int iovcnt);

// Force WAL to disk (blocks until fsync completes)
// Triggers buffer swap and writes all records from inactive buffer to disk
i32 wal_flush(WAL *wal);

// Close WAL (flushes any buffered records, updates segment header)
void wal_close(WAL *wal);
```

### Public Recovery Operations

```c
// OPAQUE: Iterator handle for reading WAL records sequentially across all segments
// IMPORTANT: Iterator enforces LSN ordering even though records may be
//            physically out of order in segment files (due to lock-free append).
//            Implementation uses a min-heap to read records in strict LSN order.
struct WALIterator;

// Create iterator starting from specified LSN
// Reads all records from segments, builds min-heap ordered by LSN
// Memory usage: O(N) where N = total records in WAL
WALIterator* wal_iter_create(WAL *wal, u64 start_lsn);

// Get next record with LSN gap detection
// Returns:
//   0: Success - record retrieved
//  -1: No more records (end of WAL)
//  -2: LSN gap detected (current LSN != previous LSN + 1)
//  -3: Checksum failure (corrupted record)
// Extracts minimum LSN from heap, guarantees LSN ordering
// Caller receives pointer to record (valid until next call or destroy)
// On LSN gap (-2): Application can choose to call next() again to skip gap, or abort
i32 wal_iter_next(WALIterator *it, const struct WALRecordHeader **out_hdr,
                  const void **out_payload);

// Destroy iterator and free min-heap
void wal_iter_destroy(WALIterator *it);

// User-defined recovery callback
// hdr: record header
// payload: user payload (length = hdr->record_len - sizeof(WALRecordHeader))
// user_ctx: application context passed to wal_recover()
typedef i32 (*wal_apply_fn)(const struct WALRecordHeader *hdr,
                            const void *payload,
                            void *user_ctx);

// Recover by replaying WAL from start_lsn
// Scans all segments in order, calls apply_fn for each record
// User is responsible for implementing idempotency checking
i32 wal_recover(WAL *wal, u64 start_lsn, wal_apply_fn apply_fn, void *user_ctx);
```

### Public Checkpoint and Segment Management

```c
// Write generic checkpoint record
// checkpoint_data: opaque user data describing checkpoint state
// data_len: length of checkpoint data
// Returns LSN of checkpoint record
u64 wal_checkpoint(WAL *wal, const void *checkpoint_data, u32 data_len);

// Retire old segment files that are no longer needed
// safe_lsn: LSN before which all data is durable (from checkpoint)
// Deletes all segments where max_lsn < safe_lsn
i32 wal_retire_segments(WAL *wal, u64 safe_lsn);

// List all segment files in WAL directory (for recovery/inspection)
// Returns array of segment numbers, sorted ascending
i32 wal_list_segments(WAL *wal, u32 **out_segments, u32 *out_count);
```

---

## Logging Protocol

### Lock-Free Append Protocol with MPSC Queues

The WAL uses double-buffered MPSC queues to eliminate buffer space reservation races:

**Append Phases:**

1. **Assign LSN:** `lsn = atomic_fetch_add(&wal->next_lsn, 1, ACQ_REL)`
2. **Allocate record in arena:** `record = active_buffer.arena.allocator().alloc(sizeof(header) + payload_len)`
   - Uses current active buffer's ArenaAllocator
   - No malloc locks, no system allocator contention
   - All records for this flush batch allocated from same arena
3. **Build complete record:**
   - Fill header fields (LSN, resource_id, record_type, etc.)
   - Copy payload data (or flatten iovecs for `wal_append_v`)
   - Compute checksums (CRC-32C for header, xxHash64/BLAKE2s for payload)
4. **Enqueue to active MPSC queue:** `q_put(&active_buffer.queue, record)`
   - Queue stores pointers into the arena
5. **Check swap threshold:** If `queue->count >= swap_threshold` (80-90% of max_records), trigger flush

**Swap Threshold Design:**

- Queue capacity: `WAL_RECORDS` (8192 slots)
- Swap threshold: `~90%` (7372 slots)
- Headroom: ~10% slots reserved for in-flight writers
- **Prevents write failures:** Writers that started before swap can always complete

**Memory Management (Arena-Based):**

- Records allocated from ArenaAllocator during append (no malloc)
- Queue stores pointers into the arena
- Flusher copies records to ring buffer, then **bulk-frees** entire arena
- Zero fragmentation, no per-record free overhead
- Simple, correct ownership: WAL owns arena from swap until flush completes

**Key Properties:**

- **Lock-free append:** Only atomic LSN assignment + MPSC enqueue
- **No allocator contention:** Arena allocations avoid malloc locks
- **No buffer space races:** Enqueue is atomic (all-or-nothing)
- **No torn records:** Complete record enqueued before swap can occur
- **Variable-size records:** Arena allocation handles any size
- **Bulk deallocation:** Arena reset frees all records at once

### Flush Protocol

**High-Level Phases:**

1. Acquire flush mutex (serializes flushes, not appends)
2. Swap active buffer index atomically (allows concurrent appends to continue)
3. Dequeue records from inactive buffer's queue into ring buffer
4. Check for segment rotation (size threshold exceeded)
5. Flush ring buffer to file with 512-byte sector alignment
6. **Reset inactive arena** (bulk-free all records: `arena.deinit()` + re-init or custom reset)
7. Rotate segment if needed (soft limit check)
8. Update `flushed_lsn` with Release semantics
9. fsync to ensure durability
10. Release flush mutex

**Dirty Tail Strategy (Padding & Overwrite):**

The WAL uses sector-aligned writes with intelligent padding to avoid `O_DIRECT` complexity:

1. **512-byte alignment:** Ring buffer accumulates records and pads to sector boundaries
2. **Zero padding:** End of file contains zeros to next 512-byte boundary
3. **Overwrite on next flush:** Subsequent writes use `pwrite` to overwrite the padding
4. **Safety mechanism:** Checksums (CRC-32C header + payload hash) are the sole source of truth
   - If a crash occurs during overwrite, checksums fail
   - Recovery detects torn write at tail and discards partial record
   - Valid checksums = valid record (ignore trailing padding)

**Key Design Properties:**

- **Concurrent appends during flush:** Queue swap enables non-blocking append path
- **Sector-aligned writes:** Ring buffer accumulates and pads to 512-byte boundaries
- **Automatic rotation:** Triggered during flush when segment exceeds size limit
- **Memory ordering:** Release/Acquire semantics ensure visibility of flushed records
- **Torn write protection:** Double-checksum design (header + payload) detects corruption

---

## Recovery Protocol

**IMPORTANT: LSN Ordering During Recovery**

Records in segment files may be **physically out of LSN order** due to the lock-free append protocol where multiple threads assign LSNs independently and enqueue to MPSC queues. The iterator implementation **MUST enforce strict LSN ordering** when replaying records.

**Implementation:** Use a min-heap to read records in strict LSN order:

- Read all records from segments into heap (O(N log N) construction)
- Extract minimum LSN on each `wal_iter_next()` call (O(log N) per call)
- Memory usage: O(N) where N = total records in WAL
- Simpler than skip list, no indexing overhead, sufficient for recovery use case

**LSN Gap Detection:**

The iterator tracks the expected next LSN and detects gaps:

- On each `wal_iter_next()`: check if `current_lsn == expected_lsn`
- If `current_lsn > expected_lsn`: return `WAL_ERR_LSN_GAP (-2)`
- Application decides if gap is fatal (data loss) or expected (e.g., log truncation in distributed consensus)
- User can call `next()` again to skip the gap and continue, or abort recovery

### Generic Recovery Process

1. **Scan WAL directory for segments**
   - Use `wal_list_segments()` to find all segment files
   - Sort by segment number (ascending order)

2. **Handle torn writes and corrupted records**
   - If last record header is incomplete (< 48 bytes), discard it
   - If `header_checksum` fails, discard record (corrupted header)
   - If `payload_checksum` fails, discard record (corrupted/incomplete payload)
   - Recovery gracefully handles partial writes at any position in log
   - Double-checksum design (header + payload) ensures robust corruption detection

3. **Find recovery start point**
   - Application scans for last checkpoint record (user-defined type)
   - Or starts from LSN 0 for full recovery

4. **Replay from start LSN (in strict LSN order)**
   - Create iterator with `wal_iter_create(start_lsn)`
   - Iterator reads records from segments and sorts by LSN internally
   - For each record (in LSN order), call user's `apply_fn` callback
   - User implements idempotency checking based on application semantics

5. **Retire old segments** (optional)
   - After recovery, call `wal_retire_segments(safe_lsn)`
   - Deletes segments no longer needed for recovery

### User Responsibility: Idempotency

The WAL library provides LSN ordering but **users must implement idempotency**. Common patterns:

**Pattern 1: Resource LSN Tracking (Page-Based Systems)**

```c
// Each resource (page, object) tracks its last applied LSN
struct Page {
    u64 page_lsn;  // Last LSN applied to this page
    // ... page data
};

i32 apply_record(const WALRecordHeader *hdr, const void *payload, void *ctx) {
    MyDB *db = (MyDB*)ctx;

    // Get resource (page)
    Page *page = db_get_page(db, hdr->resource_id);

    // Idempotency check
    if (page->page_lsn >= hdr->lsn) {
        return 0;  // Already applied, skip
    }

    // Apply operation based on record_type
    switch (hdr->record_type) {
        case DB_INSERT:
            // ... apply insert using payload
            break;
        // ... other cases
    }

    // Update resource LSN
    page->page_lsn = hdr->lsn;
    return 0;
}
```

**Pattern 2: Operation Markers (Multi-Step Operations)**

```c
// Track in-progress multi-step operations
struct RecoveryContext {
    bool split_in_progress;
    u64 split_begin_lsn;
    // ...
};

i32 apply_record(const WALRecordHeader *hdr, const void *payload, void *ctx) {
    RecoveryContext *rctx = (RecoveryContext*)ctx;

    switch (hdr->record_type) {
        case DB_SPLIT_BEGIN:
            rctx->split_in_progress = true;
            rctx->split_begin_lsn = hdr->lsn;
            break;

        case DB_SPLIT_COMPLETE:
            rctx->split_in_progress = false;
            break;

        // ... apply individual operations
    }

    return 0;
}

// After recovery, check for incomplete operations
if (rctx.split_in_progress) {
    // Complete the split operation
}
```

**Pattern 3: Hash-Based Deduplication (Key-Value Stores)**

```c
// Track applied operations in hash set
struct KVRecoveryContext {
    HashSet *applied_lsns;  // Set of LSNs already applied
    // ...
};

i32 apply_record(const WALRecordHeader *hdr, const void *payload, void *ctx) {
    KVRecoveryContext *kv = (KVRecoveryContext*)ctx;

    // Check if already applied
    if (hashset_contains(kv->applied_lsns, hdr->lsn)) {
        return 0;  // Skip
    }

    // Apply operation
    switch (hdr->record_type) {
        case KV_PUT:
            // ...
            break;
    }

    // Mark as applied
    hashset_insert(kv->applied_lsns, hdr->lsn);
    return 0;
}
```

---

## Segment Lifecycle Management

### Rotation Strategy

Segments use a **soft limit** approach for rotation to ensure records are never split across files.

**Soft Limit Behavior:**

- `segment_max_size` (e.g., 1GB) is a target, not a hard boundary
- A single flush batch is allowed to push the file size **over** the limit
- Rotation occurs **after** the flush completes, not during
- **Guarantee:** Records are never split across segment files (simplifies recovery)
- **Constraint:** Individual records must not exceed `segment_max_size` (sanity check)

**Rotation Process:**

1. During flush: write all records from inactive buffer to current segment
2. After flush: if `segment_size > segment_max_size`, trigger rotation
3. Finalize current segment: update header with `max_lsn`, fsync file
4. Close current segment file descriptor
5. Create new segment with incremented number (`wal.XXXX`)
6. Write new segment header with `min_lsn = next_lsn`
7. fsync new segment and parent directory for metadata durability
8. Continue appending to new segment

**Directory fsync Requirement:** Both segment creation and header updates require directory fsync to ensure metadata persistence, preventing "missing file" errors after crashes.

**Shutdown Process:** Flush buffered records, update final `max_lsn` in segment header, fsync file and directory, close descriptor.

### Retention Policies

The WAL supports flexible segment retention policies to accommodate different use cases:

```c
// PUBLIC: Segment retention policy (C ABI compatible enum)
enum WALRetentionMode {
    WAL_RETAIN_NONE = 0,      // Delete segments after checkpoint (default)
    WAL_RETAIN_ALL = 1,       // Never delete segments (in-memory KV use case)
    WAL_RETAIN_LSN = 2,       // Keep segments covering last N LSNs
    WAL_RETAIN_COUNT = 3,     // Keep last N segments
};

// PUBLIC: Create WAL with retention policy
WAL* wal_create_with_policy(const char *wal_dir,
                             u32 buffer_size,
                             u64 segment_max_size,
                             enum WALRetentionMode mode,
                             u64 retention_param);  // LSN range or count

// PUBLIC: Simplified create (defaults to WAL_RETAIN_NONE)
WAL* wal_create(const char *wal_dir, u32 buffer_size, u64 segment_max_size);

// PUBLIC: Update retention policy at runtime
i32 wal_set_retention(WAL *wal, enum WALRetentionMode mode, u64 param);

// PUBLIC: Retire segments respecting retention policy
i32 wal_retire_segments(WAL *wal, u64 safe_lsn);

// PUBLIC: Manual cleanup (ignores retention policy)
i32 wal_retire_segments_force(WAL *wal, u64 safe_lsn);
```

**Retention Mode Details:**

1. **WAL_RETAIN_NONE (Default):** Delete segments immediately after checkpoint
   - Use case: Traditional databases with page-based storage
   - Segments deleted when `max_lsn < checkpoint_lsn`

2. **WAL_RETAIN_ALL:** Never delete segments automatically
   - Use case: In-memory KV stores where WAL is the only persistent state
   - User must manually delete segments or implement compaction

3. **WAL_RETAIN_LSN:** Keep segments covering last N LSNs
   - Use case: Point-in-time recovery to recent states
   - Deletes segments with `max_lsn < (current_lsn - retention_lsn_range)`
   - Example: Keep last 1M LSNs for recovery to any recent operation

4. **WAL_RETAIN_COUNT:** Keep last N segments
   - Use case: Bounded storage with predictable disk usage
   - Keeps N most recent segments regardless of LSN range
   - Example: Keep last 10 segments (~10GB if 1GB each)

**Usage Examples:**

```c
// In-memory KV store - keep all segments
WAL *wal = wal_create_with_policy("/data/wal",
                                   1024 * 1024,        // 1MB buffer
                                   1024 * 1024 * 1024, // 1GB segments
                                   WAL_RETAIN_ALL,
                                   0);

// Traditional database - delete after checkpoint
WAL *wal = wal_create("/db/wal", 1024 * 1024, 1024 * 1024 * 1024);

// Point-in-time recovery - keep last 1M LSNs
WAL *wal = wal_create_with_policy("/db/wal",
                                   1024 * 1024,
                                   1024 * 1024 * 1024,
                                   WAL_RETAIN_LSN,
                                   1000000);

// Bounded storage - keep last 10 segments
WAL *wal = wal_create_with_policy("/db/wal",
                                   1024 * 1024,
                                   1024 * 1024 * 1024,
                                   WAL_RETAIN_COUNT,
                                   10);
```

---

## Performance Considerations

### Write Amplification

**Buffering:** Keep log records in memory buffer (e.g., 1MB)

- Amortize fsync cost across multiple records
- Flush on:
  - Buffer full
  - Explicit flush request
  - Transaction commit

**Group Commit:** Multiple transactions can share same fsync

- Wait briefly for more transactions
- Single fsync commits all of them

### Segment Size Management

**Target Segment Size:** 1GB per segment (configurable)

- Automatic rotation when segment reaches limit
- Keeps individual files manageable
- No need for manual truncation

**Checkpoint Frequency:** Application-defined

- Typical: Every 100MB of log data
- Enables retirement of old segments
- Bounds recovery time (only replay since last checkpoint)

---

## Integration Patterns

### Pattern 1: Page-Based Storage Engine

```c
// Logging wrapper
u64 log_page_insert(WAL *wal, u32 page_id, const void *key, const void *value) {
    struct {
        u16 key_len;
        u16 value_len;
        u8 data[];
    } *payload = ...;

    u64 lsn = wal_append(wal, page_id, DB_INSERT, payload, payload_len);
    return lsn;
}

// Write-back protocol
void flush_page(WAL *wal, Page *page) {
    // WAL protocol: log must be on disk before page
    wal_flush(wal);

    // Now safe to write page to disk
    pwrite(db_fd, page->data, PAGE_SIZE, page->page_id * PAGE_SIZE);
}
```

### Pattern 2: Key-Value Store

```c
// Logging
void kv_put(WAL *wal, KVStore *store, const char *key, const char *value) {
    // Build payload
    struct KVPutPayload *payload = ...;

    // Log the operation
    u64 hash = hash_key(key);
    u64 lsn = wal_append(wal, hash, KV_PUT, payload, payload_len);

    // Apply to in-memory store
    hashtable_insert(store->table, key, value);

    // Flush log (for durability)
    wal_flush(wal);
}

// Recovery
i32 kv_apply(const WALRecordHeader *hdr, const void *payload, void *ctx) {
    KVStore *store = (KVStore*)ctx;
    struct KVPutPayload *put = (struct KVPutPayload*)payload;

    switch (hdr->record_type) {
        case KV_PUT:
            hashtable_insert(store->table, put->data, put->data + put->key_len);
            break;
        case KV_DELETE:
            hashtable_remove(store->table, put->data);
            break;
    }
    return 0;
}
```

### Pattern 3: Object Store

```c
// Logging
u64 log_object_update(WAL *wal, u64 object_id, u64 offset, const void *data, u32 len) {
    struct {
        u64 offset;
        u32 length;
        u8 data[];
    } *payload = ...;

    return wal_append(wal, object_id, OBJ_UPDATE, payload, payload_len);
}

// Flush protocol
void flush_object(WAL *wal, Object *obj) {
    wal_flush(wal);
    fwrite(obj->data, obj->size, 1, obj->file);
    fsync(fileno(obj->file));
}
```

---

## Transaction Support

### Overview

The WAL provides full ACID transaction support with undo/redo recovery. Transactions link records via `prev_lsn` chains and support both commit and abort with automatic rollback during recovery.

### Reserved Record Types

```c
// PUBLIC: WAL library reserves top 4 type codes for transaction control (C ABI compatible)
#define WAL_TXN_BEGIN   0xFFFC  // Transaction begin marker
#define WAL_TXN_COMMIT  0xFFFD  // Transaction commit
#define WAL_TXN_ABORT   0xFFFE  // Transaction abort
#define WAL_TXN_UNDO    0xFFFF  // Undo record (inverse operation)

// User record types: 0x0000 - 0xFFFB (65,532 types available)
// Non-transactional records use txn_id = 0
```

### Transaction Record Formats (C ABI Compatible)

**Begin Record:**

```c
// PUBLIC: Transaction begin payload (C ABI compatible)
struct WALTxnBeginPayload {
    u64 timestamp;       // When transaction started (optional)
    u32 isolation_level; // User-defined isolation level (optional)
};
```

**Commit/Abort Records:**

```c
// No payload needed - record type indicates outcome
```

**Undo Record:**

```c
// PUBLIC: Undo record payload (C ABI compatible)
struct WALUndoRecordPayload {
    u64 original_lsn;    // LSN of the operation being undone
    u16 original_type;   // Original record type
    u8 undo_data[];      // User-defined undo data
};
```

### Public Transaction API

```c
// OPAQUE: Transaction handle (users only interact via pointer)
struct WALTxn {
    u32 txn_id;          // Unique transaction ID (never 0)
    u64 first_lsn;       // LSN of BEGIN record
    u64 last_lsn;        // Last LSN in this transaction
    WAL *wal;            // WAL instance
    bool active;         // Is transaction still active
};

// Begin transaction
// Writes BEGIN record, assigns unique txn_id (starting from 1)
WALTxn* wal_txn_begin(WAL *wal);

// Append record within transaction
// Automatically links via prev_lsn chain
// Returns LSN of the appended record
u64 wal_txn_append(WALTxn *txn, u64 resource_id, u16 record_type,
                   const void *payload, u16 payload_len);

// Vectorized append within transaction
// Flattens iovecs into a single contiguous record in the arena
// Returns LSN of the appended record
u64 wal_txn_append_v(WALTxn *txn, u64 resource_id, u16 record_type,
                     const struct WALIoVec *iovs, int iovcnt);

// Append with undo information
// User provides undo data that can reverse this operation
// Writes both the redo record and corresponding undo record
u64 wal_txn_append_with_undo(WALTxn *txn, u64 resource_id, u16 record_type,
                              const void *payload, u16 payload_len,
                              const void *undo_data, u16 undo_len);

// Commit transaction
// Writes COMMIT record, flushes log to ensure durability
i32 wal_txn_commit(WALTxn *txn);

// Abort transaction
// Writes ABORT record, flushes log
// Undo happens during recovery (not immediately)
i32 wal_txn_abort(WALTxn *txn);

// Free transaction handle (after commit/abort)
void wal_txn_free(WALTxn *txn);
```

### Implementation Notes

- **Transaction IDs:** Start from 1, assigned atomically (0 reserved for non-transactional)
- **Record Linkage:** Each record's `prev_lsn` points to previous record in transaction
- **Undo Records:** Contain original LSN, operation type, and undo data
- **Commit/Abort:** Both flush log to ensure durability before returning

### Recovery with Transactions

**Optimized 2-Pass Recovery Protocol:**

**Pass 1: Scan Transaction States (Memory-Efficient)**

- Scan all WAL records sequentially
- Track only transaction outcomes: `HashMap<txn_id, TxnState>`
  - TxnState: `{status: COMMIT|ABORT|INCOMPLETE, last_lsn: u64}`
- Store only transaction IDs and final states (not full record lists)
- Memory usage: O(concurrent transactions), not O(total records)
- Mark incomplete transactions (BEGIN without COMMIT/ABORT) as ABORTED

**Pass 2: Replay with On-Disk Links**

- Re-scan all WAL records sequentially
- For each record:
  - If `txn_id == 0`: Apply immediately (non-transactional)
  - If `txn_id != 0`: Check transaction state from Pass 1
    - **COMMITTED:** Apply redo records forward (skip undo records)
    - **ABORTED:** Skip redo records, collect undo records
- For aborted transactions:
  - Use on-disk `prev_lsn` chains to walk backward
  - Apply undo records in reverse order
  - No need to store full chain in memory

**Memory Advantages:**

- Pass 1: Only stores transaction state map (< 1MB for 100K concurrent txns)
- Pass 2: Processes records on-the-fly, no buffering
- Avoids OOM on large logs (billions of records)
- Suitable for constrained environments

**Public Recovery API:**

```c
// PUBLIC: User-defined redo callback (applies forward operations)
typedef i32 (*wal_redo_fn)(const struct WALRecordHeader *hdr,
                           const void *payload,
                           void *user_ctx);

// PUBLIC: User-defined undo callback (reverses operations)
typedef i32 (*wal_undo_fn)(const struct WALRecordHeader *hdr,
                           const void *payload,
                           void *user_ctx);

// PUBLIC: Recover with transaction support
i32 wal_recover_txn(WAL *wal, u64 start_lsn,
                    wal_redo_fn redo_fn,
                    wal_undo_fn undo_fn,
                    void *user_ctx);
```

### Usage Example

**Transaction Operations:**

```c
// Begin transaction
WALTxn *txn = wal_txn_begin(wal);

// Insert with undo (so we can rollback)
struct InsertPayload {
    u16 key_len;
    u16 value_len;
    u8 data[];  // [key][value]
} *insert = ...;

struct DeletePayload {
    u16 key_len;
    u8 key[];  // Just the key (for undo)
} *undo = ...;

u64 lsn = wal_txn_append_with_undo(txn, page_id, DB_INSERT,
                                    insert, insert_len,
                                    undo, undo_len);

// Commit or abort
if (success) {
    wal_txn_commit(txn);
} else {
    wal_txn_abort(txn);
}

wal_txn_free(txn);
```

**Recovery Callbacks:**

```c
// Redo callback (apply operations forward)
i32 apply_redo(const WALRecordHeader *hdr, const void *payload, void *ctx) {
    MyDB *db = (MyDB*)ctx;

    switch (hdr->record_type) {
        case DB_INSERT: {
            struct InsertPayload *p = (struct InsertPayload*)payload;
            db_insert(db, hdr->resource_id, p->data, p->data + p->key_len);
            break;
        }
        case DB_DELETE: {
            struct DeletePayload *p = (struct DeletePayload*)payload;
            db_delete(db, hdr->resource_id, p->key);
            break;
        }
    }
    return 0;
}

// Undo callback (reverse operations)
i32 apply_undo(const WALRecordHeader *hdr, const void *payload, void *ctx) {
    MyDB *db = (MyDB*)ctx;

    if (hdr->record_type == WAL_TXN_UNDO) {
        struct WALUndoRecordPayload *undo = (struct WALUndoRecordPayload*)payload;

        switch (undo->original_type) {
            case DB_INSERT:
                // Undo insert = delete
                db_delete(db, hdr->resource_id, undo->undo_data);
                break;

            case DB_DELETE:
                // Undo delete = insert with old value
                struct InsertPayload *old = (struct InsertPayload*)undo->undo_data;
                db_insert(db, hdr->resource_id,
                         old->data, old->data + old->key_len);
                break;
        }
    }
    return 0;
}

// Recovery
wal_recover_txn(wal, 0, apply_redo, apply_undo, &db);
```

**Transaction Properties:**

- **Atomicity:** Transactions are all-or-nothing (commit or abort)
- **Consistency:** Undo records restore previous state
- **Isolation:** User implements (WAL provides serialization point at commit)
- **Durability:** Commit flushes log before returning (see below for durability levels)

**Commit Durability Semantics:**

By default, `wal_txn_commit()` ensures full durability:

1. Commit record written to buffer
2. Buffer flushed to disk with `pwrite`
3. Segment file fsync'd to storage
4. `flushed_lsn` updated with Release semantics
5. Returns success to caller

**Optional Durability Modes:**

```c
// PUBLIC: Synchronous commit (default) - full durability
i32 wal_txn_commit(WALTxn *txn);

// PUBLIC: Async commit - returns before fsync (unsafe, for testing/benchmarking)
i32 wal_txn_commit_async(WALTxn *txn);
```

Async commits skip fsync, returning immediately after buffer write. This is **unsafe** for production (may lose committed transactions on crash) but useful for:

- Benchmarking maximum throughput
- Testing scenarios where durability is handled externally
- Replication scenarios where durability is at replica level

**Recovery Guarantees:**

1. **Committed transactions:** All operations replayed in order
2. **Aborted transactions:** All operations undone in reverse order
3. **Incomplete transactions:** Treated as aborted, operations undone
4. **Non-transactional records (txn_id=0):** Always applied (skip classification)

---

## Optional Helper Utilities (Public API)

While idempotency is user-implemented, the WAL library can provide optional helpers to reduce boilerplate:

### Resource LSN Tracking

```c
// PUBLIC: Helper to track last applied LSN per resource
// OPAQUE: Users only interact via pointer
struct ResourceLSNMap;

ResourceLSNMap* rmap_create(void);
void rmap_destroy(ResourceLSNMap *map);

// Check if record should be applied (returns true if record.lsn > resource_lsn)
bool rmap_should_apply(ResourceLSNMap *map, u64 resource_id, u64 record_lsn);

// Update resource LSN after successful application
void rmap_update(ResourceLSNMap *map, u64 resource_id, u64 lsn);

// Usage example
i32 apply_with_idempotency(const WALRecordHeader *hdr, const void *payload, void *ctx) {
    MyDB *db = (MyDB*)ctx;

    if (!rmap_should_apply(db->lsn_map, hdr->resource_id, hdr->lsn)) {
        return 0;  // Already applied, skip
    }

    // Apply operation
    switch (hdr->record_type) {
        case DB_INSERT:
            db_insert(db, hdr->resource_id, ...);
            break;
    }

    // Mark as applied
    rmap_update(db->lsn_map, hdr->resource_id, hdr->lsn);
    return 0;
}
```

### Transaction LSN Set

```c
// PUBLIC: Helper to track applied transaction LSNs (Bloom filter or hash set)
// OPAQUE: Users only interact via pointer
struct TxnLSNSet;

TxnLSNSet* txn_set_create(u64 estimated_size);
void txn_set_destroy(TxnLSNSet *set);

// Check if LSN already processed
bool txn_set_contains(TxnLSNSet *set, u64 lsn);

// Mark LSN as processed
void txn_set_insert(TxnLSNSet *set, u64 lsn);
```

**Note:** These helpers are optional. Many applications already have their own LSN tracking mechanisms (e.g., page headers with `page_lsn` fields). The helpers are provided for convenience, not required.

---

## Future Extensions

### Parallel Recovery

Partition log by resource ranges for parallel replay:

- Thread 1: Resources 0-1000
- Thread 2: Resources 1001-2000
- No conflicts within partition

### Log Shipping

Stream WAL segments to replica:

- Replica applies log records
- Achieves replication with minimal lag

---

## Summary

**Key Design Choices:**

- **Generic resource identification**: 64-bit `resource_id` supports any addressing scheme
- **User-defined record types**: No hardcoded assumptions about operations
- **LSN-based ordering**: Globally unique, monotonic LSNs ensure consistency
- **Idempotent recovery**: User implements checking based on application semantics
- **Independent system**: Self-contained library with no external dependencies
- **Lock-free append**: Double-buffered design with atomic cursor for high concurrency
- **Segment-based storage**: Bounded file sizes, simple retirement after checkpoint
- **Flexible integration**: Callback-based recovery supports various storage models
- **Full ACID transactions**: Transaction support with undo/redo recovery and prev_lsn chains

**Architecture:**

```
┌──────────────────────────────────────────┐
│  Application Layer                       │
│                                          │
│  - Defines record types                  │
│  - Implements idempotency checking       │
│  - Manages checkpoints                   │
│  - Calls wal_append() before mutations   │
│  - Calls wal_flush() before write-back   │
└──────────────────────────────────────────┘
                  │
                  ↓ (uses)
┌──────────────────────────────────────────┐
│  Generic WAL Library                     │
│                                          │
│  - Lock-free wal_append()                │
│  - Double-buffered flush                 │
│  - Segment rotation & retirement         │
│  - Recovery via callback iteration       │
└──────────────────────────────────────────┘
```

# Write-Ahead Log (WAL) Design

**Implementation Note:** This library is implemented in Zig and provides a C-compatible API for cross-language use. The design document describes the C API interface; the Zig implementation may use idiomatic Zig constructs internally while maintaining full C ABI compatibility.

## Overview

This document defines a generic Write-Ahead Log (WAL) system. The WAL provides crash recovery and durability guarantees for storage systems that modify persistent resources (pages, objects, records, etc.). The design is intentionally generic and can be adapted to various use cases including databases, key-value stores, object storage, and file systems.

## Design Principles

1. **Generic & Extensible:** No assumptions about resource types (pages, objects, etc.)
2. **User-Defined Record Types:** Applications define their own record type taxonomy
3. **LSN-Based Ordering:** Globally unique Log Sequence Numbers ensure correct ordering
4. **Idempotent Recovery:** Redo operations can be safely applied multiple times
5. **Independent System:** Self-contained with no external dependencies
6. **Lock-Free Append:** Double-buffered design with active writer tracking for safe concurrent appends
7. **Single-File Storage:** Append-only file with automatic compaction after checkpoint
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

### File Naming

WAL uses a single file named `wal.log` in the specified directory.

**Benefits:**

- Simpler implementation (no segment rotation complexity)
- Automatic compaction after checkpoint keeps file size bounded
- Single file descriptor reduces resource usage

### File Header

The WAL file begins with a 4KB header:

```c
struct WALFileHeader {
    u32 magic;                   // 0x57414C46 ("WALF")
    u32 version;                 // WAL format version (1)
    u8 payload_hash_type;        // Default payload hash (0=xxHash64, 1=BLAKE2s)
    u8 record_alignment;         // Record alignment (8 bytes)
    u8 _reserved[6];             // Reserved for future use

    u64 min_lsn;                 // First LSN in file (updated after compaction)
    u64 max_lsn;                 // Last LSN written (updated on each flush)
    u64 file_size;               // Current file size in bytes (updated on each flush)

    u64 safe_lsn;                // Last checkpoint LSN (compaction boundary)
    u64 max_file_size;           // File size hint for compaction trigger
    u32 buffer_size;             // Deprecated (use WAL_RECORDS = 8192)

    u32 checksum;                // CRC-32C of header (excluding this field)
    u8 _pad[4040];               // Pad to 4096 bytes
};
_Static_assert(sizeof(struct WALFileHeader) == 4096, "Header must be 4096 bytes");
```

**Key Fields:**

- **min_lsn:** First LSN in file (updated after compaction to safe_lsn + 1)
- **max_lsn:** Last LSN written (updated on every flush)
- **file_size:** Current file size including header (updated on every flush)
- **safe_lsn:** Last checkpoint LSN, records ≤ safe_lsn are safe to discard
- **max_file_size:** Size hint for when to trigger compaction (advisory, not enforced)
- **payload_hash_type:** Default hash algorithm for records in this file
  - Records can override this per-record using their own `payload_hash_type` field

**Recovery without state file:**

- Open `wal.log` and read header
- Extract configuration from header (min_lsn, max_lsn, payload_hash_type)
- Compute `next_lsn` = max_lsn + 1

### File Layout

```
┌─────────────────────────────────────┐
│ WAL File Header (4KB)               │  ← Offset 0
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

### Data Structures

```c
// Queue data element (heap-allocated record with metadata)
struct CQData {
    u64 lsn;           // LSN of this record
    u64 size;          // Record size in bytes
    u8 data[];         // Variable-length record data
};

// Copyable Queue (MPSC queue holding heap-allocated records)
struct CQueue {
    atomic_u64 head;              // Next slot to allocate (multi-producer)
    atomic_u64 size;              // Current queue size
    u64 tail;                     // Next slot to consume (single-consumer)
    u64 cap;                      // Queue capacity
    struct CQData *_Atomic slots[]; // Array of record pointers
};

// Ring buffer for aligned writes (flusher-only, serial access)
struct RingBuf {
    u64 head;          // Write position
    u64 tail;          // Read position
    u64 size;          // Current bytes in buffer
    u64 mask;          // Capacity - 1 (for power-of-2 wraparound)
    u8 data[];         // Variable-length buffer data
};

struct WAL {
    // File management
    i32 fd;                             // File descriptor for wal.log
    i32 dirfd;                          // Directory FD for openat()
    u64 max_file_size;                  // File size hint for compaction trigger
    u8 payload_hash_type;               // Default payload hash (0=xxHash64, 1=BLAKE2s)
    char *wal_dir;                      // Directory containing wal.log
    struct WALFileHeader *header;       // Cached file header (heap-allocated)

    // Double-buffered MPSC queues (hold heap-allocated CQData records)
    struct CQueue *buffers[2];          // Two queues for ping-pong (capacity: WAL_RECORDS)
    atomic_u32 active_idx;              // Which queue is active (0 or 1)
    atomic_u32 active_writers[2];       // Count of threads writing to each queue
    u32 buffer_size;                    // Deprecated (use WAL_RECORDS instead)

    // Ring buffer for aligned writes (serial, flusher-only)
    struct RingBuf *rb;                 // Accumulates bytes, handles 512-byte alignment

    // LSN management
    atomic_u64 next_lsn;                // Next LSN to assign (monotonic)
    atomic_u64 flushed_lsn;             // Last LSN guaranteed on disk
    u64 safe_lsn;                       // Last checkpoint LSN (for compaction)

    // Flush coordination
    atomic_u32 flush_in_progress;       // Prevents concurrent flushes
    pthread_mutex_t flush_mutex;        // Serializes flush operations
    pthread_cond_t flush_cond;          // Notifies threads waiting for flush

    // Compaction state
    bool compaction_needed;             // Compaction pending after checkpoint
    u32 compaction_retry_count;         // Failed compaction attempts
};
```

**Design Notes:**

- **CQueue (Copyable Queue):** Double-buffered MPSC queues hold heap-allocated records
  - `CQData` struct stores LSN + size + record data (malloc'd on append)
  - Append thread: build record → assign LSN → `cq_put()` (copies to heap)
  - Flusher: `cq_pop()` → copy to ring buffer → free `CQData`
  - Capacity: `WAL_RECORDS` (8192 slots), swap at `WAL_SWAP_THRES` (~90% = 7372)
  - Atomic head/size for multi-producer, non-atomic tail for single-consumer

- **Ring Buffer:** True circular buffer for 512-byte aligned writes
  - Power-of-2 sized with mask-based wraparound (no modulo)
  - Only flusher accesses (no concurrency, serial)
  - Accumulates dequeued records, handles wraparound automatically
  - Writes aligned portions (rounds up to 512-byte boundary with zero padding)
  - No compaction needed (true circular buffer)

- **Lock-free append:** Multiple threads can append concurrently
  - Atomic `next_lsn` for LSN assignment (`FADD`)
  - `cq_put()` atomically reserves slot and stores `CQData` pointer
  - No buffer space reservation race conditions (all-or-nothing enqueue)

- **File header metadata:** No state file required
  - All WAL configuration stored in file header
  - Recovery reads file header to extract config
  - `min_lsn`, `max_lsn`, `safe_lsn`, `max_file_size`, `buffer_size`
  - Header updated on every flush

- **Compaction:** Automatic after checkpoint
  - Create new file with records where lsn > safe_lsn
  - Atomic rename to replace old file
  - Blocks flushes during compaction (appends continue)

### Core Operations

```c
// Create new WAL directory with wal.log file
// wal_dir: directory to store wal.log (e.g., "/data/wal")
// buffer_size: deprecated (uses WAL_RECORDS = 8192 internally)
// max_file_size: size hint for compaction trigger (e.g., 1GB)
// payload_hash_type: default payload hash (0=xxHash64, 1=BLAKE2s)
// Returns: WAL handle, or NULL on error
// Note: Queue capacity is WAL_RECORDS (8192), swap threshold is WAL_SWAP_THRES (~90%)
WAL* wal_create(const char *wal_dir, u32 buffer_size, u64 max_file_size,
                u8 payload_hash_type);

// Open existing WAL directory (opens wal.log)
WAL* wal_open(const char *wal_dir);

// Append non-transactional record to WAL (lock-free, non-blocking)
// Returns LSN assigned to this record
// NOTE: Record is in-memory only until wal_flush() is called
// NOTE: For transactional records, use wal_txn_append() instead
u64 wal_append(WAL *wal, u64 resource_id, u16 record_type,
               const void *payload, u16 payload_len);

// Force all buffered records to disk (blocks until fsync completes)
// Triggers buffer swap and writes inactive buffer to disk
// Updates file header with new max_lsn and file_size
i32 wal_flush(WAL *wal);

// Close WAL (flushes any buffered records, updates file header)
void wal_close(WAL *wal);
```

### Recovery Operations

```c
// Iterator for reading WAL records sequentially from file
// IMPORTANT: Iterator MUST enforce LSN ordering even though records may be
//            physically out of order in file (due to lock-free append).
//            Implementation uses a min-heap to read records in strict LSN order.
struct WALIterator;

// Create iterator starting from specified LSN
// Reads all records from file, builds min-heap ordered by LSN
// Memory usage: O(N) where N = total records in WAL
WALIterator* wal_iter_create(WAL *wal, u64 start_lsn);

// Get next record (returns 0 on success, -1 when no more records)
// Extracts minimum LSN from heap, guarantees LSN ordering
// Caller receives pointer to record (valid until next call or destroy)
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
// Reads file, calls apply_fn for each record in LSN order
// User is responsible for implementing idempotency checking
i32 wal_recover(WAL *wal, u64 start_lsn, wal_apply_fn apply_fn, void *user_ctx);
```

### Checkpoint and Compaction

```c
// Write generic checkpoint record and update safe_lsn
// checkpoint_data: opaque user data describing checkpoint state
// data_len: length of checkpoint data
// Returns LSN of checkpoint record
// NOTE: Sets safe_lsn and marks compaction_needed = true
u64 wal_checkpoint(WAL *wal, const void *checkpoint_data, u32 data_len);
```

---

## Logging Protocol

### Lock-Free Append Protocol with MPSC Queues

The WAL uses double-buffered MPSC queues to eliminate buffer space reservation races:

**Append Phases:**

1. **Assign LSN:** `lsn = atomic_fetch_add(&wal->next_lsn, 1, ACQ_REL)`
2. **Allocate record on heap:** `record = malloc(sizeof(header) + payload_len)`
3. **Build complete record:**
   - Fill header fields (LSN, resource_id, record_type, etc.)
   - Copy payload data
   - Compute checksums (CRC-32C for header, xxHash64/BLAKE2s for payload)
4. **Enqueue to active MPSC queue:** `q_put(&wal->queues[active_idx], record, &queue_idx)`
5. **Check swap threshold:** If `queue->count >= swap_threshold` (80-90% of max_records), trigger flush

**Swap Threshold Design:**

- Queue capacity: `max_records` (e.g., 64K)
- Swap threshold: `0.8 * max_records` to `0.9 * max_records` (51.2K - 57.6K)
- Headroom: 6.4K - 12.8K slots reserved for in-flight writers
- **Prevents write failures:** Writers that started before swap can always complete

**Memory Management:**

- Records heap-allocated during append (`malloc`)
- Queue stores `void*` pointers to records
- Flusher frees records after copying to ring buffer
- Simple, correct ownership: WAL owns record from enqueue until flush completes

**Key Properties:**

- **Lock-free append:** Only atomic LSN assignment + MPSC enqueue
- **No buffer space races:** Enqueue is atomic (all-or-nothing)
- **No torn records:** Complete record enqueued before swap can occur
- **Variable-size records:** Heap allocation handles any size

### Flush Protocol with CQueue and RingBuf

**Flush Phases:**

1. Acquire flush mutex (serialize flushes, not appends)
2. Set `flush_in_progress` flag with Release semantics
3. Swap active queue atomically
4. Dequeue records from inactive queue and accumulate in ring buffer
   - Track maximum LSN seen
   - Free records after copying to ring buffer
5. Flush ring buffer to file with 512-byte alignment
   - Use 16KB staging buffer
   - Zero-pad to sector boundary
   - Count data only, not padding
6. Update file header with new `max_lsn`, `file_size`, and checksum
7. Write header to offset 0
8. Update `flushed_lsn` atomically with Release semantics
9. Fsync to ensure durability
10. Clear `flush_in_progress` with Release semantics
11. Attempt compaction if needed
12. Release flush mutex

**Ring Buffer Benefits:**

- **Serial access:** Only flusher touches ring buffer (no concurrency issues)
- **True circular buffer:** Power-of-2 sized with mask-based wraparound (no modulo)
- **Alignment handling:** Rounds up to 512-byte boundary with zero padding
- **Efficient writes:** Batches small records into 16KB chunks before `pwrite_all`
- **No compaction needed:** Wraparound handled by mask operation

**Memory Ordering:**

- `flushed_lsn` writes use **Release** semantics (ensure all writes visible before update)
- `flushed_lsn` reads use **Acquire** semantics (see all writes up to flushed LSN)

**Key Properties:**

- **Appends never block on flush:** Queue swap allows concurrent appends during flush
- **Single flusher:** `flush_mutex` ensures only one flush at a time
- **No buffer space races:** Swap threshold ensures in-flight writers can complete
- **512-byte alignment:** Ring buffer handles alignment without per-flush padding overhead
- **Torn write protection:** Sector-aligned writes ensure partial flushes are detectable during recovery

---

## Recovery Protocol

**IMPORTANT: LSN Ordering During Recovery**

Records in the WAL file may be **physically out of LSN order** due to the lock-free append protocol where multiple threads assign LSNs independently and enqueue to MPSC queues. The iterator implementation **MUST enforce strict LSN ordering** when replaying records.

**Implementation:** Use a min-heap to read records in strict LSN order:

- Read all records from file into heap (O(N log N) construction)
- Extract minimum LSN on each `wal_iter_next()` call (O(log N) per call)
- Memory usage: O(N) where N = total records in WAL
- Simpler than skip list, no indexing overhead, sufficient for recovery use case

### Generic Recovery Process

1. **Open WAL file**
   - Open `wal.log` and read file header
   - Extract `min_lsn`, `max_lsn`, `safe_lsn` from header

2. **Handle torn writes and corrupted records**
   - If last record header is incomplete (< 56 bytes), discard it
   - If `header_checksum` fails, discard record (corrupted header)
   - If `payload_checksum` fails, discard record (corrupted/incomplete payload)
   - Recovery gracefully handles partial writes at any position in log
   - Double-checksum design (header + payload) ensures robust corruption detection

3. **Find recovery start point**
   - Application scans for last checkpoint record (user-defined type)
   - Or starts from `min_lsn` for full recovery

4. **Replay from start LSN (in strict LSN order)**
   - Create iterator with `wal_iter_create(start_lsn)`
   - Iterator reads records from file and sorts by LSN internally
   - For each record (in LSN order), call user's `apply_fn` callback
   - User implements idempotency checking based on application semantics

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

## Compaction Protocol

### Trigger

Compaction is automatically triggered after checkpoint. The `wal_checkpoint()` function:

- Writes checkpoint record with user-provided data
- Sets `safe_lsn` to checkpoint LSN
- Marks `compaction_needed = true`
- Resets `compaction_retry_count = 0`

### Compaction Process

Compaction runs during the next flush or close operation:

1. Acquire flush mutex (blocks other flushes, appends continue)
2. Create temporary file `wal.log.compact`
3. Write new header with `min_lsn = safe_lsn + 1`
4. Copy all records where `lsn > safe_lsn` from old file
5. Update new header with final `max_lsn` and `file_size`, compute checksum
6. Fsync temporary file
7. Atomic rename: `wal.log.compact` → `wal.log`
8. Fsync directory for metadata durability
9. Reopen file and reload header
10. Release flush mutex

**Key Properties:**

- **Appends continue:** Only flushes block during compaction (double-buffering handles this)
- **Atomic replacement:** Old file valid until atomic rename
- **Failure recovery:** If compaction fails, old file remains intact
- **Retry logic:** Failed compactions retry on next flush/close (max retries)

### Compaction Retry Strategy

- Triggered in `wal_flush()` or `wal_close()` when `compaction_needed` is true
- Max retries: 3-5 attempts
- Retry on: Next flush, close, or explicit checkpoint
- Failure handling: Log error, continue operation (old file still valid)

---

## Performance Considerations

### Write Amplification

**Buffering:** Keep log records in memory buffer (double-buffered queues)

- Amortize fsync cost across multiple records
- Flush on:
  - Buffer swap threshold (~90% full)
  - Explicit flush request
  - Transaction commit

**Group Commit:** Multiple transactions can share same fsync

- Wait briefly for more transactions
- Single fsync commits all of them

### File Size Management

**Target File Size:** 1GB (configurable via max_file_size hint)

- Automatic compaction after checkpoint
- Keeps file size bounded
- No manual truncation needed

**Checkpoint Frequency:** Application-defined

- Typical: Every 100MB of log data or every N operations
- Triggers compaction to reclaim space
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
// WAL library reserves top 4 type codes for transaction control
#define WAL_TXN_BEGIN   0xFFFC  // Transaction begin marker
#define WAL_TXN_COMMIT  0xFFFD  // Transaction commit
#define WAL_TXN_ABORT   0xFFFE  // Transaction abort
#define WAL_TXN_UNDO    0xFFFF  // Undo record (inverse operation)

// User record types: 0x0000 - 0xFFFB (65,532 types available)
// Non-transactional records use txn_id = 0
```

### Transaction Record Formats

**Begin Record:**

```c
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
struct WALUndoRecordPayload {
    u64 original_lsn;    // LSN of the operation being undone
    u16 original_type;   // Original record type
    u8 undo_data[];      // User-defined undo data
};
```

### Transaction API

```c
// Transaction handle
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

**Recovery API:**

```c
// User-defined redo callback (applies forward operations)
typedef i32 (*wal_redo_fn)(const struct WALRecordHeader *hdr,
                           const void *payload,
                           void *user_ctx);

// User-defined undo callback (reverses operations)
typedef i32 (*wal_undo_fn)(const struct WALRecordHeader *hdr,
                           const void *payload,
                           void *user_ctx);

// Recover with transaction support
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
// Synchronous commit (default) - full durability
i32 wal_txn_commit(WALTxn *txn);

// Async commit - returns before fsync (unsafe, for testing/benchmarking)
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

## Optional Helper Utilities

While idempotency is user-implemented, the WAL library can provide optional helpers to reduce boilerplate:

### Resource LSN Tracking

```c
// Helper: track last applied LSN per resource
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
// Helper: track applied transaction LSNs (Bloom filter or hash set)
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
- **Single-file storage**: Append-only file with automatic compaction after checkpoint
- **Flexible integration**: Callback-based recovery supports various storage models
- **Full ACID transactions**: Transaction support with undo/redo recovery and prev_lsn chains
- **Simplified API**: Clean separation between transactional and non-transactional operations

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
│  - Automatic compaction                  │
│  - Recovery via callback iteration       │
└──────────────────────────────────────────┘
```

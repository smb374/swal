const std = @import("std");

const record = @import("record.zig");
const atomic = std.atomic;

const Record = record.Record;
const WALRecordHeader = record.WALRecordHeader;

pub const QueueError = error{
    Full,
    NotReservedSlot,
};

pub fn MPSCQueue(comptime cap: u64) type {
    return struct {
        slots: [cap]atomic.Value(?*Record),
        head: atomic.Value(u64),
        size: atomic.Value(u64),
        tail: u64,
        allocator: std.mem.Allocator,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator) !*Self {
            var q = try allocator.create(Self);
            @memset(&q.slots, atomic.Value(?*Record).init(null));
            q.head = atomic.Value(u64).init(0);
            q.size = atomic.Value(u64).init(0);
            q.tail = 0;
            q.allocator = allocator;

            return q;
        }

        pub fn deinit(self: *Self) void {
            self.allocator.destroy(self);
        }

        pub fn reserve(self: *Self) QueueError!u64 {
            const r = self.size.fetchAdd(1, .acq_rel);
            if (r >= cap) {
                _ = self.size.fetchSub(1, .release);
                return error.Full;
            }

            return self.head.fetchAdd(1, .acq_rel);
        }

        pub fn produce(self: *Self, slot: u64, n: *Record) !void {
            const head = self.head.load(.acquire) % cap;
            const tail = self.tail;
            const s = slot % cap;
            // Valid:
            // t .. s .. h
            // 0 .. s .. h .. t
            // h .. t .. s .. cap
            if (s >= head or s < tail) {
                return error.NotReservedSlot;
            }
            if (self.slots[s].cmpxchgStrong(null, n, .acq_rel, .monotonic)) |_| {
                return error.NotReservedSlot;
            }
        }

        pub fn consume(self: *Self) ?*Record {
            if (self.slots[self.tail].swap(null, .acq_rel)) |n| {
                _ = self.size.fetchSub(1, .release);
                self.tail = (self.tail + 1) % cap;
                return n;
            }
            return null;
        }
    };
}

const expect = std.testing.expect;

test "Queue init" {
    const q = try MPSCQueue(1024).init(std.testing.allocator);
    defer q.deinit();
}

test "Queue basic" {
    var q = try MPSCQueue(1024).init(std.testing.allocator);
    defer q.deinit();

    const slot = try q.reserve();
    try expect(slot == 0);
    try expect(q.head.raw == 1);
    try expect(q.size.raw == 1);

    const data = "Hello World";
    const hdr: WALRecordHeader = WALRecordHeader.init(.{
        .lsn = 0xdeadbeef,
        .resource_id = 0xcafebeef,
    });
    const rec = try std.testing.allocator.create(Record);
    rec.* = Record.init(hdr, data[0..]);

    try q.produce(slot, rec);
    const prec = q.consume();
    try expect(prec != null);
    defer std.testing.allocator.destroy(prec.?);
    try expect(prec == rec);
}

test "Queue multiple producers single consumer" {
    const num_producers = 4;
    const records_per_producer = 100;
    const queue_cap = 512;

    var q = try MPSCQueue(queue_cap).init(std.testing.allocator);
    defer q.deinit();

    const ProducerContext = struct {
        queue: *MPSCQueue(queue_cap),
        producer_id: usize,
        records_produced: *atomic.Value(usize),
        allocator: std.mem.Allocator,
    };

    var records_produced = atomic.Value(usize).init(0);
    var threads: [num_producers]std.Thread = undefined;

    const producer_fn = struct {
        fn run(ctx: ProducerContext) void {
            var i: usize = 0;
            while (i < records_per_producer) : (i += 1) {
                const slot = ctx.queue.reserve() catch {
                    std.Thread.sleep(1000);
                    continue;
                };

                var buf: [64]u8 = undefined;
                const data = std.fmt.bufPrint(&buf, "Producer {} - Record {}", .{ ctx.producer_id, i }) catch unreachable;

                const hdr = WALRecordHeader.init(.{
                    .lsn = ctx.producer_id * records_per_producer + i,
                    .resource_id = ctx.producer_id,
                });
                const rec = ctx.allocator.create(Record) catch unreachable;
                rec.* = Record.init(hdr, data);

                ctx.queue.produce(slot, rec) catch {
                    ctx.allocator.destroy(rec);
                    continue;
                };

                _ = ctx.records_produced.fetchAdd(1, .monotonic);
            }
        }
    }.run;

    for (&threads, 0..) |*thread, i| {
        const ctx = ProducerContext{
            .queue = q,
            .producer_id = i,
            .records_produced = &records_produced,
            .allocator = std.testing.allocator,
        };
        thread.* = try std.Thread.spawn(.{}, producer_fn, .{ctx});
    }

    var consumed: usize = 0;
    const expected = num_producers * records_per_producer;

    while (consumed < expected) {
        if (q.consume()) |rec| {
            std.testing.allocator.destroy(rec);
            consumed += 1;
        } else {
            std.Thread.sleep(100);
        }
    }

    for (threads) |thread| {
        thread.join();
    }

    try expect(consumed == expected);
    try expect(records_produced.load(.monotonic) == expected);
}

test "Queue concurrent reserve and produce" {
    const num_threads = 8;
    const records_per_thread = 50;
    const queue_cap = 512;

    var q = try MPSCQueue(queue_cap).init(std.testing.allocator);
    defer q.deinit();

    const ThreadContext = struct {
        queue: *MPSCQueue(queue_cap),
        thread_id: usize,
        allocator: std.mem.Allocator,
        success_count: *atomic.Value(usize),
    };

    var success_count = atomic.Value(usize).init(0);
    var threads: [num_threads]std.Thread = undefined;

    const thread_fn = struct {
        fn run(ctx: ThreadContext) void {
            var i: usize = 0;
            while (i < records_per_thread) : (i += 1) {
                const slot = ctx.queue.reserve() catch {
                    std.Thread.sleep(500);
                    continue;
                };

                var buf: [64]u8 = undefined;
                const data = std.fmt.bufPrint(&buf, "Thread {} - {}", .{ ctx.thread_id, i }) catch unreachable;

                const hdr = WALRecordHeader.init(.{
                    .lsn = ctx.thread_id * 1000 + i,
                    .resource_id = ctx.thread_id,
                });
                const rec = ctx.allocator.create(Record) catch unreachable;
                rec.* = Record.init(hdr, data);

                ctx.queue.produce(slot, rec) catch {
                    ctx.allocator.destroy(rec);
                    std.Thread.sleep(100);
                    continue;
                };

                _ = ctx.success_count.fetchAdd(1, .monotonic);
            }
        }
    }.run;

    for (&threads, 0..) |*thread, i| {
        const ctx = ThreadContext{
            .queue = q,
            .thread_id = i,
            .allocator = std.testing.allocator,
            .success_count = &success_count,
        };
        thread.* = try std.Thread.spawn(.{}, thread_fn, .{ctx});
    }

    var consumed: usize = 0;
    const expected = num_threads * records_per_thread;

    while (consumed < expected or q.size.load(.acquire) > 0) {
        if (q.consume()) |rec| {
            std.testing.allocator.destroy(rec);
            consumed += 1;
        } else {
            std.Thread.sleep(100);
        }
    }

    for (threads) |thread| {
        thread.join();
    }

    try expect(consumed == success_count.load(.monotonic));
    try expect(q.size.load(.acquire) == 0);
}

test "Queue ordering verification with single consumer" {
    const num_producers = 4;
    const records_per_producer = 50;
    const queue_cap = 256;

    var q = try MPSCQueue(queue_cap).init(std.testing.allocator);
    defer q.deinit();

    const ProducerContext = struct {
        queue: *MPSCQueue(queue_cap),
        producer_id: usize,
        allocator: std.mem.Allocator,
    };

    var threads: [num_producers]std.Thread = undefined;

    const producer_fn = struct {
        fn run(ctx: ProducerContext) void {
            var i: usize = 0;
            while (i < records_per_producer) : (i += 1) {
                const slot = ctx.queue.reserve() catch {
                    std.Thread.sleep(500);
                    continue;
                };

                var buf: [32]u8 = undefined;
                const data = std.fmt.bufPrint(&buf, "Data", .{}) catch unreachable;

                const hdr = WALRecordHeader.init(.{
                    .lsn = i,
                    .resource_id = ctx.producer_id,
                });
                const rec = ctx.allocator.create(Record) catch unreachable;
                rec.* = Record.init(hdr, data);

                ctx.queue.produce(slot, rec) catch {
                    ctx.allocator.destroy(rec);
                    continue;
                };
            }
        }
    }.run;

    for (&threads, 0..) |*thread, i| {
        const ctx = ProducerContext{
            .queue = q,
            .producer_id = i,
            .allocator = std.testing.allocator,
        };
        thread.* = try std.Thread.spawn(.{}, producer_fn, .{ctx});
    }

    var consumed: usize = 0;
    const expected = num_producers * records_per_producer;
    var lsn_per_producer: [num_producers]usize = [_]usize{0} ** num_producers;

    while (consumed < expected) {
        if (q.consume()) |rec| {
            const producer_id = rec.hdr.resource_id;
            const lsn = rec.hdr.lsn;

            try expect(lsn == lsn_per_producer[producer_id]);
            lsn_per_producer[producer_id] += 1;

            std.testing.allocator.destroy(rec);
            consumed += 1;
        } else {
            std.Thread.sleep(100);
        }
    }

    for (threads) |thread| {
        thread.join();
    }

    for (lsn_per_producer) |count| {
        try expect(count == records_per_producer);
    }
}

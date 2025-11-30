const std = @import("std");

const RingBufError = error{
    Overflowed,
};

pub const RingBuf = struct {
    head: u64,
    tail: u64,
    size: u64,
    mask: u64,
    buf: []u8,
    allocator: std.mem.Allocator,

    pub fn init(cap: u64, allocator: std.mem.Allocator) !RingBuf {
        const alloc_size = std.math.ceilPowerOfTwoAssert(u64, cap);
        const buf = try allocator.alloc(u8, alloc_size);

        return .{
            .head = 0,
            .tail = 0,
            .size = 0,
            .mask = alloc_size - 1,
            .buf = buf,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RingBuf) void {
        self.allocator.free(self.buf);
        self.head = 0;
        self.tail = 0;
        self.size = 0;
    }

    pub fn write(self: *RingBuf, wbuf: []const u8) !void {
        if (wbuf.len > self.mask + 1 - self.size) {
            return error.Overflowed;
        }
        const hroom = self.mask + 1 - self.head;
        if (hroom >= wbuf.len) {
            @memcpy(self.buf[self.head..][0..wbuf.len], wbuf);
        } else {
            @memcpy(self.buf[self.head..], wbuf[0..hroom]);
            @memcpy(self.buf[0 .. wbuf.len - hroom], wbuf[hroom..]);
        }
        self.head = (self.head + wbuf.len) & self.mask;
        self.size += wbuf.len;
    }

    pub fn read(self: *RingBuf, rbuf: []u8) u64 {
        const nread = @min(rbuf.len, self.size);
        const troom = self.mask + 1 - self.tail;
        if (troom >= nread) {
            @memcpy(rbuf[0..nread], self.buf[self.tail..][0..nread]);
        } else {
            @memcpy(rbuf[0..troom], self.buf[self.tail..]);
            @memcpy(rbuf[troom..nread], self.buf[0 .. nread - troom]);
        }
        self.tail = (self.tail + nread) & self.mask;
        self.size -= nread;
        return nread;
    }

    pub fn available(self: *const RingBuf) u64 {
        return self.size;
    }

    pub fn capacity(self: *const RingBuf) u64 {
        return self.mask + 1;
    }
};

const expect = std.testing.expect;

test "rb init" {
    var rb = try RingBuf.init(64, std.testing.allocator);
    defer rb.deinit();

    try expect(rb.head == 0);
    try expect(rb.tail == 0);
    try expect(rb.size == 0);
    try expect(rb.available() == 0);
    try expect(rb.buf.len == 64);
    try expect(rb.mask == 63);
}

test "rb write basic" {
    var rb = try RingBuf.init(64, std.testing.allocator);
    defer rb.deinit();

    const wbuf = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    try rb.write(&wbuf);
    try expect(rb.head == 16);
    try expect(rb.size == 16);
    try expect(rb.available() == 16);
}

test "rb write wrap-around" {
    var rb = try RingBuf.init(64, std.testing.allocator);
    defer rb.deinit();

    var wbuf: [50]u8 = undefined;
    @memset(&wbuf, 0xAA);
    try rb.write(&wbuf);
    try expect(rb.size == 50);

    var rbuf: [30]u8 = undefined;
    @memset(&rbuf, 0);
    try expect(rb.read(&rbuf) == 30);
    try expect(rb.size == 20);
    try expect(rb.tail == 30);

    @memset(wbuf[0..40], 0xBB);
    try rb.write(wbuf[0..40]);
    try expect(rb.size == 60);
    try expect(rb.head == 26);
}

test "rb write overflowed" {
    var rb = try RingBuf.init(64, std.testing.allocator);
    defer rb.deinit();

    var wbuf: [70]u8 = undefined;
    @memset(&wbuf, 0xFF);
    try expect(rb.write(&wbuf) == error.Overflowed);
    try expect(rb.size == 0);
}

test "rb read basic" {
    var rb = try RingBuf.init(64, std.testing.allocator);
    defer rb.deinit();

    const wbuf = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    try rb.write(&wbuf);
    try expect(rb.head == 16);
    try expect(rb.size == 16);

    var rbuf: [16]u8 = undefined;
    @memset(&rbuf, 0);
    try expect(rb.read(&rbuf) == 16);
    try expect(rb.size == 0);
    try expect(rb.head == rb.tail);
    try expect(std.mem.eql(u8, &wbuf, &rbuf));
}

test "rb read wrap-around" {
    var rb = try RingBuf.init(64, std.testing.allocator);
    defer rb.deinit();

    var wbuf: [60]u8 = undefined;
    var rbuf: [60]u8 = undefined;

    for (0..60) |i| {
        wbuf[i] = @truncate(i);
    }
    try rb.write(&wbuf);
    try expect(rb.size == 60);
    try expect(rb.read(rbuf[0..50]) == 50);
    try expect(std.mem.eql(u8, wbuf[0..50], rbuf[0..50]));

    for (0..50) |i| {
        wbuf[i] = @truncate(i + 100);
    }
    try rb.write(wbuf[0..50]);
    try expect(rb.size == 60);
    try expect(rb.read(rbuf[0..30]) == 30);
    try expect(std.mem.eql(u8, wbuf[50..], rbuf[0..10]));
    try expect(std.mem.eql(u8, wbuf[0..20], rbuf[10..30]));
}

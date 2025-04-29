pub const std = @import("std");
pub const lmdb = @import("lmdb-zig");

pub fn main() !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try tmp.dir.realpath("./", &buf);

    const env = try lmdb.Env.init(path, .{});
    defer env.deinit();

    const iters = 2_000;
    const word_len = 5;
    const words_cnt = 3;
    const key_len = (word_len + 1) * words_cnt - 1;
    const keys = comptime blk: {
        @setEvalBranchQuota(1.3 * iters * words_cnt * word_len);
        var keys = std.mem.zeroes([iters][key_len]u8);
        for (0..iters) |i| for (0..words_cnt) |wi| {
            for (0..word_len) |j|
                keys[i][(word_len + 1) * wi + j] = 97 + (i + j) % 25;
            if (wi != words_cnt - 1)
                keys[i][(word_len + 1) * wi + word_len] = '-';
        };
        break :blk keys;
    };

    var t = try std.time.Timer.start();
    t.reset();
    for (keys) |k| {
        const tx = try env.begin(.{});
        errdefer tx.deinit();
        const db = try tx.open(null, .{});
        defer db.close(env);
        try tx.put(db, &k, &k, .{});
        try tx.commit();
    }
    const ns_write = t.read();
    std.debug.print("write {d} keys {d:.2} ms {d:.2} ops/s\n", .{ iters, ns_write / 1_000, (iters * 1_000_000_000) / ns_write });
    t.reset();
    const get_iters_mul = 1000;
    for (0..get_iters_mul) |_| for (keys) |k| {
        const tx = try env.begin(.{});
        errdefer tx.deinit();
        const db = try tx.open(null, .{});
        defer db.close(env);
        const x = try tx.get(db, &k);
        if (std.mem.eql(u8, x, &k) == false)
            @panic(x);
        try tx.commit();
    };
    const ns_read = t.read();
    std.debug.print("read {d}*{d} keys {d:.2} ms {d:.2} ops/s\n", .{ iters, get_iters_mul, ns_read / 1_000, (get_iters_mul * iters * 1_000_000_000) / ns_read });
}

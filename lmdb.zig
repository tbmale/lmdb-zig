pub const std = @import("std");
pub const assert = std.debug.assert;
pub const c = @cImport(@cInclude("lmdb.h"));

// zig fmt: off
pub const is_debug = true;
pub inline fn t_is_indexable(T: type) bool {
    const ti = @typeInfo(T);
    if (ti == .Pointer) {
        if (ti.Pointer.size == .One)
            return @typeInfo(std.meta.Child(T)) == .Array;
        return true;
    }
    return ti == .Array or ti == .Vector or (ti == .Struct and ti.Struct.is_tuple == true);
}

pub const Mdb_Err = error { 
    already_exists, not_found, page_not_found, page_corrupted, panic, version_mismatch, file_not_database, 
    map_size_limit_reached, max_num_dbs_limit_reached, max_num_readers_limit_reached, too_many_envs_opened, 
    tx_too_big, cursor_stack_limit_reached, out_ouf_page_mem, db_exceeds_map_size_limit, incompatible_operation,
    invalid_reader_locktable_slot_reuse, tx_not_aborted, unsopported_size, bad_db_handle, no_such_file_or_dir, io_err, 
    out_of_mem, read_only, device_or_resource_busy, invalid_param, no_space_left_on_device, file_already_exists,
    unrecognized_err,
};
pub inline fn ti_call_fn_res(comptime fn_ti: std.builtin.Type.Fn) type { return if (fn_ti.return_type == c_int) Mdb_Err!void else void; }
pub inline fn call(comptime f: anytype, args: anytype) ti_call_fn_res(@typeInfo(@TypeOf(f)).Fn) {
    const ret_code = @call(.auto, f, args);
    if (ti_call_fn_res(@typeInfo(@TypeOf(f)).Fn) == void) return ret_code;
    return switch (ret_code) {
        c.MDB_SUCCESS => {},
        c.MDB_KEYEXIST => error.already_exists,
        c.MDB_NOTFOUND => error.not_found,
        c.MDB_PAGE_NOTFOUND => error.page_not_found,
        c.MDB_CORRUPTED => error.page_corrupted,
        c.MDB_PANIC => error.panic,
        c.MDB_VERSION_MISMATCH => error.version_mismatch,
        c.MDB_INVALID => error.file_not_database,
        c.MDB_MAP_FULL => error.map_size_limit_reached,
        c.MDB_DBS_FULL => error.max_num_dbs_limit_reached,
        c.MDB_READERS_FULL => error.max_num_readers_limit_reached,
        c.MDB_TLS_FULL => error.too_many_envs_opened,
        c.MDB_TXN_FULL => error.tx_too_big,
        c.MDB_CURSOR_FULL => error.cursor_stack_limit_reached,
        c.MDB_PAGE_FULL => error.out_ouf_page_mem,
        c.MDB_MAP_RESIZED => error.db_exceeds_map_size_limit,
        c.MDB_INCOMPATIBLE => error.incompatible_operation,
        c.MDB_BAD_RSLOT => error.invalid_reader_locktable_slot_reuse,
        c.MDB_BAD_TXN => error.tx_not_aborted,
        c.MDB_BAD_VALSIZE => error.unsopported_size,
        c.MDB_BAD_DBI => error.bad_db_handle,
        @intFromEnum(std.os.linux.E.NOENT) => error.no_such_file_or_dir,
        @intFromEnum(std.os.linux.E.IO) => error.io_err,
        @intFromEnum(std.os.linux.E.NOMEM) => error.out_of_mem,
        @intFromEnum(std.os.linux.E.ACCES) => error.read_only,
        @intFromEnum(std.os.linux.E.BUSY) => error.device_or_resource_busy,
        @intFromEnum(std.os.linux.E.INVAL) => error.invalid_param,
        @intFromEnum(std.os.linux.E.NOSPC) => error.no_space_left_on_device,
        @intFromEnum(std.os.linux.E.EXIST) => error.file_already_exists,
        else => if (comptime is_debug) std.debug.panic("({}) {s}", .{ ret_code, c.mdb_strerror(ret_code) }) else error.unrecognized_err,
    };
}
pub inline fn str_from_c(val: c.MDB_val) []u8 { return @as([*]u8, @ptrCast(val.mv_data))[0..val.mv_size]; }
pub inline fn str_to_c(str: []const u8) c.MDB_val { return c.MDB_val{ .mv_size = str.len, .mv_data = @constCast(str.ptr) }; }

test { std.testing.refAllDecls(@This()); }


pub const Env = packed struct {
    const Self = @This();
    inner: ?*c.MDB_env,

    pub const Flags_Open = struct {
        mode: c.mdb_mode_t = 0o664,
        map_size: ?usize = null,
        max_num_readers: ?usize = null,
        max_num_dbs: ?usize = null,

        fix_mapped_address: bool = false,
        no_sub_directory: bool = false,
        read_only: bool = false,
        use_writable_memory_map: bool = false,
        dont_sync_metadata: bool = false,
        dont_sync: bool = false,
        flush_asynchronously: bool = false,
        disable_thread_local_storage: bool = false,
        disable_locks: bool = false,
        disable_readahead: bool = false,
        disable_memory_initialization: bool = false,
        pub inline fn to_c(self: Self.Flags_Open) c_uint {
            var flags: c_uint = 0;
            if (self.fix_mapped_address) flags |= c.MDB_FIXEDMAP;
            if (self.no_sub_directory) flags |= c.MDB_NOSUBDIR;
            if (self.read_only) flags |= c.MDB_RDONLY;
            if (self.use_writable_memory_map) flags |= c.MDB_WRITEMAP;
            if (self.dont_sync_metadata) flags |= c.MDB_NOMETASYNC;
            if (self.dont_sync) flags |= c.MDB_NOSYNC;
            if (self.flush_asynchronously) flags |= c.MDB_MAPASYNC;
            if (self.disable_thread_local_storage) flags |= c.MDB_NOTLS;
            if (self.disable_locks) flags |= c.MDB_NOLOCK;
            if (self.disable_readahead) flags |= c.MDB_NORDAHEAD;
            if (self.disable_memory_initialization) flags |= c.MDB_NOMEMINIT;
            return flags;
        }
    };
    pub inline fn init(env_path: []const u8, flags: Self.Flags_Open) !Self {
        var inner: ?*c.MDB_env = null;
        try call(c.mdb_env_create, .{&inner});
        errdefer call(c.mdb_env_close, .{inner});
        if (flags.map_size) |map_size|
            try call(c.mdb_env_set_mapsize, .{ inner, map_size });
        if (flags.max_num_readers) |max_num_readers|
            try call(c.mdb_env_set_maxreaders, .{ inner, @as(c_uint, @intCast(max_num_readers)) });
        if (flags.max_num_dbs) |max_num_dbs|
            try call(c.mdb_env_set_maxdbs, .{ inner, @as(c_uint, @intCast(max_num_dbs)) });
        if (!std.mem.endsWith(u8, env_path, &[_]u8{0})) {
            assert(env_path.len + 1 <= std.fs.MAX_PATH_BYTES);
            var fixed_path: [std.fs.MAX_PATH_BYTES + 1]u8 = undefined;
            std.mem.copyForwards(u8, &fixed_path, env_path);
            fixed_path[env_path.len] = 0;
            try call(c.mdb_env_open, .{ inner, fixed_path[0 .. env_path.len + 1].ptr, flags.to_c(), flags.mode });
        } else
            try call(c.mdb_env_open, .{ inner, env_path.ptr, flags.to_c(), flags.mode });
        return .{ .inner = inner };
    }
    pub inline fn deinit(self: Self) void { call(c.mdb_env_close, .{self.inner}); }

    pub const Flags_Copy = packed struct {
        compact: bool = false,
        pub inline fn to_c(self: Self.Flags_Copy) c_uint {
            var flags: c_uint = 0;
            if (self.compact) flags |= c.MDB_CP_COMPACT;
            return flags;
        }
    };
    pub inline fn save_to(self: Self, backup_path: []const u8, flags: Flags_Copy) !void {
        if (!std.mem.endsWith(u8, backup_path, &[_]u8{0})) {
            assert(backup_path.len + 1 <= std.fs.MAX_PATH_BYTES);
            var fixed_path: [std.fs.MAX_PATH_BYTES + 1]u8 = undefined;
            std.mem.copyForwards(u8, &fixed_path, backup_path);
            fixed_path[backup_path.len] = 0;
            try call(c.mdb_env_copy2, .{ self.inner, fixed_path[0 .. backup_path.len + 1].ptr, flags.to_c() });
        } else
            try call(c.mdb_env_copy2, .{ self.inner, backup_path.ptr, flags.to_c() });
    }
    pub inline fn pipe_to(self: Self, fd_handle: std.os.linux.fd_t, flags: Flags_Copy) !void {
        try call(c.mdb_env_copyfd2, .{ self.inner, fd_handle, flags.to_c() });
    }
    pub inline fn get_max_key_size(self: Self) usize {
        return @as(usize, @intCast(c.mdb_env_get_maxkeysize(self.inner)));
    }
    pub inline fn get_max_num_readers(self: Self) !usize {
        var max_num_readers: c_uint = 0;
        try call(c.mdb_env_get_maxreaders, .{ self.inner, &max_num_readers });
        return @as(usize, @intCast(max_num_readers));
    }
    pub inline fn set_map_size(self: Self, map_size: ?usize) !void {
        try call(c.mdb_env_set_mapsize, .{ self.inner, if (map_size) |size| size else 0 });
    }

    pub const Flags = struct {
        fix_mapped_address: bool = false,
        no_sub_directory: bool = false,
        read_only: bool = false,
        use_writable_memory_map: bool = false,
        dont_sync_metadata: bool = false,
        dont_sync: bool = false,
        flush_asynchronously: bool = false,
        disable_thread_local_storage: bool = false,
        disable_locks: bool = false,
        disable_readahead: bool = false,
        disable_memory_initialization: bool = false,
        pub inline fn from_c(flags: c_uint) Flags {
            return Flags{
                .fix_mapped_address = flags & c.MDB_FIXEDMAP != 0,
                .no_sub_directory = flags & c.MDB_NOSUBDIR != 0,
                .read_only = flags & c.MDB_RDONLY != 0,
                .use_writable_memory_map = flags & c.MDB_WRITEMAP != 0,
                .dont_sync_metadata = flags & c.MDB_NOMETASYNC != 0,
                .dont_sync = flags & c.MDB_NOSYNC != 0,
                .flush_asynchronously = flags & c.MDB_MAPASYNC != 0,
                .disable_thread_local_storage = flags & c.MDB_NOTLS != 0,
                .disable_locks = flags & c.MDB_NOLOCK != 0,
                .disable_readahead = flags & c.MDB_NORDAHEAD != 0,
                .disable_memory_initialization = flags & c.MDB_NOMEMINIT != 0,
            };
        }
        pub inline fn to_c(self: Self.Flags) c_uint {
            var flags: c_uint = 0;
            if (self.fix_mapped_address) flags |= c.MDB_FIXEDMAP;
            if (self.no_sub_directory) flags |= c.MDB_NOSUBDIR;
            if (self.read_only) flags |= c.MDB_RDONLY;
            if (self.use_writable_memory_map) flags |= c.MDB_WRITEMAP;
            if (self.dont_sync_metadata) flags |= c.MDB_NOMETASYNC;
            if (self.dont_sync) flags |= c.MDB_NOSYNC;
            if (self.flush_asynchronously) flags |= c.MDB_MAPASYNC;
            if (self.disable_thread_local_storage) flags |= c.MDB_NOTLS;
            if (self.disable_locks) flags |= c.MDB_NOLOCK;
            if (self.disable_readahead) flags |= c.MDB_NORDAHEAD;
            if (self.disable_memory_initialization) flags |= c.MDB_NOMEMINIT;
            return flags;
        }
    };
    pub inline fn get_flags(self: Self) !Flags {
        var inner: c_uint = undefined;
        try call(c.mdb_env_get_flags, .{ self.inner, &inner });
        return Flags.from_c(inner);
    }

    pub const Flags_Mut = struct {
        dont_sync_metadata: bool = false,
        dont_sync: bool = false,
        flush_asynchronously: bool = false,
        disable_memory_initialization: bool = false,
        pub inline fn to_c(self: Self.Flags_Mut) c_uint {
            var flags: c_uint = 0;
            if (self.dont_sync_metadata) flags |= c.MDB_NOMETASYNC;
            if (self.dont_sync) flags |= c.MDB_NOSYNC;
            if (self.flush_asynchronously) flags |= c.MDB_MAPASYNC;
            if (self.disable_memory_initialization) flags |= c.MDB_NOMEMINIT;
            return flags;
        }
    };
    pub inline fn flags_on(self: Self, flags: Flags_Mut) !void {
        try call(c.mdb_env_set_flags, .{ self.inner, flags.to_c(), 1 });
    }
    pub inline fn flags_off(self: Self, flags: Flags_Mut) !void {
        try call(c.mdb_env_set_flags, .{ self.inner, flags.to_c(), 0 });
    }
    pub inline fn path(self: Self) ![]const u8 {
        var env_path: [:0]const u8 = undefined;
        try call(c.mdb_env_get_path, .{ self.inner, @as([*c][*c]const u8, @ptrCast(&env_path.ptr)) });
        env_path.len = std.mem.indexOfSentinel(u8, 0, env_path.ptr);
        return env_path;
    }

    pub const Stats = struct {
        page_size: usize,
        tree_height: usize,
        num_branch_pages: usize,
        num_leaf_pages: usize,
        num_overflow_pages: usize,
        num_entries: usize,
    };
    pub inline fn stat(self: Self) !Stats {
        var inner: c.MDB_stat = undefined;
        try call(c.mdb_env_stat, .{ self.inner, &inner });
        return Stats{
            .page_size = @as(usize, @intCast(inner.ms_psize)),
            .tree_height = @as(usize, @intCast(inner.ms_depth)),
            .num_branch_pages = @as(usize, @intCast(inner.ms_branch_pages)),
            .num_leaf_pages = @as(usize, @intCast(inner.ms_leaf_pages)),
            .num_overflow_pages = @as(usize, @intCast(inner.ms_overflow_pages)),
            .num_entries = @as(usize, @intCast(inner.ms_entries)),
        };
    }
    pub inline fn to_fd(self: Self) !std.os.linux.fd_t {
        var inner: std.os.linux.fd_t = undefined;
        try call(c.mdb_env_get_fd, .{ self.inner, &inner });
        return inner;
    }

    pub const Info = struct {
        map_address: ?[*]u8,
        map_size: usize,
        last_page_num: usize,
        last_tx_id: usize,
        max_num_reader_slots: usize,
        num_used_reader_slots: usize,
    };
    pub inline fn info(self: Self) !Info {
        var inner: c.MDB_envinfo = undefined;
        try call(c.mdb_env_info, .{ self.inner, &inner });
        return Info{
            .map_address = @as(?[*]u8, @ptrCast(inner.me_mapaddr)),
            .map_size = @as(usize, @intCast(inner.me_mapsize)),
            .last_page_num = @as(usize, @intCast(inner.me_last_pgno)),
            .last_tx_id = @as(usize, @intCast(inner.me_last_txnid)),
            .max_num_reader_slots = @as(usize, @intCast(inner.me_maxreaders)),
            .num_used_reader_slots = @as(usize, @intCast(inner.me_numreaders)),
        };
    }
    pub inline fn begin(self: Self, flags: Tx.Flags) !Tx {
        var inner: ?*c.MDB_txn = null;
        const maybe_parent = if (flags.parent) |parent| parent.inner else null;
        try call(c.mdb_txn_begin, .{ self.inner, maybe_parent, flags.to_c(), &inner });
        return Tx{ .inner = inner };
    }
    pub inline fn sync(self: Self, force: bool) !void {
        try call(c.mdb_env_sync, .{ self.inner, @as(c_int, if (force) 1 else 0) });
    }
    pub inline fn purge(self: Self) !usize {
        var count: c_int = undefined;
        try call(c.mdb_reader_check, .{ self.inner, &count });
        return @as(usize, @intCast(count));
    }
};

pub const Db = struct {
    const Self = @This();
    inner: c.MDB_dbi,
    pub inline fn close(self: Self, env: Env) void { call(c.mdb_dbi_close, .{ env.inner, self.inner }); }

    pub const Flags_Open = packed struct {
        compare_keys_in_reverse_order: bool = false,
        allow_duplicate_keys: bool = false,
        keys_are_integers: bool = false,
        duplicate_entries_are_fixed_size: bool = false,
        duplicate_keys_are_integers: bool = false,
        compare_duplicate_keys_in_reverse_order: bool = false,
        /// only use with named database
        create_if_not_exists: bool = false,
        pub inline fn to_c(self: @This()) c_uint {
            var flags: c_uint = 0;
            if (self.compare_keys_in_reverse_order) flags |= c.MDB_REVERSEKEY;
            if (self.allow_duplicate_keys) flags |= c.MDB_DUPSORT;
            if (self.keys_are_integers) flags |= c.MDB_INTEGERKEY;
            if (self.duplicate_entries_are_fixed_size) flags |= c.MDB_DUPFIXED;
            if (self.duplicate_keys_are_integers) flags |= c.MDB_INTEGERDUP;
            if (self.compare_duplicate_keys_in_reverse_order) flags |= c.MDB_REVERSEDUP;
            if (self.create_if_not_exists) flags |= c.MDB_CREATE;
            return flags;
        }
    };
};



pub const Tx = packed struct {
    const Self = @This();
    inner: ?*c.MDB_txn,

    pub const Flags = struct {
        parent: ?Self = null,
        read_only: bool = false,
        dont_sync: bool = false,
        dont_sync_metadata: bool = false,
        pub inline fn to_c(self: Self.Flags) c_uint {
            var flags: c_uint = 0;
            if (self.read_only) flags |= c.MDB_RDONLY;
            if (self.dont_sync) flags |= c.MDB_NOSYNC;
            if (self.dont_sync_metadata) flags |= c.MDB_NOMETASYNC;
            return flags;
        }
    };
    pub inline fn id(self: Self) usize { return @as(usize, @intCast(c.mdb_txn_id(self.inner))); }
    pub inline fn open(self: Self, name: ?[]const u8, flags: Db.Flags_Open) !Db {
        var inner: c.MDB_dbi = 0;
        try call(c.mdb_dbi_open, .{ self.inner, if (name) |s| s.ptr else null, flags.to_c(), &inner });
        return Db{ .inner = inner };
    }
    pub inline fn cursor(self: Self, db: Db) !Cursor {
        var inner: ?*c.MDB_cursor = undefined;
        try call(c.mdb_cursor_open, .{ self.inner, db.inner, &inner });
        return Cursor{ .inner = inner };
    }
    pub inline fn set_key_order(self: Self, db: Db, comptime order: fn (a: []const u8, b: []const u8) std.math.Order) !void {
        const S = struct {
            fn cmp(a: ?*const c.MDB_val, b: ?*const c.MDB_val) callconv(.C) c_int {
                return switch (order(str_from_c(a.?.*), str_from_c(b.?.*))) {
                    .eq => 0,
                    .lt => -1,
                    .gt => 1,
                };
            }
        };
        try call(c.mdb_set_compare, .{ self.inner, db.inner, S.cmp });
    }
    pub inline fn set_item_order(self: Self, db: Db, comptime order: fn (a: []const u8, b: []const u8) std.math.Order) !void {
        const S = struct {
            fn cmp(a: ?*const c.MDB_val, b: ?*const c.MDB_val) callconv(.C) c_int {
                return switch (order(str_from_c(a.?.*), str_from_c(b.?.*))) {
                    .eq => 0,
                    .lt => -1,
                    .gt => 1,
                };
            }
        };
        try call(c.mdb_set_dupsort, .{ self.inner, db.inner, S.cmp });
    }
    pub inline fn get(self: Self, db: Db, key: []const u8) ![]const u8 {
        var k = str_to_c(key);
        var v: c.MDB_val = undefined;
        try call(c.mdb_get, .{ self.inner, db.inner, &k, &v });
        return str_from_c(v);
    }

    pub const Flags_Put = packed struct {
        overwrite_key: bool = true,
        overwrite_item: bool = true,
        data_already_sorted: bool = false,
        set_already_sorted: bool = false,
        pub inline fn to_c(self: Flags_Put) c_uint {
            var flags: c_uint = 0;
            if (self.overwrite_key == false) flags |= c.MDB_NOOVERWRITE;
            if (self.overwrite_item == false) flags |= c.MDB_NODUPDATA;
            if (self.data_already_sorted) flags |= c.MDB_APPEND;
            if (self.set_already_sorted) flags |= c.MDB_APPENDDUP;
            return flags;
        }
    };
    pub inline fn put(self: Self, db: Db, key: []const u8, val: []const u8, flags: Flags_Put) !void {
        var k = str_to_c(key);
        var v = str_to_c(val);
        try call(c.mdb_put, .{ self.inner, db.inner, &k, &v, flags.to_c() });
    }
    pub inline fn get_or_put(self: Self, db: Db, key: []const u8, val: []const u8) !?[]const u8 {
        var k = str_to_c(key);
        var v = str_to_c(val);
        call(c.mdb_put, .{ self.inner, db.inner, &k, &v, c.MDB_NOOVERWRITE }) catch |err| switch (err) {
            error.already_exists => return str_from_c(v),
            else => return err,
        };
        return null;
    }

    pub const Flags_Reserve = packed struct {
        overwrite_key: bool = true,
        data_already_sorted: bool = false,
        pub inline fn to_c(self: Flags_Reserve) c_uint {
            var flags: c_uint = c.MDB_RESERVE;
            if (self.overwrite_key == false) flags |= c.MDB_NOOVERWRITE;
            if (self.data_already_sorted) flags |= c.MDB_APPEND;
            return flags;
        }
    };

    pub const Reserve_Res = union(enum) { successful: []u8, found_existing: []const u8 };
    pub inline fn reserve(self: Self, db: Db, key: []const u8, val_len: usize, flags: Flags_Reserve) !Reserve_Res {
        var k = str_to_c(key);
        var v = c.MDB_val{ .mv_size = val_len, .mv_data = null };
        call(c.mdb_put, .{ self.inner, db.inner, &k, &v, flags.to_c() }) catch |err| switch (err) {
            error.already_exists => return .{ .found_existing = str_from_c(v) },
            else => return err,
        };
        return .{ .successful = str_from_c(v) };
    }
    pub inline fn del(self: Self, db: Db, key: []const u8, op: union(enum) { key: void, item: []const u8 }) !void {
        var k = str_to_c(key);
        if (op == .key)
            return try call(c.mdb_del, .{ self.inner, db.inner, &k, @as([*c]c.MDB_val, @ptrCast(@alignCast(@constCast(&null))))});
        if (op == .item) |item| {
            var v = str_to_c(item);
            return try call(c.mdb_del, .{ self.inner, db.inner, &k, &v });
        }
    }
    pub inline fn drop(self: Self, db: Db, method: enum(c_int) { empty = 0, delete = 1 }) !void {
        try call(c.mdb_drop, .{ self.inner, db.inner, @intFromEnum(method) });
    }
    pub inline fn deinit(self: Self) void { call(c.mdb_txn_abort, .{self.inner}); }
    pub inline fn commit(self: Self) !void { try call(c.mdb_txn_commit, .{self.inner}); }
    pub inline fn renew(self: Self) !void { try call(c.mdb_txn_renew, .{self.inner}); }
    pub inline fn reset(self: Self) !void { try call(c.mdb_txn_reset, .{self.inner}); }
};

pub const Cursor = packed struct {
    pub const Entry = struct { key: []const u8, val: []const u8 };
    pub inline fn Page(comptime T: type) type { return struct { key: []const u8, items: []align(1) const T }; }
    const Self = @This();

    inner: ?*c.MDB_cursor,
    pub inline fn deinit(self: Self) void { call(c.mdb_cursor_close, .{self.inner}); }
    pub inline fn tx(self: Self) Tx { return .{ .inner = c.mdb_cursor_txn(self.inner) }; }
    pub inline fn db(self: Self) Db { return .{ .inner = c.mdb_cursor_dbi(self.inner) }; }
    pub inline fn renew(self: Self, parent: Tx) !void { try call(c.mdb_cursor_renew, .{ parent.inner, self.inner }); }
    pub inline fn count(self: Self) !usize {
        var inner: c.mdb_size_t = undefined;
        try call(c.mdb_cursor_count, .{ self.inner, &inner });
        return @as(usize, @intCast(inner));
    }

    pub fn update_item_inplc(self: Self, current_key: []const u8, new_val: anytype) !void {
        const bytes = if (t_is_indexable(@TypeOf(new_val))) std.mem.span(new_val) else std.mem.asBytes(&new_val);
        return self.update_inplc(current_key, bytes);
    }

    pub fn update_inplc(self: Self, current_key: []const u8, new_val: []const u8) !void {
        var k = str_to_c(current_key);
        var v = str_to_c(new_val);
        try call(c.mdb_cursor_put, .{ self.inner, &k, &v, c.MDB_CURRENT });
    }

    /// May not be used with databases supporting duplicate keys.
    pub fn reserve_inplc(self: Self, current_key: []const u8, new_val_len: usize) ![]u8 {
        var k = str_to_c(current_key);
        var v = c.MDB_val{ .mv_size = new_val_len, .mv_data = null };
        try call(c.mdb_cursor_put, .{ self.inner, &k, &v, c.MDB_CURRENT | c.MDB_RESERVE });
        return str_from_c(v);
    }

    pub const Flags_Put = packed struct {
        overwrite_key: bool = true,
        overwrite_item: bool = true,
        data_already_sorted: bool = false,
        set_already_sorted: bool = false,
        pub inline fn to_c(self: Flags_Put) c_uint {
            var flags: c_uint = 0;
            if (self.overwrite_key == false) flags |= c.MDB_NOOVERWRITE;
            if (self.overwrite_item == false) flags |= c.MDB_NODUPDATA;
            if (self.data_already_sorted) flags |= c.MDB_APPEND;
            if (self.set_already_sorted) flags |= c.MDB_APPENDDUP;
            return flags;
        }
    };
    pub inline fn put(self: Self, key: []const u8, val: []const u8, flags: Flags_Put) !void {
        var k = str_to_c(key);
        var v = str_to_c(val);
        try call(c.mdb_cursor_put, .{ self.inner, &k, &v, flags.to_c() });
    }
    // pub inline fn putItem(self: Self, key: []const u8, value: anytype, flags: Flags_Put) !usize {
    //     var k = str_to_c(key);
    //     var v = c.MDB_val{ .mv_size = @sizeOf(@TypeOf(value)), .mv_data = @as(*anyopaque, @constCast(&value)) };
    //     try call(c.mdb_cursor_put, .{ self.inner, &k, &v, flags.to_c() });
    // }
    /// Insert multiple values for a key
    /// value must be contiguous in memory, like []Foo
    pub inline fn put_batch(self: Self, key: []const u8, batch: anytype, flags: Flags_Put) !usize {
        comptime assert(t_is_indexable(@TypeOf(batch)));
        const el_size = comptime @sizeOf(std.meta.Elem(@TypeOf(batch)));
        var k = str_to_c(key);
        var batch_0 = batch[0];
        var v = [_]c.MDB_val{ .{ .mv_size = el_size, .mv_data = &batch_0 }, .{ .mv_size = batch.len, .mv_data = undefined } };
        try call(c.mdb_cursor_put, .{ self.inner, &k, &v, @as(c_uint, @intCast(c.MDB_MULTIPLE)) | flags.to_c() });
        return @as(usize, @intCast(v[1].mv_size));
    }
    pub inline fn get_or_put(self: Self, key: []const u8, val: []const u8) !?[]const u8 {
        var k = str_to_c(key);
        var v = str_to_c(val);
        call(c.mdb_cursor_put, .{ self.inner, &k, &v, c.MDB_NOOVERWRITE }) catch |err| switch (err) {
            error.already_exists => return str_from_c(v),
            else => return err,
        };
        return null;
    }

    pub const Flags_Reserve = packed struct {
        overwrite_key: bool = true,
        data_already_sorted: bool = false,
        pub inline fn to_c(self: Flags_Reserve) c_uint {
            var flags: c_uint = c.MDB_RESERVE;
            if (self.overwrite_key == false) flags |= c.MDB_NOOVERWRITE;
            if (self.data_already_sorted) flags |= c.MDB_APPEND;
            return flags;
        }
    };

    pub const Reserve_Res = union(enum) { successful: []u8, found_existing: []const u8 };
    pub inline fn reserve(self: Self, key: []const u8, val_len: usize, flags: Flags_Reserve) !Reserve_Res {
        var k = str_to_c(key);
        var v = c.MDB_val{ .mv_size = val_len, .mv_data = null };
        call(c.mdb_cursor_put, .{ self.inner, &k, &v, flags.to_c() }) catch |err| switch (err) {
            error.already_exists => return .{ .found_existing = str_from_c(v) },
            else => return err,
        };
        return .{ .successful = str_from_c(v) };
    }
    pub inline fn del(self: Self, op: enum(c_uint) { key = c.MDB_NODUPDATA, item = 0 }) !void {
        call(c.mdb_cursor_del, .{ self.inner, @intFromEnum(op) }) catch |err| switch (err) {
            error.invalid_param => return error.not_found,
            else => return err,
        };
    }

    pub const Pos = enum(c.MDB_cursor_op) {
        first = c.MDB_FIRST,
        first_item = c.MDB_FIRST_DUP,
        current = c.MDB_GET_CURRENT,
        last = c.MDB_LAST,
        last_item = c.MDB_LAST_DUP,
        next = c.MDB_NEXT,
        next_item = c.MDB_NEXT_DUP,
        next_key = c.MDB_NEXT_NODUP,
        prev = c.MDB_PREV,
        prev_item = c.MDB_PREV_DUP,
        prev_key = c.MDB_PREV_NODUP,
    };
    pub inline fn get(self: Self, pos: Pos) !?Entry {
        var k: c.MDB_val = undefined;
        var v: c.MDB_val = undefined;
        call(c.mdb_cursor_get, .{ self.inner, &k, &v, @intFromEnum(pos) }) catch |err| switch (err) {
            error.invalid_param => return if (pos == .current) null else err,
            error.not_found => return null,
            else => return err,
        };
        return .{ .key = str_from_c(k), .val = str_from_c(v) };
    }

    pub const Pos_Page = enum(c.MDB_cursor_op) {
        current = c.MDB_GET_MULTIPLE,
        next = c.MDB_NEXT_MULTIPLE,
        prev = c.MDB_PREV_MULTIPLE,
    };
    pub inline fn get_page(self: Self, comptime T: type, pos: Pos_Page) !?Page(T) {
        var k: c.MDB_val = undefined;
        var v: c.MDB_val = undefined;
        call(c.mdb_cursor_get, .{ self.inner, &k, &v, @intFromEnum(pos) }) catch |err| switch (err) {
            error.not_found => return null,
            else => return err,
        };
        return Page(T){ .key = str_from_c(k), .items = std.mem.bytesAsSlice(T, str_from_c(v)) };
    }
    pub inline fn seek_to_Item(self: Self, key: []const u8, val: []const u8) !void {
        var k = str_to_c(key);
        var v = str_to_c(val);
        try call(c.mdb_cursor_get, .{ self.inner, &k, &v, .MDB_GET_BOTH });
    }
    pub inline fn seek_from_item(self: Self, key: []const u8, val: []const u8) ![]const u8 {
        var k = str_to_c(key);
        var v = str_to_c(val);
        try call(c.mdb_cursor_get, .{ self.inner, &k, &v, c.MDB_GET_BOTH_RANGE });
        return str_from_c(v);
    }
    pub inline fn seek_to(self: Self, key: []const u8) ![]const u8 {
        @setEvalBranchQuota(10000);
        var k = str_to_c(key);
        var v: c.MDB_val = undefined;
        try call(c.mdb_cursor_get, .{ self.inner, &k, &v, c.MDB_SET_KEY });
        return str_from_c(v);
    }
    pub inline fn seek_from(self: Self, key: []const u8) !Entry {
        var k = str_to_c(key);
        var v: c.MDB_val = undefined;
        try call(c.mdb_cursor_get, .{ self.inner, &k, &v, c.MDB_SET_RANGE });
        return .{ .key = str_from_c(k), .val = str_from_c(v) };
    }
    pub inline fn first(self: Self) !?Entry { return self.get(.first); }
    pub inline fn first_item(self: Self) !?Entry { return self.get(.first_item); }
    pub inline fn current(self: Self) !?Entry { return self.get(.current); }
    pub inline fn last(self: Self) !?Entry { return self.get(.last); }
    pub inline fn last_item(self: Self) !?Entry { return self.get(.last_item); }
    pub inline fn next(self: Self) !?Entry { return self.get(.next); }
    pub inline fn next_item(self: Self) !?Entry { return self.get(.next_item); }
    pub inline fn next_key(self: Self) !?Entry { return self.get(.next_key); }
    pub inline fn prev(self: Self) !?Entry { return self.get(.prev); }
    pub inline fn prev_item(self: Self) !?Entry { return self.get(.prev_item); }
    pub inline fn prev_key(self: Self) !?Entry { return self.get(.prev_key); }
    pub inline fn current_page(self: Self, comptime T: type) !?Page(T) { return self.get_page(T, .current); }
    pub inline fn next_page(self: Self, comptime T: type) !?Page(T) { return self.get_page(T, .next); }
    pub inline fn prev_page(self: Self, comptime T: type) !?Page(T) { return self.get_page(T, .prev); }
};

pub inline fn test_eql_true(actual: bool) !void { try std.testing.expectEqual(true, actual); }
pub inline fn test_eql(actual: anytype, exp: anytype) !void { try std.testing.expectEqual(exp, actual); }
pub inline fn test_eql_str(actual: []const u8, exp: []const u8) !void { try std.testing.expectEqualStrings(exp, actual); }
pub inline fn test_eql_err(actual: anytype, exp_err: anyerror) !void { try std.testing.expectError(exp_err, actual); }
pub const test_tmp_dir = std.testing.tmpDir;

test "Env: .init() .deinit() .stats() .info() and flags" {
    var tmp = test_tmp_dir(.{});
    defer tmp.cleanup();

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try tmp.dir.realpath("./", &buf);

    const env = try Env.init(path, .{
        .use_writable_memory_map = true,
        .dont_sync_metadata = true,
        .map_size = 4 * 1024 * 1024,
        .max_num_readers = 42,
    });
    defer env.deinit();

    try test_eql_str(try env.path(), path);
    try test_eql_true(env.get_max_key_size() > 0);
    try test_eql(try env.get_max_num_readers(), 42);

    const stat = try env.stat();
    try test_eql(stat.tree_height, 0);
    try test_eql(stat.num_branch_pages, 0);
    try test_eql(stat.num_leaf_pages, 0);
    try test_eql(stat.num_overflow_pages, 0);
    try test_eql(stat.num_entries, 0);

    const flags = try env.get_flags();
    try test_eql(flags.use_writable_memory_map, true);
    try test_eql(flags.dont_sync_metadata, true);

    try env.flags_off(.{ .dont_sync_metadata = true });
    try test_eql((try env.get_flags()).dont_sync_metadata, false);

    try env.flags_on(.{ .dont_sync_metadata = true });
    try test_eql((try env.get_flags()).dont_sync_metadata, true);

    const info = try env.info();
    try test_eql(info.map_address, null);
    try test_eql(info.map_size, 4 * 1024 * 1024);
    try test_eql(info.last_page_num, 1);
    try test_eql(info.last_tx_id, 0);
    try test_eql_true(info.max_num_reader_slots > 0);
    try test_eql(info.num_used_reader_slots, 0);

    try env.set_map_size(8 * 1024 * 1024);
    try test_eql((try env.info()).map_size, 8 * 1024 * 1024);

    // The file descriptor should be >= 0.

    try test_eql_true((try env.to_fd()) >= 0);

    try test_eql((try env.purge()), 0);
}

test "Env.save_to(): backups and restore" {
    var tmp_a = test_tmp_dir(.{});
    defer tmp_a.cleanup();

    var tmp_b = test_tmp_dir(.{});
    defer tmp_b.cleanup();

    var buf_a: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    var buf_b: [std.fs.MAX_PATH_BYTES]u8 = undefined;

    const path_a = try tmp_a.dir.realpath("./", &buf_a);
    const path_b = try tmp_b.dir.realpath("./", &buf_b);

    const env_a = try Env.init(path_a, .{});
    {
        defer env_a.deinit();

        const tx = try env_a.begin(.{});
        errdefer tx.deinit();

        const db = try tx.open(null, .{});
        defer db.close(env_a);

        var i: u8 = 0;
        while (i < 128) : (i += 1) {
            try tx.put(db, &[_]u8{i}, &[_]u8{i}, .{ .overwrite_key = false });
            try test_eql_str(try tx.get(db, &[_]u8{i}), &[_]u8{i});
        }

        try tx.commit();
        try env_a.save_to(path_b, .{ .compact = true });
    }

    const env_b = try Env.init(path_b, .{});
    {
        defer env_b.deinit();

        const tx = try env_b.begin(.{});
        defer tx.deinit();

        const db = try tx.open(null, .{});
        defer db.close(env_b);

        var i: u8 = 0;
        while (i < 128) : (i += 1) {
            try test_eql_str(try tx.get(db, &[_]u8{i}), &[_]u8{i});
        }
    }
}

test "Env.sync(): manually flush system buffers" {
    var tmp = test_tmp_dir(.{});
    defer tmp.cleanup();

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try tmp.dir.realpath("./", &buf);

    const env = try Env.init(path, .{
        .dont_sync = true,
        .dont_sync_metadata = true,
        .use_writable_memory_map = true,
    });
    defer env.deinit();

    {
        const tx = try env.begin(.{});
        errdefer tx.deinit();

        const db = try tx.open(null, .{});
        defer db.close(env);

        var i: u8 = 0;
        while (i < 128) : (i += 1) {
            try tx.put(db, &[_]u8{i}, &[_]u8{i}, .{ .overwrite_key = false });
            try test_eql_str(try tx.get(db, &[_]u8{i}), &[_]u8{i});
        }

        try tx.commit();
        try env.sync(true);
    }

    {
        const tx = try env.begin(.{});
        defer tx.deinit();

        const db = try tx.open(null, .{});
        defer db.close(env);

        var i: u8 = 0;
        while (i < 128) : (i += 1) {
            try test_eql_str(try tx.get(db, &[_]u8{i}), &[_]u8{i});
        }
    }
}

test "Tx: .get() .put() .reserve() .delete() .commit() several entries with .overwrite_key = true / false" {
    var tmp = test_tmp_dir(.{});
    defer tmp.cleanup();

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try tmp.dir.realpath("./", &buf);

    const env = try Env.init(path, .{});
    defer env.deinit();

    const tx = try env.begin(.{});
    errdefer tx.deinit();

    const db = try tx.open(null, .{});
    defer db.close(env);

    // Tx.put() / Tx.get()

    try tx.put(db, "hello", "world", .{});
    try test_eql_str(try tx.get(db, "hello"), "world");

    // Tx.put() / Tx.reserve() / Tx.get() (.{ .overwrite_key = false })

    try test_eql_err(tx.put(db, "hello", "world", .{ .overwrite_key = false }), error.already_exists);
    {
        const result = try tx.reserve(db, "hello", "world".len, .{ .overwrite_key = false });
        try test_eql_str(result.found_existing, "world");
    }
    try test_eql_str(try tx.get(db, "hello"), "world");

    // Tx.put() / Tx.get() / Tx.reserve() (.{ .overwrite_key = true })

    try tx.put(db, "hello", "other_value", .{});
    try test_eql_str(try tx.get(db, "hello"), "other_value");
    {
        const result = try tx.reserve(db, "hello", "new_value".len, .{});
        try test_eql(result.successful.len, "new_value".len);
        std.mem.copyForwards(u8, result.successful, "new_value");
    }
    try test_eql_str(try tx.get(db, "hello"), "new_value");

    // Tx.del() / Tx.get() / Tx.put() / Tx.get()

    try tx.del(db, "hello", .key);

    try test_eql_err(tx.del(db, "hello", .key), error.not_found);
    try test_eql_err(tx.get(db, "hello"), error.not_found);

    try tx.put(db, "hello", "world", .{});
    try test_eql_str(try tx.get(db, "hello"), "world");

    // Tx.commit()

    try tx.commit();
}

test "Tx: .reserve() .write() and attempt to .reserve() again with overwrite_key = false" {
    var tmp = test_tmp_dir(.{});
    defer tmp.cleanup();

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try tmp.dir.realpath("./", &buf);

    const env = try Env.init(path, .{});
    defer env.deinit();

    const tx = try env.begin(.{});
    errdefer tx.deinit();

    const db = try tx.open(null, .{});
    defer db.close(env);

    switch (try tx.reserve(db, "hello", "world!".len, .{ .overwrite_key = false })) {
        .found_existing => try test_eql_true(false),
        .successful => |dst| std.mem.copyForwards(u8, dst, "world!"),
    }

    switch (try tx.reserve(db, "hello", "world!".len, .{ .overwrite_key = false })) {
        .found_existing => |src| try test_eql_str(src, "world!"),
        .successful => try test_eql_true(false),
    }

    try tx.commit();
}

test "Tx.get_or_put() twice" {
    var tmp = test_tmp_dir(.{});
    defer tmp.cleanup();

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try tmp.dir.realpath("./", &buf);

    const env = try Env.init(path, .{});
    defer env.deinit();

    const tx = try env.begin(.{});
    errdefer tx.deinit();

    const db = try tx.open(null, .{});
    defer db.close(env);

    try test_eql(try tx.get_or_put(db, "hello", "world"), @as(?[]const u8, null));
    try test_eql_str(try tx.get(db, "hello"), "world");
    try test_eql_str(try tx.get_or_put(db, "hello", "world") orelse unreachable, "world");

    try tx.commit();
}

test "Tx: use multiple named databases in a single transaction" {
    var tmp = test_tmp_dir(.{});
    defer tmp.cleanup();

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try tmp.dir.realpath("./", &buf);

    const env = try Env.init(path, .{ .max_num_dbs = 2 });
    defer env.deinit();

    {
        const tx = try env.begin(.{});
        errdefer tx.deinit();

        const a = try tx.open("A", .{ .create_if_not_exists = true });
        defer a.close(env);

        const b = try tx.open("B", .{ .create_if_not_exists = true });
        defer b.close(env);

        try tx.put(a, "hello", "this is in A!", .{});
        try tx.put(b, "hello", "this is in B!", .{});

        try tx.commit();
    }

    {
        const tx = try env.begin(.{});
        errdefer tx.deinit();

        const a = try tx.open("A", .{});
        defer a.close(env);

        const b = try tx.open("B", .{});
        defer b.close(env);

        try test_eql_str(try tx.get(a, "hello"), "this is in A!");
        try test_eql_str(try tx.get(b, "hello"), "this is in B!");

        try tx.commit();
    }
}

test "Tx: nest transaction inside transaction" {
    var tmp = test_tmp_dir(.{});
    defer tmp.cleanup();

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try tmp.dir.realpath("./", &buf);

    const env = try Env.init(path, .{});
    defer env.deinit();

    const parent = try env.begin(.{});
    errdefer parent.deinit();

    const db = try parent.open(null, .{});
    defer db.close(env);

    {
        const child = try env.begin(.{ .parent = parent });
        errdefer child.deinit();

        // Parent ID is equivalent to Child ID. Parent is not allowed to perform
        // operations while child has yet to be aborted / committed.

        try test_eql(child.id(), parent.id());

        // Operations cannot be performed against a parent transaction while a child
        // transaction is still active.

        try test_eql_err(parent.get(db, "hello"), error.tx_not_aborted);

        try child.put(db, "hello", "world", .{});
        try child.commit();
    }

    try test_eql_str(try parent.get(db, "hello"), "world");
    try parent.commit();
}

test "Tx: custom key comparator" {
    const Descending = struct {
        fn order(a: []const u8, b: []const u8) std.math.Order {
            return switch (std.mem.order(u8, a, b)) {
                .eq => .eq,
                .lt => .gt,
                .gt => .lt,
            };
        }
    };

    var tmp = test_tmp_dir(.{});
    defer tmp.cleanup();

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try tmp.dir.realpath("./", &buf);

    const env = try Env.init(path, .{ .max_num_dbs = 2 });
    defer env.deinit();

    const tx = try env.begin(.{});
    errdefer tx.deinit();

    const db = try tx.open(null, .{});
    defer db.close(env);

    const items = [_][]const u8{ "a", "b", "c" };

    try tx.set_key_order(db, Descending.order);

    for (items) |item| {
        try tx.put(db, item, item, .{ .overwrite_key = false });
    }

    {
        const cursor = try tx.cursor(db);
        defer cursor.deinit();

        var i: usize = 0;
        while (try cursor.next()) |item| : (i += 1) {
            try test_eql_str(item.key, items[items.len - 1 - i]);
            try test_eql_str(item.val, items[items.len - 1 - i]);
        }
    }

    try tx.commit();
}

test "Cursor: move around a database and add / delete some entries" {
    var tmp = test_tmp_dir(.{});
    defer tmp.cleanup();

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try tmp.dir.realpath("./", &buf);

    const env = try Env.init(path, .{});
    defer env.deinit();

    const tx = try env.begin(.{});
    errdefer tx.deinit();

    const db = try tx.open(null, .{});
    defer db.close(env);

    {
        const cursor = try tx.cursor(db);
        defer cursor.deinit();

        const items = [_][]const u8{ "a", "b", "c" };

        // Cursor.put()

        inline for (items) |item|
            try cursor.put(item, item, .{ .overwrite_key = false });

        // Cursor.current() / Cursor.first() / Cursor.last() / Cursor.next() / Cursor.prev()

        {
            const last_item = try cursor.last();
            try test_eql_str(last_item.?.key, items[items.len - 1]);
            try test_eql_str(last_item.?.val, items[items.len - 1]);

            {
                var i: usize = items.len - 1;
                while (true) {
                    const item = (try cursor.prev()) orelse break;
                    try test_eql_str(item.key, items[i - 1]);
                    try test_eql_str(item.val, items[i - 1]);
                    i -= 1;
                }
            }

            const current = try cursor.current();
            const first_item = try cursor.first();
            try test_eql_str(first_item.?.key, items[0]);
            try test_eql_str(first_item.?.val, items[0]);
            try test_eql_str(current.?.key, first_item.?.key);
            try test_eql_str(current.?.val, first_item.?.val);

            {
                var i: usize = 1;
                while (true) {
                    const item = (try cursor.next()) orelse break;
                    try test_eql_str(item.key, items[i]);
                    try test_eql_str(item.val, items[i]);
                    i += 1;
                }
            }
        }

        // Cursor.delete()

        try cursor.del(.key);
        while (try cursor.prev()) |_| try cursor.del(.key);
        try test_eql_err(cursor.del(.key), error.not_found);
        try test_eql((try cursor.current()), null);

        // Cursor.put() / Cursor.update_inplc() / Cursor.reserve_inplc()

        inline for (items) |item| {
            try cursor.put(item, item, .{ .overwrite_key = false });

            try cursor.update_inplc(item, "???");
            try test_eql_str((try cursor.current()).?.val, "???");

            std.mem.copyForwards(u8, try cursor.reserve_inplc(item, item.len), item);
            try test_eql_str((try cursor.current()).?.val, item);
        }

        // Cursor.seek_to()

        try test_eql_err(cursor.seek_to("0"), error.not_found);
        try test_eql_str(try cursor.seek_to(items[items.len / 2]), items[items.len / 2]);

        // Cursor.seek_from()

        try test_eql_str((try cursor.seek_from("0")).val, items[0]);
        try test_eql_str((try cursor.seek_from(items[items.len / 2])).val, items[items.len / 2]);
        try test_eql_err(cursor.seek_from("z"), error.not_found);
        try test_eql_str((try cursor.seek_from(items[items.len - 1])).val, items[items.len - 1]);
    }

    try tx.commit();
}

test "Cursor: interact with variable-sized items in a database with duplicate keys" {
    var tmp = test_tmp_dir(.{});
    defer tmp.cleanup();

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try tmp.dir.realpath("./", &buf);

    const env = try Env.init(path, .{ .max_num_dbs = 1 });
    defer env.deinit();

    const tx = try env.begin(.{});
    errdefer tx.deinit();

    const db = try tx.open(null, .{ .allow_duplicate_keys = true });
    defer db.close(env);

    const expected = comptime .{
        .{ "Another Set C", [_][]const u8{ "be", "ka", "kra", "tan" } },
        .{ "Set A", [_][]const u8{ "a", "kay", "zay" } },
        .{ "Some Set B", [_][]const u8{ "bru", "ski", "vle" } },
    };

    inline for (expected) |entry|
        inline for (entry[1]) |val|
            try tx.put(db, entry[0], val, .{ .overwrite_item = false });

    {
        const cursor = try tx.cursor(db);
        defer cursor.deinit();

        comptime var i = 0;
        comptime var j = 0;

        inline while (i < expected.len) : ({
            i += 1;
            j = 0;
        }) {
            inline while (j < expected[i][1].len) : (j += 1) {
                const maybe_entry = try cursor.next();
                const entry = maybe_entry orelse unreachable;
                try test_eql_str(entry.key, expected[i][0]);
                try test_eql_str(entry.val, expected[i][1][j]);
            }
        }
    }

    try tx.commit();
}

// test "Cursor: interact with batches of fixed-sized items in a database with duplicate keys" {
//     const U64 = struct {
//         fn order(a: []const u8, b: []const u8) std.math.Order {
//             const num_a = std.mem.bytesToValue(u64, a[0..8]);
//             const num_b = std.mem.bytesToValue(u64, b[0..8]);
//             if (num_a < num_b) return .lt;
//             if (num_a > num_b) return .gt;
//             return .eq;
//         }
//     };
// 
//     var tmp = test_tmp_dir(.{});
//     defer tmp.cleanup();
// 
//     var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
//     const path = try tmp.dir.realpath("./", &buf);
// 
//     const env = try Env.init(path, .{ .max_num_dbs = 1 });
//     defer env.deinit();
// 
//     const tx = try env.begin(.{});
//     errdefer tx.deinit();
// 
//     const db = try tx.open(null, .{
//         .allow_duplicate_keys = true,
//         .duplicate_entries_are_fixed_size = true,
//     });
//     defer db.close(env);
// 
//     try tx.set_item_order(db, U64.order);
// 
//     comptime var items: [512]u64 = undefined;
//     inline for (&items, 0..) |*item, i| item.* = @as(u64, i);
// 
//     const expected = comptime .{
//         .{ "Set A", &items },
//         .{ "Set B", &items },
//     };
// 
//     {
//         const cursor = try tx.cursor(db);
//         defer cursor.deinit();
//         inline for (expected) |entry|
//             try test_eql(try cursor.put_batch(entry[0], @as(*[512][8]u8, @ptrCast(entry[1])), .{}), entry[1].len);
//     }
// 
//     {
//         const cursor = try tx.cursor(db);
//         defer cursor.deinit();
// 
//         inline for (expected) |expected_entry| {
//             const maybe_entry = try cursor.next();
//             const entry = maybe_entry orelse unreachable;
//             try test_eql_str(entry.key, expected_entry[0]);
// 
//             var i: usize = 0;
//             while (try cursor.next_page(u64)) |page| {
//                 for (page.items) |item| {
//                     try test_eql(item, expected_entry[1][i]);
//                     i += 1;
//                 }
//             }
//         }
//     }
// 
//     try tx.commit();
// }

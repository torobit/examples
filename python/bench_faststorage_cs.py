import ctypes, os, platform, time
from enum import IntEnum

# ─────────────────── 1. Wire format enums & structs ────────────────────
class MessageKind(IntEnum): Depth = 0; Tick = 1
class MarketFlag(IntEnum):  Buy = 1; Sell = 2; Clear = 4

class MessageHeader(ctypes.Structure):
    _pack_ = 1
    _fields_ = [("Kind", ctypes.c_short),
                ("Size", ctypes.c_ushort),
                ("Time", ctypes.c_longlong)]

class DepthItem(ctypes.Structure):
    _pack_ = 1
    _fields_ = [("Header", MessageHeader),
                ("Price",  ctypes.c_longlong),
                ("Volume", ctypes.c_longlong),
                ("Flags",  ctypes.c_byte)]

class TickItem(ctypes.Structure):
    _pack_ = 1
    _fields_ = [("Header", MessageHeader),
                ("Id",     ctypes.c_longlong),
                ("Price",  ctypes.c_longlong),
                ("Volume", ctypes.c_longlong),
                ("Type",   ctypes.c_byte)]

# ─────────────────── 2. Native reader wrapper  ─────────────────────────
class FastReader:
    def __init__(self, file_path: str):
        self.lib = None
        self.reader_handle = None

        system = platform.system()
        if system == "Windows":
            lib_name = "FastStorage.Native.dll"
        elif system == "Darwin":
            lib_name = "FastStorage.Native.dylib"
        else:
            lib_name = "libFastStorage.Native.so"

        lib_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), lib_name)

        if not os.path.exists(lib_path):
            alt_lib_name = "lib" + lib_name if system == "Darwin" else ""
            alt_lib_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), alt_lib_name)
            if alt_lib_name and os.path.exists(alt_lib_path):
                lib_path = alt_lib_path
            else:
                raise FileNotFoundError(
                    f"Native library not found. Looked for '{lib_name}' at {os.path.dirname(lib_path)}. "
                    "Please copy the correct library file from the C# project's 'publish' directory."
                )

        self.lib = ctypes.CDLL(lib_path)
        self._define_signatures()

        self.reader_handle = ctypes.c_void_p()
        file_path_bytes = file_path.encode('utf-8')

        result = self.lib.open_reader(file_path_bytes, ctypes.byref(self.reader_handle))
        if result != 0 or not self.reader_handle.value:
            raise IOError(f"Native library failed to open file '{file_path}'. It may not exist or is locked.")

    def _define_signatures(self):
        self.lib.open_reader.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
        self.lib.open_reader.restype = ctypes.c_int
        self.lib.read_message.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        self.lib.read_message.restype = ctypes.c_int
        self.lib.close_reader.argtypes = [ctypes.c_void_p]
        self.lib.close_reader.restype = None

    def __iter__(self):
        return self

    def __next__(self):
        message_ptr = ctypes.c_void_p()
        message_size = self.lib.read_message(self.reader_handle, ctypes.byref(message_ptr))

        if message_size == 0:
            raise StopIteration
        if message_size < 0:
            if message_size == -2:
                raise IOError(
                    "Native library error: Corrupted data block. The file format is likely incorrect because it was created with an older version of the writer.")
            elif message_size == -3:
                raise IOError("Native library error: Unexpected end of file.")
            else:
                raise IOError(f"An unknown error occurred in the native library (code: {message_size}).")

        header = ctypes.cast(message_ptr, ctypes.POINTER(MessageHeader)).contents

        if header.Kind == MessageKind.Depth:
            return ctypes.cast(message_ptr, ctypes.POINTER(DepthItem)).contents
        elif header.Kind == MessageKind.Tick:
            return ctypes.cast(message_ptr, ctypes.POINTER(TickItem)).contents
        else:
            return None

    def close(self):
        if self.reader_handle and self.lib:
            self.lib.close_reader(self.reader_handle)
            self.reader_handle = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()


# ─────────────────── 3. Depth & trade handlers  ────────────────────────
class DepthBook:
    def __init__(self):
        self.bids = {}  # price → volume
        self.asks = {}

    def apply(self, msg: DepthItem):
        if msg.Flags & MarketFlag.Clear:
            self.bids.clear()
            self.asks.clear()
        side = self.bids if msg.Flags & MarketFlag.Buy else self.asks
        price  = msg.Price  / 1e8
        volume = msg.Volume / 1e8
        if msg.Volume > 0:
            side[price] = volume
        else:
            side.pop(price, None)

    # helpers for final report
    def best_bid(self):
        return max(self.bids) if self.bids else None
    def best_ask(self):
        return min(self.asks) if self.asks else None

class TradeLog(list):
    def push(self, msg: TickItem):
        self.append((msg.Header.Time, msg.Price / 1e8, msg.Volume / 1e8))

# ─────────────────── 4. Benchmark loop  ────────────────────────────────
def run_benchmark(path: str):
    print(f"Benchmarking {os.path.basename(path)}")
    start = time.perf_counter()

    depth  = DepthBook()
    trades = TradeLog()
    msgs   = 0
    building_snapshot      = True
    first_snapshot_reported = False

    with FastReader(path) as rdr:
        for raw in rdr:
            if raw is None:
                continue

            if isinstance(raw, DepthItem):
                depth.apply(raw)

            elif isinstance(raw, TickItem):
                trades.push(raw)
                if building_snapshot:
                    building_snapshot = False
                    bb, ba = depth.best_bid(), depth.best_ask()
                    print("\nFirst complete book:")
                    print(f"  best bid {bb:.2f}" if bb else "  best bid N/A")
                    print(f"  best ask {ba:.2f}" if ba else "  best ask N/A")
                    first_snapshot_reported = True

            msgs += 1

    elapsed = time.perf_counter() - start
    print(f"\nProcessed {msgs:,} messages in {elapsed:.3f}s  "
          f"({msgs/elapsed:,.0f} msg/s)")

    if building_snapshot:
        print("⚠  No trade message encountered – snapshot may be incomplete.")
    else:
        bb, ba = depth.best_bid(), depth.best_ask()
        print(f"Book levels → bids {len(depth.bids):,}, asks {len(depth.asks):,}")
        print(f"Best bid  : {bb:.2f}" if bb else "Best bid  : N/A")
        print(f"Best ask  : {ba:.2f}" if ba else "Best ask  : N/A")
    print(f"Trades captured: {len(trades):,}")
    if trades:
        print(f"Last trade     : {trades[-1]}")

# ─────────────────── 5. entry point  ───────────────────────────────────
if __name__ == "__main__":
    FILE = "/depths/20250618/ETHUSDT@BINANCEFUT_20250618.bin.lz4"
    if not os.path.exists(FILE):
        raise SystemExit(f"file not found: {FILE}")
    run_benchmark(FILE)

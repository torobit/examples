import ctypes
import os
import platform
import time
from enum import IntEnum


# =============================================================================
# 1.STRUCTURES
# =============================================================================

class MessageKind(IntEnum):
    Depth = 0
    Tick = 1
    Symbol = 2
    Candle = 3
    CandleEnd = 4


class OrderSide(IntEnum):
    Unknown = 0
    Buy = 1
    Sell = 2


class MarketFlag(IntEnum):
    Buy = 1
    Sell = 2
    Clear = 4
    EndOfTransaction = 8


class MessageHeader(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("Kind", ctypes.c_short),
        ("Size", ctypes.c_ushort),
        ("Time", ctypes.c_longlong),
    ]


class DepthItem(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("Header", MessageHeader),
        ("Price", ctypes.c_longlong),
        ("Volume", ctypes.c_longlong),
        ("Flags", ctypes.c_byte),
    ]


class TickItem(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("Header", MessageHeader),
        ("Id", ctypes.c_longlong),
        ("Price", ctypes.c_longlong),
        ("Volume", ctypes.c_longlong),
        ("Type", ctypes.c_byte),
    ]


# =============================================================================
# 2. NATIVE LIBRARY WRAPPER CLASS
# =============================================================================

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


# =============================================================================
# 3. DOMAIN LOGIC
# =============================================================================

class DepthSnapshot:
    def __init__(self):
        self.bids = {}
        self.asks = {}

    def update(self, msg: DepthItem):
        if msg.Flags & MarketFlag.Clear:
            self.bids.clear()
            self.asks.clear()
        items = self.bids if msg.Flags & MarketFlag.Buy else self.asks
        price = msg.Price / 10 ** 8
        volume = msg.Volume / 10 ** 8
        if msg.Volume > 0:
            items[price] = volume
        else:
            items.pop(price, None)

    def printstate(self):
        best_ask = min(self.asks) if self.asks else None
        best_bid = max(self.bids) if self.bids else None
        bid_str = f"{best_bid:.2f}" if best_bid is not None else "N/A"
        ask_str = f"{best_ask:.2f}" if best_ask is not None else "N/A"
        # MODIFICATION: Changed to print on a new line for final summary
        print(f"Bids: {len(self.bids):<6} Asks: {len(self.asks):<6} "
              f"Best Bid: {bid_str:<10} Best Ask: {ask_str:<10}")


class TradeProcessor:
    def __init__(self):
        self.trades = []

    def update(self, msg: TickItem):
        price = msg.Price / 10 ** 8
        volume = msg.Volume / 10 ** 8
        self.trades.append((msg.Header.Time, price, volume))

    def printstate(self):
        print(f"Trades count: {len(self.trades)}")
        if self.trades:
            print(f"Last trade: {self.trades[-1]}")


def process_messages(file_path):
    print(f"Starting benchmark for: {file_path}")
    start_time = time.perf_counter()
    count = 0

    depth = DepthSnapshot()
    trades = TradeProcessor()

    try:
        with FastReader(file_path) as reader:
            for msg in reader:
                if msg is None: continue

                if isinstance(msg, DepthItem):
                    depth.update(msg)
                elif isinstance(msg, TickItem):
                    trades.update(msg)

                count += 1
    except (IOError, FileNotFoundError) as e:
        print(f"\nError: {e}")
        return

    end_time = time.perf_counter()
    duration = end_time - start_time

    print("\n" + "=" * 50)
    print("BENCHMARK COMPLETE")
    print(f"Processed {count:,} messages in {duration:.4f} seconds.")
    if duration > 0:
        print(f"Messages per second: {count / duration:,.0f}")
    print("-" * 50)
    print("Final State:")
    depth.printstate()
    trades.printstate()
    print("=" * 50)


if __name__ == "__main__":
    file_path = '/depths/20250618/ETHUSDT@BINANCEFUT_20250618.bin.lz4'
    if not os.path.exists(file_path):
        print(f"Data file not found: {file_path}")
    else:
        process_messages(file_path)

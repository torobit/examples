#!/usr/bin/env python3
"""
FastStorage *.bin.lz4 benchmark – zero‑dependency pure Python.

Usage
-----
    python bench_faststorage.py  <file.bin.lz4>
"""

from __future__ import annotations
import ctypes, os, platform, sys, time
from enum import IntEnum, IntFlag
from pathlib import Path


# ────────────────────────── 1. on‑wire constants ──────────────────────────
class Kind(IntEnum):  Depth, Tick, Symbol, Candle, CandleEnd = range(5)
class Flag(IntFlag):  Buy = 1; Sell = 2; Clear = 4; EoTx = 8


# ───────────────────────── 2. minimal header view ─────────────────────────
class _Hdr(ctypes.Structure):
    _pack_ = 1
    _fields_ = [("kind", ctypes.c_int16),
                ("size", ctypes.c_uint16),
                ("time", ctypes.c_int64)]

# quick offsets (no full struct for every msg)
_PX, _VOL, _FLG, _TS = 12, 20, 28, 4
_SCALE = 1.0e-8


# ───────────────────────── 3. load Rust shared lib ────────────────────────
def _libname() -> str:
    return {"Darwin": "libfaststorage_native.dylib",
            "Windows": "faststorage_native.dll"}.get(platform.system(),
            "libfaststorage_native.so")

def _load() -> ctypes.CDLL:
    env = os.getenv("FASTSTORAGE_NATIVE_PATH")
    if env and Path(env).exists():
        return ctypes.CDLL(env)

    here = Path(__file__).resolve().parent / _libname()
    return ctypes.CDLL(here if here.exists() else _libname())

_lib = _load()
_lib.open_reader .argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
_lib.read_message.argtypes = [ctypes.c_void_p , ctypes.POINTER(ctypes.c_void_p)]
_lib.close_reader.argtypes = [ctypes.c_void_p]
_lib.open_reader .restype  = _lib.read_message.restype = ctypes.c_int
_lib.close_reader.restype  = None


# ───────────────────────── 4. zero‑alloc FastReader ───────────────────────
class FastReader:

    def __init__(self, path: str | os.PathLike):
        if not Path(path).is_file():
            raise FileNotFoundError(path)
        self._h = ctypes.c_void_p()
        rc = _lib.open_reader(os.fsencode(path), ctypes.byref(self._h))
        if rc or not self._h.value:
            raise OSError(f"open_reader failed ({rc}) for {path}")
        self._closed = False

    def __iter__(self): return self

    def __next__(self) -> int:
        ptr = ctypes.c_void_p()
        sz = _lib.read_message(self._h, ctypes.byref(ptr))
        if sz > 0:
            return ptr.value            # raw pointer address
        if sz == 0:
            raise StopIteration
        raise OSError(f"native reader error {sz}")

    # safe, idempotent close() -------------------------------------------
    def close(self):
        if not self._closed and self._h.value:
            _lib.close_reader(self._h)
            self._closed = True

    def __enter__(self): return self
    def __exit__(self, *_): self.close()
    def __del__(self):      self.close()


# ───────────────────────── 5. benchmark routine ───────────────────────────
def benchmark(path: str) -> None:
    print("Starting benchmark for:", path)
    t0 = time.perf_counter()

    bids, asks, trades = {}, {}, []
    cnt                       = 0
    building_snapshot         = True   # True from CLEAR until first Tick
    first_snapshot_printed    = False  # guard one‑off print

    hdr = _Hdr.from_address
    i64 = ctypes.c_int64.from_address
    u8  = ctypes.c_uint8.from_address

    with FastReader(path) as rdr:
        for addr in rdr:
            k = hdr(addr).kind

            if k == Kind.Depth:
                px  = i64(addr + _PX ).value * _SCALE
                vol = i64(addr + _VOL).value * _SCALE
                fl  = u8 (addr + _FLG).value

                if fl & Flag.Clear:
                    bids.clear(); asks.clear()
                    building_snapshot = True

                book = bids if fl & Flag.Buy else asks
                if vol > 0:
                    book[px] = vol
                else:
                    book.pop(px, None)

            elif k == Kind.Tick:
                trades.append(( i64(addr + _TS ).value,
                                i64(addr + _PX ).value * _SCALE,
                                i64(addr + _VOL).value * _SCALE ))

                if building_snapshot:
                    building_snapshot = False
                    if bids and asks and not first_snapshot_printed:
                        bb, ba = max(bids), min(asks)
                        print("\nFirst complete book ➜ "
                              f"best bid {bb:.2f}, best ask {ba:.2f}")
                        first_snapshot_printed = True

            cnt += 1

    # ---------- final summary ----------
    dt = time.perf_counter() - t0
    bb = max(bids) if bids else None
    ba = min(asks) if asks else None

    print("\n" + "=" * 62)
    print(f"Processed {cnt:,} msgs in {dt:.3f}s  ({cnt / dt:,.1f} msg/s)")
    print("-" * 62)
    print(f"Bids {len(bids):<6}  Asks {len(asks):<6}  "
          f"BestBid {bb or 'N/A':<10}  BestAsk {ba or 'N/A':<10}")
    print(f"Trades: {len(trades):,}")
    if trades:
        print("Last trade:", trades[-1])
    print("=" * 62)


# ───────────────────────── 6. CLI entry‑point ────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python bench_faststorage.py  <file.bin.lz4>")
        sys.exit(1)
    benchmark(sys.argv[1])

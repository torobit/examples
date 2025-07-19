# FastStorage Python Benchmarks

zero‑dependency Python scripts that benchmark **FastStorage** `*.bin.lz4` market‑data files:

| Script | Native library | Highlights |
|--------|---------------|------------|
| **`bench_faststorage.py`** | `faststorage‑rs` (Rust) | ultra‑lean, zero allocations, ~2× faster |
| **`bench_faststorage_cs.py`** | `FastStorage.Native` (C#) | richer error handling, parity with .NET writer |



## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| **Python**  | 3.8 +   | Standard library only (uses `ctypes`). |
| **Rust**    | 1.70 +  | Only if you want to compile `faststorage‑rs`. |
| **.NET SDK**| 7.0 +   | Only if you want to compile `FastStorage.Native`. |

---




import math

LCG_A = 1103515245
LCG_C = 12345
LCG_MASK = (1 << 31) - 1
LEVELS = 4096


def process_slice(seed: int, size: int, q_lo: float, q_hi: float) -> dict:
    n = size * size
    # крок 1: генерація зрізу і гістограма
    pixels = [0] * n
    hist = [0] * LEVELS
    x = seed
    for i in range(n):
        x = (LCG_A * x + LCG_C) & LCG_MASK
        p = x >> 19
        pixels[i] = p
        hist[p] += 1

    # крок 2: межі вікна за гістограмою
    need_lo = max(1, math.ceil(q_lo * n))
    need_hi = max(1, math.ceil(q_hi * n))
    p_lo = p_hi = -1
    cum = 0
    for v in range(LEVELS):
        cum += hist[v]
        if p_lo < 0 and cum >= need_lo:
            p_lo = v
        if cum >= need_hi:
            p_hi = v
            break
    d = p_hi - p_lo

    # крок 3: перетворення кожного пікселя
    removed = 0
    kept = 0
    s1 = 0
    s2 = 0
    for p in pixels:
        if p < p_lo or p > p_hi:
            removed += 1
            continue
        out = (2 * 4095 * (p - p_lo) + d) // (2 * d) if d > 0 else 0
        kept += 1
        s1 += out
        s2 += out * out
    if kept:
        mean = s1 / kept
        var = (s2 * kept - s1 * s1) / (kept * kept)
        std = math.sqrt(var) if var > 0 else 0.0
    else:
        mean = std = 0.0
    return {"p_lo": p_lo, "p_hi": p_hi, "removed": removed,
            "mean": round(mean, 6), "std": round(std, 6)}

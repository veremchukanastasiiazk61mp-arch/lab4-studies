"""Синтетичний зріз і крок обробки (ЛР4). Лише цикли Python, без NumPy і Numba.

Зріз S x S значень від 0 до 4095 генерується з seed лінійним конгруентним генератором:
    x(0) = seed, x(i+1) = (1103515245 * x(i) + 12345) mod 2^31, p(i) = x(i+1) >> 19.
Крок обробки: гістограма з 4096 рівнів, межі вікна p_lo і p_hi за квантилями q_lo і q_hi,
перетворення кожного пікселя: 0 поза вікном, інакше округлення до найближчого цілого від
4095 * (p - p_lo) / (p_hi - p_lo).

Уточнення, яких у завданні немає, зафіксовані тут, щоб результат був однаковим для всіх реалізацій:
  * квантиль: p_q найменший рівень v, для якого кількість пікселів із значенням <= v не менша за ceil(q * n),
    де n = S * S (для q_lo береться не менше одного пікселя);
  * округлення: половини округлюються вгору, обчислення цілочисельне: (2*4095*(p-lo) + d) // (2*d), d = p_hi - p_lo;
  * якщо p_hi = p_lo, усі пікселі всередині вікна дають 0;
  * середнє і стандартне відхилення (генеральне) беруться за значеннями p' пікселів у вікні [p_lo, p_hi];
    коли таких немає, обидва дорівнюють 0;
  * вилученими вважаються пікселі поза вікном.
"""
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

"""Розрахункові функції ЛР2 без зовнішніх залежностей: універсальний закон масштабованості
і оцінка квантиля за гістограмою. Формули — з розділу «Теоретичні відомості» завдання."""
import math


def usl(n, lam, sigma, kappa):
    """X(N) = λN / (1 + σ(N−1) + κN(N−1))."""
    return lam * n / (1 + sigma * (n - 1) + kappa * n * (n - 1))


def fit_usl(x1, points):
    """Оцінка σ і κ методом найменших квадратів без вільного члена.

    x1      — X(1), пропускна здатність одного екземпляра (λ = X(1));
    points  — список (N, X(N)) для N > 1, що входять у підбір.

    N/C(N) − 1 = σ(N−1) + κN(N−1),  C(N) = X(N)/X(1).
    Відʼємну оцінку прирівнюють до нуля, а другу рахують за однопараметричною формулою.
    """
    pts = [(n, x) for n, x in points if n > 1]
    if len(pts) < 2:
        raise ValueError("для підбору двох параметрів потрібно щонайменше дві точки з N > 1")
    u = [n - 1 for n, _ in pts]
    v = [n * (n - 1) for n, _ in pts]
    y = [n / (x / x1) - 1 for n, x in pts]
    suu = sum(a * a for a in u)
    svv = sum(a * a for a in v)
    suv = sum(a * b for a, b in zip(u, v))
    suy = sum(a * b for a, b in zip(u, y))
    svy = sum(a * b for a, b in zip(v, y))
    det = suu * svv - suv * suv
    if det == 0:
        raise ValueError("вироджена система: потрібні різні значення N")
    sigma = (svv * suy - suv * svy) / det
    kappa = (suu * svy - suv * suy) / det
    method = "двопараметрова"
    note = ""
    if kappa < 0:  # якщо відʼємні обидві, спершу обнуляється κ
        kappa = 0.0
        sigma = suy / suu
        method = "κ відʼємна → κ = 0, σ = Σuy/Σu²"
    elif sigma < 0:
        sigma = 0.0
        kappa = svy / svv
        method = "σ відʼємна → σ = 0, κ = Σvy/Σv²"
    if sigma < 0 or kappa < 0:  # однопараметрова оцінка теж відʼємна: масштабування не гірше за лінійне
        sigma, kappa = max(sigma, 0.0), max(kappa, 0.0)
        note = "однопараметрова оцінка теж відʼємна, прирівняна до нуля"
    n_star = x_star = None
    limit = None
    if kappa > 0 and sigma < 1:
        n_star = math.sqrt((1 - sigma) / kappa)
        x_star = usl(n_star, x1, sigma, kappa)
    elif kappa == 0 and sigma > 0:
        limit = x1 / sigma  # X(N) → λ/σ при N → ∞
    pred = {n: usl(n, x1, sigma, kappa) for n, _ in pts}
    return {
        "lambda": x1, "sigma": sigma, "kappa": kappa, "method": method, "note": note,
        "n_points": len(pts), "N_star": n_star, "X_at_N_star": x_star, "X_limit": limit,
        "points": [{"N": n, "X": x, "C": x / x1, "y": n / (x / x1) - 1, "X_pred": pred[n],
                    "rel_err": (pred[n] - x) / x} for n, x in pts],
    }


def quantile_from_buckets(bounds, cum, total, q):
    """Оцінка квантиля рівня q за гістограмою з лінійною інтерполяцією всередині бакета.

    bounds — верхні межі скінченних бакетів b(1)<…<b(k) (за зростанням);
    cum    — накопичені кількості c(1)≤…≤c(k) для цих меж;
    total  — загальна кількість спостережень n (бакет +Inf).

    Повертає словник: estimate, lower, upper (межі бакета; upper=None, якщо ранг за останньою
    скінченною межею), max_error (ширина бакета; None, якщо не обмежена зверху), beyond.
    """
    if total <= 0:
        return None
    r = q * total
    prev_b, prev_c = 0.0, 0.0
    for b, c in zip(bounds, cum):
        if prev_c < r <= c:
            est = prev_b + (b - prev_b) * (r - prev_c) / (c - prev_c)
            return {"estimate": est, "lower": prev_b, "upper": b, "max_error": b - prev_b, "beyond": False}
        prev_b, prev_c = b, c
    return {"estimate": bounds[-1], "lower": bounds[-1], "upper": None, "max_error": None, "beyond": True}

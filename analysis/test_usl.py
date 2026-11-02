"""Перевірка розрахункових функцій на відомих відповідях. Запуск: python -m pytest analysis  (або python analysis/test_usl.py)."""
import math
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from usl import fit_usl, quantile_from_buckets, usl  # noqa: E402


def test_fit_recovers_known_parameters():
    lam, sigma, kappa = 600.0, 0.08, 0.004
    pts = [(n, usl(n, lam, sigma, kappa)) for n in (2, 3, 4)]
    r = fit_usl(lam, pts)
    assert math.isclose(r["sigma"], sigma, rel_tol=1e-9)
    assert math.isclose(r["kappa"], kappa, rel_tol=1e-9)
    assert math.isclose(r["N_star"], math.sqrt((1 - sigma) / kappa), rel_tol=1e-9)


def test_two_points_are_enough():
    lam, sigma, kappa = 500.0, 0.1, 0.01
    r = fit_usl(lam, [(2, usl(2, lam, sigma, kappa)), (4, usl(4, lam, sigma, kappa))])
    assert math.isclose(r["sigma"], sigma, rel_tol=1e-9) and math.isclose(r["kappa"], kappa, rel_tol=1e-9)


def test_one_point_is_rejected():
    try:
        fit_usl(500.0, [(2, 900.0)])
    except ValueError:
        return
    raise AssertionError("очікувалась помилка")


def test_negative_kappa_goes_to_zero_and_sigma_refit():
    lam = 500.0
    pts = [(2, 2 * lam / (1 + 0.1)), (3, 3 * lam / (1 + 0.2 - 0.0)), (4, 4 * lam / (1 + 0.3 - 0.01))]  # зростає швидше за σ-модель
    r = fit_usl(lam, pts)
    assert r["kappa"] >= 0 and r["sigma"] >= 0
    assert r["kappa"] == 0 or r["sigma"] == 0


def test_ideal_scaling_gives_zero():
    lam = 400.0
    r = fit_usl(lam, [(2, 800.0), (3, 1200.0), (4, 1600.0)])
    assert abs(r["sigma"]) < 1e-12 and abs(r["kappa"]) < 1e-12


def test_quantile_interpolation_inside_bucket():
    # межі 10, 20, 30; накопичено 50, 90, 100; n=100; p99: r=99 → бакет (20,30], c(i−1)=90, c(i)=100
    q = quantile_from_buckets([10, 20, 30], [50, 90, 100], 100, 0.99)
    assert math.isclose(q["estimate"], 20 + 10 * (99 - 90) / (100 - 90))
    assert (q["lower"], q["upper"], q["max_error"], q["beyond"]) == (20, 30, 10, False)


def test_quantile_first_bucket_starts_at_zero():
    q = quantile_from_buckets([5, 10], [100, 100], 100, 0.5)
    assert math.isclose(q["estimate"], 2.5) and q["lower"] == 0 and q["max_error"] == 5


def test_quantile_beyond_last_finite_bound():
    q = quantile_from_buckets([10, 20], [50, 90], 100, 0.99)  # 10 спостережень за межею 20
    assert q["beyond"] and q["estimate"] == 20 and q["upper"] is None and q["max_error"] is None


def test_quantile_empty():
    assert quantile_from_buckets([10], [0], 0, 0.99) is None


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
    print("усі перевірки пройдено")

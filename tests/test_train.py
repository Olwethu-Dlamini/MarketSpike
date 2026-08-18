import math

import numpy as np
import pytest

from marketspike.ml.evaluate import pinball_loss
from marketspike.ml.features import Sample
from marketspike.ml.train import fit_quantiles, predict
from marketspike.risk.slippage import FEATURE_ORDER


def _synthetic_samples(n, seed=42):
    """Samples with deliberately mismatched feature scales (spec Task 20):
    one feature swings +-300, another +-100, another +-0.05 -- similar in
    spirit to log_spread_bps (~-6.5) vs quote_rate_hz (~100) in the real
    feature set.
    """
    rng = np.random.default_rng(seed)
    p = len(FEATURE_ORDER)
    scales = np.array([1.0, 2.0, 0.5, 1.0, 100.0, 1.0, 300.0, 1.0, 0.05])
    matrix = rng.normal(size=(n, p)) * scales
    true_w = rng.normal(size=p) * 0.01
    true_b = 2.0
    noise = rng.standard_t(df=4, size=n) * 0.3
    target = np.maximum(0.0, true_b + matrix.dot(true_w) + np.abs(noise))

    samples = []
    for i in range(n):
        features = {name: float(matrix[i, j]) for j, name in enumerate(FEATURE_ORDER)}
        samples.append(
            Sample(
                t_ns=i, symbol="TEST", features=features, delta_ms=60.0,
                direction=1, cost_bps=float(target[i]), regime="NORMAL",
            )
        )
    return samples


def test_gradient_descent_pinball_loss_within_two_percent_of_sklearn_lp_solver():
    sklearn = pytest.importorskip("sklearn", reason="sklearn not installed")
    from sklearn.linear_model import QuantileRegressor

    samples = _synthetic_samples(2000)
    samples.sort(key=lambda s: s.t_ns)
    cut = int(len(samples) * 0.7)
    train, test = samples[:cut], samples[cut:]

    fitted_gd = fit_quantiles(train)
    predictions_gd = predict(fitted_gd, test)

    train_matrix = np.array(
        [[s.features[name] for name in FEATURE_ORDER] for s in train], dtype=float
    )
    train_target = np.array([s.cost_bps for s in train], dtype=float)
    test_matrix = np.array(
        [[s.features[name] for name in FEATURE_ORDER] for s in test], dtype=float
    )
    test_target = [s.cost_bps for s in test]

    for label, tau in (("p50", 0.5), ("p95", 0.95)):
        sk_model = QuantileRegressor(quantile=tau, alpha=1e-4, solver="highs")
        sk_model.fit(train_matrix, train_target)
        predictions_sklearn = [
            max(0.0, value) for value in sk_model.predict(test_matrix)
        ]

        loss_gd = pinball_loss(test_target, predictions_gd[label], tau)
        loss_sklearn = pinball_loss(test_target, predictions_sklearn, tau)

        assert loss_gd <= loss_sklearn * 1.02, (
            "{0}: gradient-descent pinball loss {1:.6f} is more than 2% worse "
            "than sklearn's LP solver {2:.6f}".format(label, loss_gd, loss_sklearn)
        )


def _known_linear_samples(n, seed):
    """Samples generated from a *known* linear relationship, with two
    features on deliberately mismatched scales and non-zero means -- one
    tiny (mean ~1e-3), one large (mean ~1e2) -- mirroring the real
    log_spread_bps (~-6.5) vs quote_rate_hz (~100) mismatch (spec Task 20,
    review finding 1).

    Every other feature is pure zero-mean noise with zero true coefficient,
    so the only way `fit_quantiles` can recover `true_intercept`/`true_coef`
    on the two informative features is if its raw-space un-standardisation
    is correct in *both* of the ways it can be wrong:

    - a dropped `/std_safe` on the coefficients is caught because the
      informative features' stds (~2e-4 and ~25) are far from 1, so the
      standardized-space weight and the correct raw-space coefficient
      differ by orders of magnitude;
    - a dropped `- np.dot(w_std, mean/std_safe)` intercept term is caught
      only because these two features have a substantial non-zero mean
      (unlike a feature merely centered near 0 by sampling noise) -- the
      correction term is ~0.8 and ~1.0 respectively, not negligible.
    """
    rng = np.random.default_rng(seed)
    p = len(FEATURE_ORDER)
    small_idx = FEATURE_ORDER.index("log_spread_bps")
    large_idx = FEATURE_ORDER.index("quote_rate_hz")

    matrix = rng.normal(size=(n, p))  # other features: mean 0, std 1
    matrix[:, small_idx] = 1e-3 + rng.normal(scale=2e-4, size=n)
    matrix[:, large_idx] = 100.0 + rng.normal(scale=25.0, size=n)

    true_intercept = 5.0
    true_coef = np.zeros(p)
    true_coef[small_idx] = 800.0
    true_coef[large_idx] = 0.01

    # Symmetric, mean/median-zero noise: the pinball-optimal p50 fit under
    # this noise is exactly true_intercept + matrix @ true_coef, so we can
    # assert recovery directly rather than just self-consistency.
    noise = rng.normal(scale=0.05, size=n)
    target = true_intercept + matrix.dot(true_coef) + noise
    assert (target > 0).all(), "test fixture must avoid the max(0, ...) clamp"

    samples = []
    for i in range(n):
        features = {name: float(matrix[i, j]) for j, name in enumerate(FEATURE_ORDER)}
        samples.append(
            Sample(
                t_ns=i, symbol="TEST", features=features, delta_ms=60.0,
                direction=1, cost_bps=float(target[i]), regime="NORMAL",
            )
        )
    return samples, true_intercept, true_coef


def test_unstandardisation_round_trips_to_raw_feature_space():
    """Exercise the real `fit_quantiles` un-standardisation path and check
    that the *returned raw-space* coefficients recover the known linear
    relationship the data was generated from (spec Task 20, review finding
    1) -- not a re-derivation of the same formula computed inline, which
    would hold by construction even if `fit_quantiles`'s un-standardisation
    were wrong.

    Verified to actually catch a broken un-standardisation: temporarily
    dropping the `- np.dot(w_std, mean/std_safe)` intercept term, and
    separately dropping the `/std_safe` division on the coefficients, each
    make this test fail (see task-20 report for the exact failures).
    """
    samples, true_intercept, true_coef = _known_linear_samples(4000, seed=7)
    small_idx = FEATURE_ORDER.index("log_spread_bps")
    large_idx = FEATURE_ORDER.index("quote_rate_hz")

    fitted = fit_quantiles(samples)

    # p50 (the median) is the quantile the pinball-GD solver converges on
    # most precisely in a fixed iteration budget -- the loss surface for
    # tau=0.95 is lopsided (see `_fit_pinball_gd`'s docstring) and converges
    # to the true coefficients less tightly, so recovery is checked here at
    # tau=0.5, where a correct un-standardisation should reproduce the
    # generating coefficients closely.
    coef_raw = fitted["p50"]["coefficients"]
    intercept_raw = fitted["p50"]["intercept"]

    assert coef_raw[small_idx] == pytest.approx(true_coef[small_idx], rel=0.05)
    assert coef_raw[large_idx] == pytest.approx(true_coef[large_idx], rel=0.05)
    for idx, name in enumerate(FEATURE_ORDER):
        if idx in (small_idx, large_idx):
            continue
        assert coef_raw[idx] == pytest.approx(0.0, abs=0.05)
    assert intercept_raw == pytest.approx(true_intercept, abs=0.5)


def test_fit_quantiles_return_shape_is_unchanged():
    samples = _synthetic_samples(300, seed=1)
    fitted = fit_quantiles(samples)

    assert set(fitted.keys()) == {"p50", "p95"}
    for label in ("p50", "p95"):
        assert set(fitted[label].keys()) == {"intercept", "coefficients"}
        assert isinstance(fitted[label]["intercept"], float)
        coefficients = fitted[label]["coefficients"]
        assert isinstance(coefficients, list)
        assert len(coefficients) == len(FEATURE_ORDER)
        assert all(isinstance(c, float) for c in coefficients)


def test_zero_variance_feature_yields_finite_coefficients_and_zero_weight():
    """A constant (zero-variance) feature column drives std == 0, which the
    `std_safe = np.where(std == 0.0, 1.0, std)` guard exists to protect
    against (spec Task 20, review finding 2) -- without it, standardising
    that column would divide by zero. Assert the guard actually keeps every
    returned value finite, and that the constant feature contributes
    nothing (coefficient 0.0), since it carries no information to fit on.
    """
    rng = np.random.default_rng(11)
    n = 500
    p = len(FEATURE_ORDER)
    const_idx = FEATURE_ORDER.index("log_v_ratio")

    matrix = rng.normal(size=(n, p))
    matrix[:, const_idx] = 3.0  # zero-variance column

    true_coef = rng.normal(size=p) * 0.1
    true_coef[const_idx] = 0.0
    noise = rng.normal(scale=0.05, size=n)
    target = np.maximum(0.0, 2.0 + matrix.dot(true_coef) + noise)

    samples = []
    for i in range(n):
        features = {name: float(matrix[i, j]) for j, name in enumerate(FEATURE_ORDER)}
        samples.append(
            Sample(
                t_ns=i, symbol="TEST", features=features, delta_ms=60.0,
                direction=1, cost_bps=float(target[i]), regime="NORMAL",
            )
        )

    fitted = fit_quantiles(samples)

    for label in ("p50", "p95"):
        intercept = fitted[label]["intercept"]
        coefficients = fitted[label]["coefficients"]
        assert math.isfinite(intercept)
        assert all(math.isfinite(c) for c in coefficients)
        assert coefficients[const_idx] == 0.0

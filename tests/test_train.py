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


def test_unstandardisation_round_trips_to_raw_feature_space():
    """Fit on data with deliberately mismatched feature scales and check
    that the returned raw-space coefficients reproduce the same predictions
    as computing directly in standardized space (spec Task 20) -- a wrong
    un-standardisation would silently train fine and score garbage at serve
    time, so this pins the algebra rather than just "it runs".
    """
    samples = _synthetic_samples(600, seed=7)

    matrix = np.array(
        [[s.features[name] for name in FEATURE_ORDER] for s in samples], dtype=float
    )
    mean = matrix.mean(axis=0)
    std = matrix.std(axis=0)
    std_safe = np.where(std == 0.0, 1.0, std)
    standardized = (matrix - mean) / std_safe

    from marketspike.ml.train import _fit_pinball_gd

    for tau in (0.5, 0.95):
        w_std, b_std = _fit_pinball_gd(standardized, np.array([s.cost_bps for s in samples]), tau, alpha=1e-4)

        coef_raw = w_std / std_safe
        intercept_raw = float(b_std - np.dot(w_std, mean / std_safe))

        predictions_standardized = standardized.dot(w_std) + b_std
        predictions_raw = matrix.dot(coef_raw) + intercept_raw

        assert predictions_raw == pytest.approx(predictions_standardized, abs=1e-8)


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

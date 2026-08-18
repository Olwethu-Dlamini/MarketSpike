import pytest

from marketspike.ml.evaluate import coverage, pinball_loss


def test_pinball_loss_is_zero_for_a_perfect_forecast():
    assert pinball_loss([1.0, 2.0, 3.0], [1.0, 2.0, 3.0], tau=0.5) == 0.0


def test_pinball_loss_penalises_under_prediction_more_at_high_tau():
    under = pinball_loss([10.0], [8.0], tau=0.95)
    over = pinball_loss([10.0], [12.0], tau=0.95)
    assert under > over


def test_pinball_loss_is_symmetric_at_the_median():
    assert pinball_loss([10.0], [8.0], tau=0.5) == pytest.approx(
        pinball_loss([10.0], [12.0], tau=0.5)
    )


def test_coverage_counts_exceedances():
    actuals = [1.0, 2.0, 3.0, 4.0]
    predictions = [5.0, 5.0, 5.0, 5.0]
    assert coverage(actuals, predictions) == 0.0
    assert coverage(actuals, [0.0, 0.0, 5.0, 5.0]) == pytest.approx(0.5)


def test_coverage_of_empty_series_is_zero():
    assert coverage([], []) == 0.0

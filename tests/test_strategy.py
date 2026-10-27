import pytest

import os
os.environ.setdefault("BOTKLINE_AUTOSTART", "false")

import app as st  # noqa: E402  (la estrategia vive en app.py)


def test_body_change_pct():
    assert st.body_change_pct(100, 103) == pytest.approx(3.0)
    assert st.body_change_pct(100, 97) == pytest.approx(-3.0)
    assert st.body_change_pct(0, 1) == 0.0


@pytest.mark.parametrize("o,c,expected", [
    (100, 103, st.LONG),       # justo el 3 % cuenta
    (100, 110, st.LONG),
    (100, 97, st.SHORT),
    (100, 102.99, None),
    (100, 97.01, None),
])
def test_signal_follows_trend(o, c, expected):
    assert st.signal_direction(o, c, 3.0) == expected


def test_take_profit_long_and_short():
    assert st.take_profit_price(100, st.LONG, 2) == pytest.approx(102)
    assert st.take_profit_price(100, st.SHORT, 2) == pytest.approx(98)
    assert not st.take_profit_hit(101.99, 100, st.LONG, 2)
    assert st.take_profit_hit(102, 100, st.LONG, 2)
    assert st.take_profit_hit(105, 100, st.LONG, 2)
    assert not st.take_profit_hit(98.01, 100, st.SHORT, 2)
    assert st.take_profit_hit(98, 100, st.SHORT, 2)
    assert not st.take_profit_hit(0, 100, st.SHORT, 2)


def test_favorable_move_and_pnl():
    assert st.favorable_move_pct(102, 100, st.LONG) == pytest.approx(2)
    assert st.favorable_move_pct(102, 100, st.SHORT) == pytest.approx(-2)
    assert st.pnl_usd(98, 100, 0.05, st.SHORT) == pytest.approx(0.1)
    assert st.pnl_usd(98, 100, 0.05, st.LONG) == pytest.approx(-0.1)


def test_size_order_rounds_up_to_reach_min_notional():
    qty, eff, why = st.size_order(5, 0.3333, step=1, min_notional=5)
    assert why == "" and qty == 16 and eff >= 5


def test_size_order_skips_when_pair_minimum_is_higher():
    qty, _, why = st.size_order(5, 100, step=0.001, min_notional=20)
    assert qty == 0 and "mínimo" in why


def test_size_order_skips_coarse_step_on_expensive_pair():
    qty, _, why = st.size_order(5, 60000, step=0.001, min_notional=5, max_over=1.5)
    assert qty == 0 and "stepSize" in why


def test_size_order_decimal_step():
    qty, eff, why = st.size_order(5, 2.1234, step=0.1, min_notional=5)
    assert why == "" and qty == pytest.approx(2.4) and 5 <= eff < 5.2


def test_inverted_direction_and_stop_loss():
    from app import entry_direction, exit_price, exit_hit
    assert entry_direction(1.0, 1.04, 3) == "LONG"
    assert entry_direction(1.0, 1.04, 3, inverted=True) == "SHORT"
    assert entry_direction(1.0, 0.96, 3, inverted=True) == "LONG"
    assert entry_direction(1.0, 1.01, 3, inverted=True) is None
    sl = exit_price(100.0, "SHORT", 2, inverted=True)       # SHORT invertido: SL arriba
    assert abs(sl - 102.0) < 1e-9
    assert not exit_hit(101.9, sl, "SHORT", inverted=True)
    assert exit_hit(102.0, sl, "SHORT", inverted=True)
    sl = exit_price(100.0, "LONG", 2, inverted=True)        # LONG invertido: SL abajo
    assert abs(sl - 98.0) < 1e-9 and exit_hit(97.9, sl, "LONG", True) and not exit_hit(105, sl, "LONG", True)
    tp = exit_price(100.0, "LONG", 2)                       # normal: TP
    assert exit_hit(102.0, tp, "LONG") and not exit_hit(101.0, tp, "LONG")


def test_net_pnl_counts_entry_and_exit_fees():
    from app import net_pnl_usd, fees_usd
    # 5 USDT a 1.0 → 5 de qty; sale a 1.02 → bruto 0.10; fees 0.05 % de 5 + 5.1
    assert abs(fees_usd(1.0, 1.02, 5, 0.05) - 0.00505) < 1e-12
    assert abs(net_pnl_usd(1.02, 1.0, 5, "LONG", 0.05) - (0.10 - 0.00505)) < 1e-12
    assert abs(net_pnl_usd(1.02, 1.0, 5, "SHORT", 0.05) - (-0.10 - 0.00505)) < 1e-12


def test_tp_sl_prices_defaults_ratio_bot():
    # TP +10 % / SL -5 % (valores por defecto del bot de ratio)
    tp, sl = st.tp_sl_prices(100, st.SHORT, 10, 5)
    assert tp == pytest.approx(90) and sl == pytest.approx(105)
    tp, sl = st.tp_sl_prices(100, st.LONG, 10, 5)
    assert tp == pytest.approx(110) and sl == pytest.approx(95)
    assert st.tp_sl_prices(100, st.LONG, 0, 0) == (0.0, 0.0)     # 0 = apagado


@pytest.mark.parametrize("price,direction,expected", [
    (89.9, st.SHORT, "TP"), (95, st.SHORT, None), (105, st.SHORT, "SL"),
    (110.5, st.LONG, "TP"), (100, st.LONG, None), (95, st.LONG, "SL"),
])
def test_price_exit(price, direction, expected):
    tp, sl = st.tp_sl_prices(100, direction, 10, 5)
    assert st.price_exit(price, direction, tp, sl) == expected


def test_price_exit_disabled_legs():
    assert st.price_exit(200, st.LONG, 0, 0) is None
    assert st.price_exit(50, st.LONG, 110, 0) is None
    assert st.price_exit(50, st.LONG, 0, 95) == "SL"

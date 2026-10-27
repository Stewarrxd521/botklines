import random
import threading
import time
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


# ── Bots de ratio en paralelo y trade de venganza ────────────────────────────

def _bot_sin_red(tmp_path, monkeypatch):
    """Instancia limpia del bot, sin WebSockets ni REST: solo la lógica."""
    monkeypatch.setattr(st, "SETTINGS_FILE", str(tmp_path / "s.json"))
    bot = st.KlineBot()
    bot.store.can_open = lambda: True
    bot.store.can_act = lambda: True
    bot.store.mark_dirty = lambda *a, **k: None
    bot.store.append_trade = lambda *a, **k: None
    bot.executor.send = lambda *a, **k: None
    bot.filters = {"AUSDT": {"stepSize": 1, "minQty": 0, "minNotional": 5}}
    bot.fee_pct = 0.0
    return bot


def test_los_tres_bots_de_ratio_abren_con_la_misma_senal(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    now = time.time()
    bot._tick = {"AUSDT": (25.0, 1.0, now)}
    bot._price = lambda sym: 1.0
    ts = int(now * 1000)
    bot._on_ratio_points("AUSDT", [(ts - 300_000, 1.18), (ts, 0.86)])
    abiertas = {n: list(bot.engines[n].positions.values()) for n in st.RATIO_ENGINES}
    assert all(len(ps) == 1 and ps[0].direction == st.SHORT for ps in abiertas.values())
    # El bot "solo TP ratio" no lleva TP/SL de precio; los otros dos sí.
    assert abiertas[st.ENGINE_RATIO_TP][0].tp_price == 0 and abiertas[st.ENGINE_RATIO_TP][0].sl_price == 0
    for name in (st.ENGINE_RATIO, st.ENGINE_RATIO_REV):
        assert abiertas[name][0].tp_price > 0 and abiertas[name][0].sl_price > 0


def test_la_venganza_recupera_la_perdida_y_no_se_encadena(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot.rev_cfg.update(revenge=True, rev_mult=10, rev_recover_pct=65, rev_sl_pct=100, rev_dir="opposite")
    eng = bot.engines[st.ENGINE_RATIO_REV]
    perdedora = st.Position(trade_id=1, symbol="AUSDT", direction=st.SHORT, entry_price=1.0, qty=5,
                            notional=5.0, tp_price=0.9, sl_price=1.05, signal_pct=-27, candle_open=1.18,
                            candle_close=0.86, candle_time=0, last_price=1.0, strategy=st.ENGINE_RATIO_REV)
    eng.positions[1] = perdedora
    bot._price = lambda sym: 1.05
    bot._close(1, 1.05, "SL")                       # pierde 0,25 USDT (sin fees)
    venganzas = [p for p in eng.positions.values() if p.revenge]
    assert len(venganzas) == 1
    v = venganzas[0]
    assert v.direction == st.LONG                   # contraria a la perdedora
    assert v.rev_parent == 1
    assert v.notional == pytest.approx(5.0 * 10, rel=0.25)     # ×10, redondeado al stepSize
    assert v.rev_tp_usd == pytest.approx(0.25 * 0.65)
    assert v.rev_sl_usd == pytest.approx(0.25 * 10)
    # El TP de la venganza deja el beneficio pedido, y su SL la pérdida tope.
    assert st.pnl_usd(v.tp_price, v.entry_price, v.qty, v.direction) == pytest.approx(v.rev_tp_usd, rel=0.02)
    assert st.pnl_usd(v.sl_price, v.entry_price, v.qty, v.direction) == pytest.approx(-v.rev_sl_usd, rel=0.02)
    # Si la venganza pierde, NO se abre otra.
    bot._close(v.trade_id, v.sl_price, "SL")
    assert not eng.positions


def test_la_venganza_no_se_dispara_al_cerrar_todas(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    eng = bot.engines[st.ENGINE_RATIO_REV]
    eng.positions[2] = st.Position(trade_id=2, symbol="AUSDT", direction=st.SHORT, entry_price=1.0, qty=5,
                                   notional=5.0, tp_price=0.9, sl_price=1.05, signal_pct=-27, candle_open=1.18,
                                   candle_close=0.86, candle_time=0, last_price=1.0, strategy=st.ENGINE_RATIO_REV)
    bot._price = lambda sym: 1.05
    bot.close_all(eng)
    assert not eng.positions


def test_una_perdedora_simulada_no_dispara_venganza_real(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot.executor_paused = False                     # el executor se reanudó después de abrirla
    eng = bot.engines[st.ENGINE_RATIO_REV]
    eng.positions[3] = st.Position(trade_id=3, symbol="AUSDT", direction=st.SHORT, entry_price=1.0, qty=5,
                                   notional=5.0, tp_price=0.9, sl_price=1.05, signal_pct=-27, candle_open=1.18,
                                   candle_close=0.86, candle_time=0, last_price=1.0, strategy=st.ENGINE_RATIO_REV,
                                   sent=False)
    bot._price = lambda sym: 1.05
    bot._close(3, 1.05, "SL")
    venganzas = [p for p in eng.positions.values() if p.revenge]
    assert len(venganzas) == 1 and venganzas[0].sent is False


def test_cerrar_un_bot_no_cierra_en_el_executor_lo_de_los_otros(tmp_path, monkeypatch):
    # El executor suma en una posición las de todos los bots del mismo par y dirección.
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot.executor_paused = False
    enviados = []
    bot.executor.send = lambda payload, *a, **k: enviados.append(payload)
    now = time.time()
    bot._tick = {"AUSDT": (25.0, 1.0, now)}
    bot._price = lambda sym: 1.0
    ts = int(now * 1000)
    bot._on_ratio_points("AUSDT", [(ts - 300_000, 1.18), (ts, 0.86)])
    ps = [p for n in st.RATIO_ENGINES for p in bot.engines[n].positions.values()]
    assert len(ps) == 3 and all(p.sent for p in ps)
    enviados.clear()
    for p in ps:
        bot._close(p.trade_id, 1.0, "MANUAL")
    cierres = [e for e in enviados if e["action"] == "close"]
    assert [c["trade_id"] for c in cierres] == [p.trade_id for p in ps]
    assert [c.get("quantity") for c in cierres[:2]] == [ps[0].qty, ps[1].qty]   # parciales
    assert "quantity" not in cierres[2]             # la última cierra todo lo que haya en Binance


def test_solo_se_consulta_el_ratio_de_las_abiertas_que_esperan_tp_por_ratio(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot.watch = ["AUSDT"]
    bot.ratio_cfg["ratio_exit"] = False

    def pos(tid, sym, name, **kw):
        bot.engines[name].positions[tid] = st.Position(
            trade_id=tid, symbol=sym, direction=st.SHORT, entry_price=1.0, qty=5, notional=5.0, tp_price=0,
            signal_pct=-27, candle_open=1.18, candle_close=0.86, candle_time=0, last_price=1.0, strategy=name,
            ratio_trigger=1.18, **kw)

    pos(1, "BUSDT", st.ENGINE_RATIO_TP)             # espera el TP por ratio
    pos(2, "CUSDT", st.ENGINE_RATIO_REV, revenge=True)  # venganza: sale por precio
    pos(3, "DUSDT", st.ENGINE_RATIO)                # salida por ratio apagada en ese bot
    assert bot._ratio_symbols() == ["AUSDT", "BUSDT"]


@pytest.mark.parametrize("value, expected", [
    (True, True), (False, False), (1, True), (0, False), (None, False),
    ("true", True), ("false", False), ("0", False), ("off", False), ("", False),
])
def test_flag(value, expected):
    assert st._flag(value) is expected


def test_las_senales_de_un_par_llegan_al_executor_en_orden():
    # Varios hilos de envío: un cierre total no puede adelantar al parcial del mismo par.
    bridge = st.ExecutorBridge("http://executor.test", "s", logger=lambda m: None)
    bridge._request = lambda *a, **k: (200, b"")
    got, lock, done, n = [], threading.Lock(), threading.Event(), 60

    def deliver(conns, payload, target, t0):
        time.sleep(random.random() * 0.004)
        with lock:
            got.append((payload["symbol"], payload["i"]))
            if len(got) == n:
                done.set()
        return True

    bridge._deliver = deliver
    for i in range(n):
        bridge.send({"action": "close", "symbol": f"S{i % 3}USDT", "i": i})
    assert done.wait(10)
    for sym in {s for s, _ in got}:
        seq = [i for s, i in got if s == sym]
        assert seq == sorted(seq)

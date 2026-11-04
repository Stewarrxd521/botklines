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
    bot.min_hold_s = 0.0                       # las pruebas de la demora la ponen a mano

    def sin_red(*a, **k):
        raise RuntimeError("sin red en las pruebas")
    bot._rest_get = sin_red

    async def sin_ws(sym):
        raise RuntimeError("sin red en las pruebas")
    bot._ws_depth = sin_ws
    bot._alt_http = sin_red
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
def test_aggtrade_da_el_precio_al_instante_y_el_mark_no_lo_pisa():
    from WS import SymbolWebSocketPriceCache
    pc = SymbolWebSocketPriceCache([], symbol_stream="aggTrade")
    assert pc._streams_for("FOOUSDT") == ["foousdt@aggTrade", "foousdt@markPrice@1s"]
    now = time.time()
    pc._dispatch_event({"e": "aggTrade", "s": "FOOUSDT", "p": "1.2345"}, now)
    pc._dispatch_event({"e": "markPriceUpdate", "s": "FOOUSDT", "p": "1.2000"}, now + 0.5)
    assert pc.price_cache["FOOUSDT"][0] == pytest.approx(1.2345)
    assert pc.price_cache["FOOUSDT"][1] == pytest.approx(now + 0.5)   # el mark solo refresca la hora


def test_la_exploracion_24h_se_corta_cuando_ya_llego_el_mercado(tmp_path, monkeypatch):
    import asyncio
    import json
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot.filters = {}
    syms = [f"S{i}USDT" for i in range(300)]
    msgs = [syms[:250], syms[:290], syms[:291], syms]           # el 3.º ya casi no trae nuevos
    sent = []

    async def fake_ws(url):
        for chunk in msgs:
            sent.append(chunk)
            yield json.dumps([{"s": s, "c": "1", "P": "5"} for s in chunk])

    monkeypatch.setattr(st, "_ws_messages", fake_ws)
    seen = asyncio.run(bot._ticker_ws_snapshot(5, quick=True))
    assert len(sent) == 3 and len(seen) == 291
    sent.clear()
    seen = asyncio.run(bot._ticker_ws_snapshot(5, quick=False))   # la diaria lo lee todo
    assert len(sent) == 4 and len(seen) == 300


def test_vigilados_con_precio_lento_y_posiciones_con_precio_rapido():
    from WS import SymbolWebSocketPriceCache
    pc = SymbolWebSocketPriceCache(["AUSDT"], symbol_stream="aggTrade")
    pc.set_slow_symbols(["AUSDT", "BUSDT"])
    assert pc._desired_streams() == {"ausdt@aggTrade", "ausdt@markPrice@1s", "busdt@markPrice"}


def test_poner_a_cero_el_realizado_de_un_bot_no_mueve_el_objetivo(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot.engines[st.ENGINE_RATIO].total_realized_pnl = 3.0
    bot.engines[st.ENGINE_RATIO_REV].total_realized_pnl = -7.0
    antes = bot.session_pnl()[0]
    r = bot.reset_realized(bot.engines[st.ENGINE_RATIO_REV])
    assert r["was"] == -7.0 and bot.engines[st.ENGINE_RATIO_REV].total_realized_pnl == 0
    assert bot.engines[st.ENGINE_RATIO].total_realized_pnl == 3.0
    assert bot.session_pnl()[0] == pytest.approx(antes)


def _bot_con_envios(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot.executor_paused = False
    enviados = []
    bot.executor.send = lambda payload, *a, **k: enviados.append(payload)
    now = time.time()
    bot._tick = {"AUSDT": (25.0, 1.0, now)}
    bot._price = lambda sym: 1.0
    return bot, enviados, int(now * 1000)


def test_el_tp_va_en_la_apertura_y_se_mueve_en_caliente(tmp_path, monkeypatch):
    bot, enviados, ts = _bot_con_envios(tmp_path, monkeypatch)
    for n in (st.ENGINE_RATIO_TP, st.ENGINE_RATIO_REV):
        bot.engines[n].paused = True                # solo abre el bot ratio TP/SL
    bot._on_ratio_points("AUSDT", [(ts - 300_000, 1.18), (ts, 0.86)])
    (pos,) = bot.engines[st.ENGINE_RATIO].positions.values()
    assert [e["action"] for e in enviados] == ["open"]
    assert enviados[0]["tp"] == pytest.approx(pos.tp_price) and pos.ex_tp == pos.tp_price
    # Cambiar el TP en caliente lo mueve también en Binance (solo el de ese trade_id).
    enviados.clear()
    bot.set_ratio_config(tp_pct=8)
    assert enviados[0]["action"] == "open_tp" and enviados[0]["trigger_price"] == pytest.approx(pos.tp_price)
    assert enviados[0]["trade_id"] == pos.trade_id


def test_con_varios_bots_en_el_mismo_par_cada_uno_lleva_su_tp(tmp_path, monkeypatch):
    bot, enviados, ts = _bot_con_envios(tmp_path, monkeypatch)
    bot._on_ratio_points("AUSDT", [(ts - 300_000, 1.18), (ts, 0.86)])
    assert [e["action"] for e in enviados] == ["open", "open", "open"]
    por_bot = {e["strategy"]: e for e in enviados}
    assert "tp" in por_bot[st.ENGINE_RATIO] and "tp" in por_bot[st.ENGINE_RATIO_REV]
    assert "tp" not in por_bot[st.ENGINE_RATIO_TP]          # el bot "solo TP ratio" no tiene TP por precio


def test_cambiar_el_tp_de_un_bot_de_velas_lo_mueve_en_binance(tmp_path, monkeypatch):
    bot, enviados, _ = _bot_con_envios(tmp_path, monkeypatch)
    eng = bot.engines[st.ENGINE_CLOSE]
    eng.positions[9] = st.Position(trade_id=9, symbol="AUSDT", direction=st.LONG, entry_price=1.0, qty=5,
                                   notional=5.0, tp_price=1.02, signal_pct=3, candle_open=0.97, candle_close=1.0,
                                   candle_time=0, last_price=1.0, strategy=st.ENGINE_CLOSE, ex_tp=1.02)
    bot.set_config(eng, tp_pct=3)
    assert enviados[-1]["action"] == "open_tp" and enviados[-1]["trigger_price"] == pytest.approx(1.03)


def test_ahorro_de_datos_desde_la_web(tmp_path, monkeypatch):
    from WS import SymbolWebSocketPriceCache
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot.price_cache = pc = SymbolWebSocketPriceCache(["AUSDT"], symbol_stream="aggTrade")
    bot.watch = ["BUSDT"]
    bot.set_global(watch_live_price=True, fast_prices=True)
    bot._sync_watch_prices()
    assert pc._desired_streams() == {"ausdt@aggTrade", "ausdt@markPrice@1s", "busdt@markPrice"}
    g = bot.set_global(watch_live_price=False, fast_prices=False)
    assert g["watch_live_price"] is False and g["fast_prices"] is False
    assert pc._desired_streams() == {"ausdt@markPrice@1s"}


# ── Precio de libro (como el executor) ────────────────────────────────────────

def test_recorrer_el_libro_da_el_precio_medio_y_el_peor_mas_alla_de_20_niveles():
    asks = [["1.0", "2"], ["1.1", "3"]]
    assert st.walk_book(asks, 4) == pytest.approx((2 * 1.0 + 2 * 1.1) / 4)
    assert st.walk_book(asks, 10) == pytest.approx((2 * 1.0 + 3 * 1.1 + 5 * 1.1) / 10)   # resto al peor
    assert st.walk_book(asks, 1) == pytest.approx(1.0)
    assert st.walk_book([], 1) is None and st.walk_book([["0", "5"]], 1) is None


def _libro(bot, bids, asks):
    """El libro llega por el WebSocket depth20 (sin REST)."""
    pedidos = []

    async def ws(sym):
        pedidos.append(sym)
        if bids is None:
            raise RuntimeError("sin libro")
        return bids, asks
    bot._ws_depth = ws
    return pedidos


def test_las_aperturas_de_ratio_entran_al_precio_del_libro(tmp_path, monkeypatch):
    bot, enviados, ts = _bot_con_envios(tmp_path, monkeypatch)
    pedidos = _libro(bot, bids=[["0.99", "3"], ["0.98", "10"]], asks=[["1.01", "10"]])
    bot._on_ratio_points("AUSDT", [(ts - 300_000, 1.18), (ts, 0.86)])
    abiertas = [p for n in st.RATIO_ENGINES for p in bot.engines[n].positions.values()]
    assert len(abiertas) == 3 and pedidos == ["AUSDT"]                       # un libro para los 3 bots
    esperado = (3 * 0.99 + 2 * 0.98) / 5                                    # SHORT vende contra los bids
    for p in abiertas:
        assert p.entry_price == pytest.approx(esperado) and p.entry_src == "libro" and p.signal_price == 1.0
    pos = next(iter(bot.engines[st.ENGINE_RATIO].positions.values()))
    assert pos.tp_price == pytest.approx(esperado * 0.9)                    # TP desde la entrada real
    assert enviados[0]["price"] == 1.0 and enviados[0]["tp"] == pytest.approx(pos.tp_price)


def test_el_cierre_se_valora_con_el_libro_y_la_orden_sale_antes(tmp_path, monkeypatch):
    bot, enviados, _ = _bot_con_envios(tmp_path, monkeypatch)
    eng = bot.engines[st.ENGINE_RATIO]
    eng.positions[7] = st.Position(trade_id=7, symbol="AUSDT", direction=st.SHORT, entry_price=1.0, qty=5,
                                   notional=5.0, tp_price=0.9, sl_price=1.05, signal_pct=-27, candle_open=1.18,
                                   candle_close=0.86, candle_time=0, last_price=1.0, strategy=st.ENGINE_RATIO)
    _libro(bot, bids=[["0.89", "10"]], asks=[["0.90", "2"], ["0.92", "10"]])
    assert bot._close(7, 0.9, "TP")
    assert enviados[0]["action"] == "close" and enviados[0]["close_price"] == 0.9
    rec = eng.closed_trades[0]
    esperado = (2 * 0.90 + 3 * 0.92) / 5                                    # cerrar SHORT = comprar asks
    assert rec["close_price"] == pytest.approx(esperado) and rec["close_src"] == "libro"
    assert rec["trigger_price"] == 0.9 and rec["pnl"] == pytest.approx((1.0 - esperado) * 5)


def test_sin_libro_usa_el_precio_en_vivo_y_sin_eso_el_de_la_senal(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    rest = []
    bot._rest_get = lambda *a, **k: rest.append(a)            # el libro NUNCA va por REST
    _libro(bot, bids=None, asks=None)
    bot._fetch_book("AUSDT")
    assert not rest and bot.book_errors == 1
    bot._price = lambda sym: 1.02
    assert bot._fill_from_book("AUSDT", True, 5, 1.0) == (1.02, "precio en vivo")
    bot._price = lambda sym: None
    assert bot._fill_from_book("AUSDT", True, 5, 1.0) == (1.0, "señal")


# ── Demora mínima antes de cerrar ─────────────────────────────────────────────

def test_no_cierra_por_precio_antes_de_la_demora_minima(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot.set_global(min_hold_s=2)
    eng = bot.engines[st.ENGINE_RATIO_REV]
    eng.positions[8] = st.Position(trade_id=8, symbol="AUSDT", direction=st.LONG, entry_price=1.0, qty=50,
                                   notional=50.0, tp_price=1.001, sl_price=0.99, signal_pct=0, candle_open=1,
                                   candle_close=1, candle_time=0, last_price=1.0, strategy=st.ENGINE_RATIO_REV,
                                   revenge=True)
    bot._price = lambda sym: 1.002                  # el TP se toca en el mismo instante de abrir
    bot._check_take_profit("AUSDT")
    assert 8 in eng.positions
    eng.positions[8].opened_ts -= 2.1               # ya cumplió los 2 s
    bot._check_take_profit("AUSDT")
    assert 8 not in eng.positions and eng.closed_trades[0]["duration_s"] >= 2
    assert bot.global_settings()["min_hold_s"] == 2


# ── RSI + VWAP ────────────────────────────────────────────────────────────────

def test_velas_a_pedir_por_rest():
    day = 1_762_128_000_000                          # 00:00 UTC
    assert st.klines_needed(day + 45 * 60_000, 14) == 141       # 00:45 → 140 de calentamiento del RSI + la en curso
    assert st.klines_needed(day + 12 * 3600_000, 14) == 721     # 12:00 → las 720 de hoy + la en curso
    assert st.klines_needed(day + 1439 * 60_000 + 30_000, 14) == 1440


def test_vwap_y_sigma_del_dia_utc():
    ind = st.CandleIndicators(14)
    day = 1_762_128_000_000
    ind.add(day - 60_000, 50, 50, 50, 100)          # vela de ayer: no entra en el VWAP de hoy
    ind.add(day, 10, 10, 10, 1)
    ind.add(day + 60_000, 20, 20, 20, 3)
    v, sd = ind.vwap()
    assert v == pytest.approx(17.5) and sd == pytest.approx(18.75 ** 0.5)
    lo, mid, hi = ind.bands(2.5)
    assert lo == pytest.approx(17.5 - 2.5 * sd) and hi == pytest.approx(17.5 + 2.5 * sd)
    assert ind.day_candles == 2


def test_rsi_de_wilder_progresivo_igual_que_de_una_vez():
    rnd = random.Random(7)
    closes, px = [], 100.0
    for _ in range(300):
        px *= 1 + rnd.uniform(-0.01, 0.01)
        closes.append(px)
    a, b = st.CandleIndicators(14), st.CandleIndicators(14)
    for i, c in enumerate(closes):
        a.add(i * 60_000 + 60_000, c, c, c, 1)
    for i, c in enumerate(closes[:200]):            # "REST"
        b.add(i * 60_000 + 60_000, c, c, c, 1)
    for i, c in enumerate(closes[200:], start=200):  # "WebSocket"
        b.add(i * 60_000 + 60_000, c, c, c, 1)
    assert a.rsi == pytest.approx(b.rsi) and 0 < a.rsi < 100
    # Referencia: Wilder clásico (media simple de los 14 primeros y suavizado después).
    ch = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    ag = sum(max(x, 0) for x in ch[:14]) / 14
    al = sum(max(-x, 0) for x in ch[:14]) / 14
    for x in ch[14:]:
        ag, al = (ag * 13 + max(x, 0)) / 14, (al * 13 + max(-x, 0)) / 14
    assert a.rsi == pytest.approx(100 - 100 / (1 + ag / al))
    sube = st.CandleIndicators(3)
    for i in range(6):
        sube.add(i * 60_000 + 60_000, 1 + i, 1 + i, 1 + i, 1)
    assert sube.rsi == 100.0


def _abierta_ratio(bot, name, tid, direction=st.SHORT, revenge=False):
    p = st.Position(trade_id=tid, symbol="AUSDT", direction=direction, entry_price=1.0, qty=5, notional=5.0,
                    tp_price=0, sl_price=0, signal_pct=-27, candle_open=1.18, candle_close=0.86, candle_time=0,
                    last_price=1.0, strategy=name, revenge=revenge)
    bot.engines[name].positions[tid] = p
    return p


def test_tp_por_rsi_y_vwap_cierra_solo_en_los_bots_encendidos(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    _abierta_ratio(bot, st.ENGINE_RATIO_TP, 11)
    _abierta_ratio(bot, st.ENGINE_RATIO, 12)                       # este bot lo tiene apagado
    _abierta_ratio(bot, st.ENGINE_RATIO_REV, 13, revenge=True)     # las venganzas no lo usan
    bot.engines[st.ENGINE_RATIO_REV].positions[13].strategy = st.ENGINE_RATIO_REV
    bot.set_ind_config(enabled={st.ENGINE_RATIO_TP: True, st.ENGINE_RATIO_REV: True})
    assert bot._ind_buf == {"AUSDT": []}                           # pidiendo las velas por REST
    now_ms = int(time.time() // 60 * 60) * 1000                     # minuto en curso
    rows, px = [], 1.0
    for i in range(150, 0, -1):                                    # 150 velas cerradas, suaves
        t = now_ms - i * 60_000
        px *= 1.0005 if i % 2 else 0.9996
        rows.append([t, px, px * 1.001, px * 0.999, px, 100.0, t + 59_999])
    rows.append([now_ms, px, px, px, px, 1.0, now_ms + 59_999])   # la vela en curso: se descarta
    bot._ind_seeded("AUSDT", rows)
    ind = bot._ind["AUSDT"]
    assert ind.count == 150 and ind.ready()
    # Desplome en la vela que cierra después de abrir: RSI < 13 y cierre bajo VWAP − 2,5σ.
    bot._ind_candle("AUSDT", now_ms, px, px * 0.80, px * 0.80, 5000.0)
    assert ind.rsi < 13
    assert 11 not in bot.engines[st.ENGINE_RATIO_TP].positions
    assert bot.engines[st.ENGINE_RATIO_TP].closed_trades[0]["reason"] == "TP_RSI_VWAP"
    assert 12 in bot.engines[st.ENGINE_RATIO].positions and 13 in bot.engines[st.ENGINE_RATIO_REV].positions


def test_un_hueco_de_velas_recarga_por_rest(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    _abierta_ratio(bot, st.ENGINE_RATIO_TP, 21)
    bot.set_ind_config(enabled={st.ENGINE_RATIO_TP: True})
    bot._ind_seeded("AUSDT", [[i * 60_000, 1, 1, 1, 1, 1, i * 60_000 + 59_999] for i in range(1, 40)])
    bot._ind_candle("AUSDT", 45 * 60_000, 1, 1, 1, 1)               # faltan las de 40..44
    assert "AUSDT" not in bot._ind and bot._ind_buf["AUSDT"] == [(45 * 60_000, 1, 1, 1, 1)]


def test_config_del_tp_por_indicadores(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    r = bot.set_ind_config(rsi_period=21, rsi_level=15, vwap_k=2, enabled={st.ENGINE_RATIO: True})
    assert r["cfg"]["rsi_period"] == 21 and r["cfg"]["enabled"][st.ENGINE_RATIO] is True
    with pytest.raises(ValueError):
        bot.set_ind_config(rsi_level=60)
    with pytest.raises(ValueError):
        bot.set_ind_config(use_rsi=False, use_vwap=False)
    nuevo = st.KlineBot()                                            # se guarda en los ajustes
    assert nuevo.ind_cfg["rsi_period"] == 21 and nuevo.ind_cfg["vwap_k"] == 2


def test_ws_velas_1m_de_los_pares_con_tp_por_indicadores():
    from WS import SymbolWebSocketPriceCache
    pc = SymbolWebSocketPriceCache(["AUSDT"], symbol_stream="aggTrade")
    got = []
    pc.set_kline_symbols(["AUSDT"], lambda *a: got.append(a))
    assert "ausdt@kline_1m" in pc._desired_streams()
    k = {"t": 60_000, "o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10", "x": False}
    pc._dispatch_event({"e": "kline", "s": "AUSDT", "k": k}, time.time())
    assert got == []                                                 # la vela en curso no cuenta
    pc._dispatch_event({"e": "kline", "s": "AUSDT", "k": {**k, "x": True}}, time.time())
    assert got == [("AUSDT", 60_000, 1.0, 2.0, 0.5, 1.5, 10.0)]


def test_en_el_loop_el_libro_se_pide_en_segundo_plano_sin_bloquear(tmp_path, monkeypatch):
    import asyncio
    bot, enviados, _ = _bot_con_envios(tmp_path, monkeypatch)
    eng = bot.engines[st.ENGINE_RATIO]
    eng.positions[31] = st.Position(trade_id=31, symbol="AUSDT", direction=st.LONG, entry_price=1.0, qty=5,
                                    notional=5.0, tp_price=1.1, sl_price=0.95, signal_pct=27, candle_open=1,
                                    candle_close=1, candle_time=0, last_price=1.0, strategy=st.ENGINE_RATIO)

    async def ws(sym):
        await asyncio.sleep(0.05)
        return [["1.09", "100"]], [["1.11", "100"]]
    bot._ws_depth = ws

    async def run():
        bot.loop = asyncio.get_running_loop()
        bot._close(31, 1.1, "TP")
        assert enviados[-1]["action"] == "close" and not eng.closed_trades    # la orden ya salió; el papel, después
        for _ in range(100):
            if eng.closed_trades:
                break
            await asyncio.sleep(0.01)
    asyncio.run(run())
    assert eng.closed_trades[0]["close_price"] == pytest.approx(1.09)          # LONG cierra vendiendo a bids


def test_al_encender_se_cargan_las_velas_una_vez_por_rest(tmp_path, monkeypatch):
    import asyncio
    bot = _bot_sin_red(tmp_path, monkeypatch)
    _abierta_ratio(bot, st.ENGINE_RATIO, 41)
    pedidos = []

    def rest(path, params=None, timeout=15.0):
        pedidos.append((path, params))
        assert params["limit"] <= 499                        # tramos de peso 2
        ts = range(params["startTime"], params["endTime"] + 1, 60_000)
        return [[t, 1, 1.01, 0.99, 1 + t / 6e12, 10, t + 59_999] for t in ts]
    bot._rest_get = rest
    monkeypatch.setattr(st, "IND_EXCHANGES", ["binance"])

    async def run():
        bot.loop, bot.running = asyncio.get_running_loop(), True
        bot.set_ind_config(enabled={st.ENGINE_RATIO: True})
        for _ in range(100):
            if "AUSDT" in bot._ind:
                break
            await asyncio.sleep(0.01)
        bot.running = False
    asyncio.run(run())
    n = st.klines_needed(int(time.time() * 1000), 14)
    assert len(pedidos) == -(-n // 499) and all(p[0] == "/fapi/v1/klines" for p in pedidos)
    assert bot._ind["AUSDT"].count == n - 1 and not bot._ind_seeding        # la vela en curso se descarta


# ── Bloqueos 418/429 y velas de Bybit ─────────────────────────────────────────

def test_un_418_bloquea_solo_esa_ruta_y_no_vuelve_a_pedir(tmp_path, monkeypatch):
    import urllib.error
    from email.message import Message
    monkeypatch.setattr(st, "SETTINGS_FILE", str(tmp_path / "s.json"))
    monkeypatch.setattr(st, "REST_PROXIES", ["http://proxy:1"])
    bot = st.KlineBot()
    llamadas = []

    def fake(url, proxy="", timeout=15.0, **k):
        llamadas.append(proxy)
        if proxy == "":
            h = Message()
            h["Retry-After"] = "163000"
            raise urllib.error.HTTPError(url, 418, "banned", h, None)
        return {"ok": True}, 10
    monkeypatch.setattr(st, "http_get_json", fake)
    assert bot._rest_get("/fapi/v1/depth") == {"ok": True}  # sigue por el proxy
    assert llamadas == ["", "http://proxy:1"] and bot._rest_block_until == 0
    llamadas.clear()
    bot._rest_get("/fapi/v1/depth")
    assert llamadas == ["http://proxy:1"]                   # la vetada ya no se toca

    def todo_418(url, proxy="", timeout=15.0, **k):
        llamadas.append(proxy)
        h = Message()
        h["Retry-After"] = "120"
        raise urllib.error.HTTPError(url, 418, "banned", h, None)
    monkeypatch.setattr(st, "http_get_json", todo_418)
    with pytest.raises(RuntimeError):
        bot._rest_get("/fapi/v1/depth")
    assert bot._rest_block_until > time.time() + 100
    llamadas.clear()
    with pytest.raises(RuntimeError, match="en pausa"):
        bot._rest_get("/fapi/v1/klines")
    assert llamadas == []                                   # con todo vetado no sale nada


def test_con_la_ip_vetada_rota_entre_las_proxies(tmp_path, monkeypatch):
    monkeypatch.setattr(st, "SETTINGS_FILE", str(tmp_path / "s.json"))
    monkeypatch.setattr(st, "REST_PROXIES", ["http://a:1@p:80", "http://b:2@p:80", "http://c:3@p:80"])
    bot = st.KlineBot()
    usadas = []
    monkeypatch.setattr(st, "http_get_json", lambda url, proxy="", timeout=15.0, **k: (usadas.append(proxy), ({}, 1))[1])
    bot._rest_get("/futures/data/x")
    assert usadas == [""]                                   # sin veto: IP directa
    bot._route_block[""] = time.time() + 3600
    usadas.clear()
    for _ in range(6):
        bot._rest_get("/futures/data/x")
    assert usadas == st.REST_PROXIES * 2                    # vetada: proxies en rueda
    assert "clave" not in st.proxy_label(st.REST_PROXIES[0]) and ":1@" not in st.proxy_label(st.REST_PROXIES[0])


def test_las_proxies_por_defecto_son_solo_para_binance():
    assert len(st.REST_PROXIES) >= 5 and all(u.startswith("http://fixie:") for u in st.REST_PROXIES[:5])


def _respuestas(ex, now_ms, start, end):
    ts = [t for t in range(start, end + 1, 60_000) if t <= now_ms]
    if ex == "bybit":
        return {"retCode": 0, "result": {"list": [[str(t), "1", "1.01", "0.99", "1", "10", "10"] for t in reversed(ts)]}}
    if ex == "kucoin":
        return {"code": "200000", "data": [[t, 1, 1.01, 0.99, 1, 10] for t in ts]}
    return [[t // 1000, 0.99, 1.01, 1, 1, 10] for t in reversed(ts)]


def test_velas_de_otros_exchanges_en_orden_y_binance_al_final(tmp_path, monkeypatch):
    from urllib.parse import urlsplit, parse_qs
    from datetime import datetime
    bot = _bot_sin_red(tmp_path, monkeypatch)
    rest_binance = []
    bot._rest_get = lambda *a, **k: rest_binance.append(a) or []
    pedidos = []
    tiene = {"bybit": False, "kucoin": False, "coinbase": True, "mexc": False}

    def http(url, timeout=10.0):
        u = urlsplit(url)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        ex = next(k for k, b in st.ALT_URLS.items() if url.startswith(b))
        pedidos.append(ex)
        if not tiene[ex]:
            raise LookupError("HTTP 404")
        if ex == "coinbase":
            assert u.path == "/products/A-USD/candles" and q["granularity"] == "60"
            iso = lambda x: int(datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=st.timezone.utc).timestamp() * 1000)
            start, end = iso(q["start"]), iso(q["end"])
            assert (end - start) // 60_000 + 1 <= 300
        return _respuestas(ex, int(time.time() * 1000), start, end)
    bot._alt_http = http
    rows, ex = bot._fetch_seed_klines("AUSDT")
    n = st.klines_needed(int(time.time() * 1000), 14)
    assert ex == "coinbase" and not rest_binance             # Binance no se toca si otro tiene el par
    assert len(rows) == n and rows[0][0] < rows[-1][0]
    assert pedidos[:2] == ["bybit", "kucoin"]                  # Bybit y KuCoin no lo tienen (un intento cada uno)
    assert pedidos.count("coinbase") == 1 + -(-n // 290)       # prueba del nombre + páginas de 290
    # Ninguno lo tiene: Binance (REST) como último recurso.
    tiene["coinbase"] = False
    bot._alt_missing.clear(); bot._alt_sym.clear()
    pedidos.clear()
    bot._rest_get = lambda *a, **k: rest_binance.append(a) or [[0, 1, 1, 1, 1, 1, 59_999]]
    rows, ex = bot._fetch_seed_klines("AUSDT")
    assert pedidos.index("mexc") > pedidos.index("coinbase")  # MEXC justo antes de Binance
    assert ex == "binance" and len(rest_binance) == -(-n // 499)


def test_mexc_futuros_normaliza_columnas_y_pagina(tmp_path, monkeypatch):
    from urllib.parse import urlsplit, parse_qs
    bot = _bot_sin_red(tmp_path, monkeypatch)
    pedidos = []

    def http(url, timeout=10.0):
        u = urlsplit(url)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        pedidos.append(u.path)
        if u.path.endswith("/1000PEPE_USDT"):
            return {"success": False, "code": 1001, "message": "contract not exists"}
        assert u.path == "/api/v1/contract/kline/PEPE_USDT" and q["interval"] == "Min1"
        now_s = int(time.time())
        ts = [t for t in range(int(q["start"]), int(q["end"]) + 1, 60) if t <= now_s]
        return {"success": True, "code": 0, "data": {"time": ts, "open": [1] * len(ts), "high": [1.01] * len(ts),
                                                     "low": [0.99] * len(ts), "close": [1] * len(ts),
                                                     "vol": [10] * len(ts)}}
    bot._alt_http = http
    rows = bot._alt_klines("mexc", "1000PEPEUSDT", 1441)
    assert len(rows) == 1441 and rows[0][0] < rows[-1][0]
    assert all(r[6] == r[0] + 59_999 and r[2] == 1.01 and r[3] == 0.99 and r[5] == 10 for r in rows)
    assert len(pedidos) == 2 + 2                                 # 2 nombres probados + 2 páginas de 1000


def test_con_mucho_peso_usado_la_ruta_descansa_hasta_el_minuto_siguiente(tmp_path, monkeypatch):
    monkeypatch.setattr(st, "SETTINGS_FILE", str(tmp_path / "s.json"))
    monkeypatch.setattr(st, "REST_PROXIES", ["http://proxy:1"])
    bot = st.KlineBot()
    usadas = []

    def fake(url, proxy="", timeout=15.0, headers_out=None):
        usadas.append(proxy)
        headers_out["x-mbx-used-weight-1m"] = "2000" if proxy == "" else "10"
        return [], 1
    monkeypatch.setattr(st, "http_get_json", fake)
    bot._rest_get("/futures/data/x")
    assert 0 < bot._route_block[""] - time.time() <= 62 and bot._route_weight[""] == 2000
    bot._rest_get("/futures/data/x")
    assert usadas == ["", "http://proxy:1"]                     # sin esperar al 429: sigue por el proxy
    assert bot.rest_routes_snapshot()[1]["weight"] == 10


def test_bybit_y_kucoin_paginan_y_normalizan(tmp_path, monkeypatch):
    from urllib.parse import urlsplit, parse_qs
    bot = _bot_sin_red(tmp_path, monkeypatch)
    pedidos = []

    def http(url, timeout=10.0):
        q = {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}
        ex = next(k for k, b in st.ALT_URLS.items() if url.startswith(b))
        pedidos.append((ex, q))
        if ex == "bybit":
            return _respuestas(ex, int(time.time() * 1000), int(q["start"]), int(q["end"]))
        assert q["symbol"] == "XBTUSDTM"
        return _respuestas(ex, int(time.time() * 1000), int(q["from"]), int(q["to"]))
    bot._alt_http = http
    rows = bot._alt_klines("bybit", "AUSDT", 1441)
    assert [p[0] for p in pedidos] == ["bybit"] * 3 and len(rows) == 1441     # prueba + 2 páginas
    assert all(r[6] == r[0] + 59_999 and r[2] == 1.01 and r[3] == 0.99 for r in rows)
    pedidos.clear()
    rows = bot._alt_klines("kucoin", "BTCUSDT", 1200)
    assert len(pedidos) == 1 + 3 and len(rows) == 1200


def test_velas_de_otro_exchange_no_se_mezclan_y_rellenan_minutos_sin_operaciones(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    _abierta_ratio(bot, st.ENGINE_RATIO_TP, 51)
    bot.set_ind_config(enabled={st.ENGINE_RATIO_TP: True})
    rows = [[i * 60_000, 1, 1, 1, 1, 1, i * 60_000 + 59_999] for i in range(1, 40)]
    bot._ind_seeded("AUSDT", rows, "coinbase")
    ind = bot._ind["AUSDT"]
    bot._ind_candle("AUSDT", 40 * 60_000, 1, 1, 1, 1)                  # del WS de Binance: se ignora
    assert ind.last_open == 39 * 60_000
    bot._ind_candle("AUSDT", 42 * 60_000, 1.2, 1.1, 1.15, 3, "coinbase")  # sin velas en 40 y 41
    assert ind.last_open == 42 * 60_000 and ind.count == 42 and "AUSDT" in bot._ind
    assert bot.ind_snapshot()["symbols"]["AUSDT"]["src"] == "coinbase"


def test_el_libro_se_toma_del_websocket_depth20(tmp_path, monkeypatch):
    import asyncio
    monkeypatch.setattr(st, "SETTINGS_FILE", str(tmp_path / "s.json"))
    bot = st.KlineBot()
    urls = []

    async def fake_ws(url):
        urls.append(url)
        if "/public/" in url:
            raise OSError("404")
        yield '{"e":"depthUpdate","s":"AUSDT","b":[["0.99","3"]],"a":[["1.01","4"]]}'
    monkeypatch.setattr(st, "_ws_messages", fake_ws)
    bids, asks = asyncio.run(bot._ws_depth("AUSDT"))
    assert bids == [["0.99", "3"]] and asks == [["1.01", "4"]]
    assert urls == ["wss://fstream.binance.com/public/ws/ausdt@depth20@100ms",
                    "wss://fstream.binance.com/market/ws/ausdt@depth20@100ms"]


def test_tp_por_precio_se_confirma_con_el_libro_antes_de_cerrar(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    enviados = []
    bot.executor.send = lambda payload, *a, **k: enviados.append(payload)
    p = _abierta_ratio(bot, st.ENGINE_RATIO_REV, 61)          # SHORT desde 1.0, TP al 0.5 %
    p.tp_price = 0.995
    eng = bot.engines[st.ENGINE_RATIO_REV]
    bot._price = lambda sym: 0.9948                            # el mechazo toca el TP...
    _libro(bot, [["0.997", "100"]], [["0.9974", "100"]])       # ...pero los asks están en +0.26 %
    bot._check_take_profit("AUSDT")
    assert 61 in eng.positions and not enviados and not eng.closed_trades   # rechazado: sigue abierta
    assert p.tp_rejects == 1 and not bot._tp_checking
    bot._books.clear()
    _libro(bot, [["0.9945", "100"]], [["0.9949", "100"]])      # ahora sí hay asks al TP o mejor
    bot._check_take_profit("AUSDT")
    assert 61 not in eng.positions and enviados[-1]["action"] == "close"
    assert enviados[-1]["close_price"] == pytest.approx(0.9949)
    assert eng.closed_trades[0]["close_price"] == pytest.approx(0.9949) and eng.closed_trades[0]["reason"] == "TP"

import asyncio
import os
import time

import pytest

os.environ.setdefault("BOTKLINE_AUTOSTART", "false")

import app as st  # noqa: E402

from test_strategy import _bot_sin_red, _libro  # noqa: E402


def test_lado_que_cobra_el_funding():
    assert st.funding_side(0.35) == st.SHORT          # positivo: los LONG pagan a los SHORT
    assert st.funding_side(-0.4) == st.LONG
    assert st.funding_side(0.0) is None
    assert st.funding_income_usd(20, 0.3, st.SHORT) == pytest.approx(0.06)
    assert st.funding_income_usd(20, -0.3, st.LONG) == pytest.approx(0.06)
    assert st.funding_income_usd(20, 0.3, st.LONG) == pytest.approx(-0.06)


def test_proximo_cobro_en_hora_en_punto():
    assert st.next_funding_ts(3600 * 10 + 1800) == 3600 * 11
    assert st.next_funding_ts(3600 * 11 - 3, lead_s=2) == 3600 * 11
    assert st.next_funding_ts(3600 * 11 - 1, lead_s=2) == 3600 * 12    # ya no da tiempo a entrar


def test_config_por_defecto_y_validacion(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    c = bot.funding_cfg
    assert (c["min_pct"], c["notional"], c["open_before_s"], c["close_after_s"]) == (0.3, 20, 2, 1)
    assert bot.set_funding_config(min_pct=-0.5, notional=50)["min_pct"] == 0.5
    with pytest.raises(ValueError):
        bot.set_funding_config(notional=1)
    with pytest.raises(ValueError):
        bot.set_funding_config(open_before_s=40, scan_before_s=30)
    with pytest.raises(ValueError):                         # menos que FUNDING_MIN_LEAD_S: nunca entraría
        bot.set_funding_config(open_before_s=st.FUNDING_MIN_LEAD_S)
    again = st.KlineBot()                                   # se guarda en disco
    assert again.funding_cfg["notional"] == 50 and again.funding_cfg["min_pct"] == 0.5


def _filas(ts):
    return [{"symbol": "AUSDT", "rate_pct": 0.45, "next_ts": ts, "mark": 1.0},
            {"symbol": "BUSDT", "rate_pct": -0.31, "next_ts": ts, "mark": 2.0},
            {"symbol": "CUSDT", "rate_pct": 0.29, "next_ts": ts, "mark": 1.0},        # bajo el umbral
            {"symbol": "DUSDT", "rate_pct": 0.9, "next_ts": ts + 3600 * 3, "mark": 1.0}]  # cobra en otra hora


def test_abre_en_el_lado_que_cobra_y_cierra_tras_el_cobro(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot.filters["BUSDT"] = {"stepSize": 1, "minQty": 0, "minNotional": 5}
    bot.filters["DUSDT"] = bot.filters["CUSDT"] = bot.filters["BUSDT"]
    enviados = []
    bot.executor.send = lambda payload, *a, **k: enviados.append((time.time(), payload))
    bot._price = lambda sym: {"AUSDT": 1.0, "BUSDT": 2.0}.get(sym)
    pedidos = _libro(bot, [["0.99", "1000"]], [["1.01", "1000"]])
    bot.funding_cfg.update(open_before_s=0.6, close_after_s=0.15)
    eng = bot.engines[st.ENGINE_FUNDING]

    async def run():
        bot.loop = asyncio.get_running_loop()
        ts = time.time() + 0.9

        async def foto():
            return [dict(r) for r in _filas(ts)]
        bot._funding_ws_snapshot = foto
        await bot._funding_cycle(ts)
        await asyncio.sleep(0.6)                                    # abiertas antes del cobro
        opened = {p.symbol: p for p in eng.positions.values()}
        await asyncio.sleep(0.8)                                    # cerradas tras el cobro
        return ts, opened

    ts, opened = asyncio.run(run())
    assert set(opened) == {"AUSDT", "BUSDT"}
    assert not bot._price_hold and not bot._bg_tasks                # precios soltados al terminar, sin tareas colgadas
    assert opened["AUSDT"].direction == st.SHORT and opened["BUSDT"].direction == st.LONG
    opens = [(t, p) for t, p in enviados if p["action"] == "open"]
    closes = [(t, p) for t, p in enviados if p["action"] == "close"]
    # Al executor con el precio en vivo, sin esperar el libro, antes del cobro; el cierre, después.
    assert {p["symbol"]: p["price"] for _, p in opens} == {"AUSDT": 1.0, "BUSDT": 2.0}
    assert all(p["strategy"] == "funding" and "tp" not in p for _, p in opens)
    assert all(t < ts for t, _ in opens) and all(t >= ts for t, _ in closes) and len(closes) == 2
    assert all(p["reason"] == "FUNDING" for _, p in closes)
    assert "AUSDT" in pedidos
    assert not eng.positions
    a = next(t for t in eng.closed_trades if t["symbol"] == "AUSDT")
    # La entrada en papel se corrigió con el libro (SHORT vende a los bids) y el PnL suma el funding.
    assert a["entry_price"] == pytest.approx(0.99) and a["entry_src"] == "libro"
    assert a["funding_usd"] == pytest.approx(a["notional"] * 0.45 / 100)
    assert a["pnl"] == pytest.approx(a["pnl_gross"] - a["fees"] + a["funding_usd"])


def test_pausado_bloqueado_o_tarde_no_abre(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    eng = bot.engines[st.ENGINE_FUNDING]
    bot._price = lambda sym: 1.0
    ts = bot._srv_now() + 5
    eng.blocked[st.SHORT] = True
    assert bot._funding_open("AUSDT", st.SHORT, 0.5, ts, 1.0) is None
    assert eng.signals[0]["action"].startswith("bloqueada")
    eng.blocked[st.SHORT] = False
    assert bot._funding_open("AUSDT", st.SHORT, 0.5, bot._srv_now() - 0.1, 1.0) is None
    assert "tarde" in eng.signals[0]["action"]
    eng.paused = True
    assert bot._funding_open("AUSDT", st.SHORT, 0.5, ts, 1.0) is None
    assert eng.signals[0]["action"] == "pausado"
    eng.paused = False
    assert bot._funding_open("AUSDT", st.SHORT, 0.5, ts, 1.0) is not None
    assert bot._funding_open("AUSDT", st.SHORT, 0.5, ts, 1.0) is None          # una por cobro y par
    assert "ya hay" in eng.signals[0]["action"]


def test_funding_solo_cuenta_si_estaba_abierta_en_el_cobro(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    now = time.time()
    p = st.Position(trade_id=1, symbol="AUSDT", direction=st.SHORT, entry_price=1, qty=20, notional=20,
                    tp_price=0, signal_pct=0.5, candle_open=1, candle_close=1, candle_time=0,
                    opened_ts=now - 2, strategy=st.ENGINE_FUNDING, funding_rate=0.5, funding_ts=now + 1)
    assert bot._funding_paid(p) == 0.0                                 # aún no cobró
    assert bot._funding_paid(p, now + 1.5) == pytest.approx(0.1)
    p.opened_ts = now + 1.2                                            # abrió tarde: no cobra
    assert bot._funding_paid(p, now + 1.5) == 0.0


def test_tras_un_reinicio_cierra_las_de_funding_vencidas(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    cerradas = []
    bot._close = lambda tid, px, reason, *a, **k: cerradas.append((tid, reason))
    eng = bot.engines[st.ENGINE_FUNDING]
    for tid, ts in ((1, time.time() - 30), (2, time.time() + 60)):
        eng.positions[tid] = st.Position(trade_id=tid, symbol="AUSDT", direction=st.SHORT, entry_price=1, qty=20,
                                         notional=20, tp_price=0, signal_pct=0.5, candle_open=1, candle_close=1,
                                         candle_time=0, strategy=st.ENGINE_FUNDING, funding_rate=0.5, funding_ts=ts)
    bot._funding_overdue()
    assert cerradas == [(1, "FUNDING")]


def test_mark_price_por_websocket_y_reloj(tmp_path, monkeypatch):
    import json
    raw = json.dumps({"stream": "!markPrice@arr", "data": [
        {"e": "markPriceUpdate", "s": "AUSDT", "p": "1.1", "r": "0.0035", "T": 3600000},
        {"e": "markPriceUpdate", "s": "ABTC", "p": "1", "r": "0.01", "T": 3600000}]})
    assert st.KlineBot._parse_mark_rows(raw) == [
        {"symbol": "AUSDT", "rate_pct": pytest.approx(0.35), "next_ts": 3600.0, "mark": 1.1}]
    assert st.KlineBot._parse_mark_rows("no es json") is None
    # Reloj: solo valen las muestras rápidas; si discrepan demasiado, no hay medición fiable.
    off, rtt, _spread = st.pick_clock_offset([(0.05, 1.50), (0.06, 1.51), (0.055, 1.49), (0.9, 3.0)])
    assert off == pytest.approx(1.50) and rtt == pytest.approx(0.05)
    assert st.pick_clock_offset([(0.05, 1.0), (0.05, 2.0), (0.05, 3.0)]) is None
    assert st.clock_compare(None, {"a": (1.0, 0.01, 0), "b": (1.2, 0.01, 0)})[:2] == (pytest.approx(1.1), "proveedores")


def test_precio_pedido_por_dos_no_se_suelta_antes_de_tiempo(tmp_path, monkeypatch):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot._hold_price(["AUSDT"])                     # p. ej. la consulta del ratio
    bot._hold_price(["AUSDT"])                     # y la entrada de funding del mismo par
    bot._release_price(["AUSDT"])                  # el ratio termina…
    assert "AUSDT" in bot._price_hold              # …y el funding sigue teniendo su precio en vivo
    bot._release_price(["AUSDT"])
    assert not bot._price_hold


def test_api_funding(tmp_path, monkeypatch):
    monkeypatch.setattr(st, "SETTINGS_FILE", str(tmp_path / "s.json"))
    monkeypatch.setattr(st.bot.store, "can_act", lambda: True)
    c = st.app.test_client()
    r = c.post("/api/funding", json={"notional": 25, "min_pct": 0.4})
    assert r.status_code == 200 and r.get_json()["notional"] == 25 and r.get_json()["min_pct"] == 0.4
    assert c.post("/api/funding", json={"close_after_s": 999}).status_code == 400
    assert c.get("/api/funding").get_json()["cfg"]["notional"] == 25
    assert "funding" in c.get("/api/status?view=bots").get_json()

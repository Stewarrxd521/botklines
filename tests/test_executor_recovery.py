"""Recuperar del executor (GET /api/state) las abiertas que el bot perdió tras un reinicio.
Fuera de eso el bot ya no consulta al executor: abre y cierra con el precio en vivo."""
import gzip
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import app as st

ESTADO = {"open_trades": [], "closed_trades": [{"id": i, "symbol": "BUSDT", "direction": "SHORT", "close_price": 0.88,
                                                "tranches": [{"trade_id": i, "fill_price": 0.96}]}
                                               for i in range(20)]}


def _bot(tmp_path, monkeypatch, estado):
    monkeypatch.setattr(st, "SETTINGS_FILE", str(tmp_path / "s.json"))
    bot = st.KlineBot()
    bot.store.can_open = lambda: True
    bot.store.can_act = lambda: True
    bot.store.mark_dirty = lambda *a, **k: None
    bot.store.append_trade = lambda *a, **k: None
    bot.executor.set_target("http://executor.test")
    enviados = []
    bot.executor.send = lambda payload, *a, **k: enviados.append(payload)
    bot.executor.fetch_state = lambda conns: (estado, 1000)
    bot.fee_pct = 0.0
    bot.min_hold_s = 0.0
    bot._price = lambda sym: None
    return bot, enviados


def _pos(tid, sym="BUSDT", entry=1.0, eng=st.ENGINE_RATIO):
    return st.Position(trade_id=tid, symbol=sym, direction=st.SHORT, entry_price=entry, qty=5, notional=5.0,
                       tp_price=0.9, sl_price=1.05, signal_pct=-27, candle_open=1.18, candle_close=0.86,
                       candle_time=0, last_price=entry, strategy=eng)


def test_fetch_state_pide_y_descomprime_gzip():
    vistos = {}

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            vistos["enc"] = self.headers.get("Accept-Encoding")
            vistos["secret"] = self.headers.get("X-Signal-Secret")
            body = gzip.compress(json.dumps(ESTADO).encode())
            self.send_response(200)
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        b = st.ExecutorBridge(f"http://127.0.0.1:{srv.server_port}", "s3", logger=lambda m: None)
        doc, wire = b.fetch_state({})
        assert doc == ESTADO and wire < len(json.dumps(ESTADO))
        assert vistos == {"enc": "gzip", "secret": "s3"}
    finally:
        srv.shutdown()


# ── Abiertas recuperadas del executor tras un reinicio ───────────────────────
# Foto real del executor (12-11-2026): una "adoptada" sin lotes del bot y una PLAY SHORT con dos lotes
# apilados (dos bots de ratio en el mismo par y dirección).
ABIERTAS_REALES = {"open_trades": [
    {"id": 1, "symbol": "HYPEUSDT", "direction": "LONG", "status": "OPEN", "quantity": 0.26,
     "entry_price": 73.161, "source": "adoptada", "lots": {}, "paper_ids": [], "tranches": []},
    {"id": 85, "symbol": "PLAYUSDT", "direction": "SHORT", "status": "OPEN", "quantity": 1360.0,
     "bot_quantity": 1308.0, "entry_price": 0.00766, "opened_ts": 1794499106.110159, "source": "signal",
     "lots": {"1794499106": 680.0, "1794499107": 680.0}, "paper_ids": [1794499106, 1794499107],
     "tranches": [
         {"level": -27.7192, "signal_price": 0.00765661, "trade_id": 1794499106, "ts": 1794499106.0397065,
          "bot_qty": 654.0, "qty": 680.0, "fill_price": 0.00766, "fill_ms": 1794499106075, "price_real": True},
         {"level": -27.7192, "signal_price": 0.00765661, "trade_id": 1794499107, "ts": 1794499106.1414185,
          "bot_qty": 654.0, "qty": 680.0, "fill_price": 0.00766, "fill_ms": 1794499106177, "price_real": True}]},
], "closed_trades": []}


def test_lotes_abiertos_del_bot_en_el_executor():
    lots = st.executor_open_lots(ABIERTAS_REALES)
    assert [(l["trade_id"], l["symbol"], l["direction"], l["price"], l["qty"]) for l in lots] == [
        (1794499106, "PLAYUSDT", "SHORT", 0.00766, 654.0), (1794499107, "PLAYUSDT", "SHORT", 0.00766, 654.0)]


def test_tras_reiniciar_sin_estado_se_recuperan_las_abiertas_del_executor(tmp_path, monkeypatch):
    bot, enviados = _bot(tmp_path, monkeypatch, ABIERTAS_REALES)
    assert bot._adopt_executor_lots(st.executor_open_lots(ABIERTAS_REALES)) == 2
    a = bot.engines[st.ENGINE_RATIO].positions[1794499106]
    b = bot.engines[st.ENGINE_RATIO_TP].positions[1794499107]      # una por bot de ratio y par/dirección
    for p in (a, b):
        assert p.symbol == "PLAYUSDT" and p.direction == "SHORT" and p.entry_price == 0.00766
        assert p.qty == 654.0 and p.sent and p.entry_src == "executor" and p.opened_ts > 1794499106
    tp, sl = bot._ratio_exit_prices(0.00766, "SHORT", st.ENGINE_RATIO)
    assert a.tp_price == pytest.approx(tp) and a.sl_price == pytest.approx(sl)
    assert b.tp_price == 0 and b.sl_price == 0                        # solo TP ratio: sin TP/SL de precio
    assert bot.trade_id_seq >= 1794499107
    # Una segunda pasada (o tras recuperar el estado guardado) no duplica nada.
    assert bot._adopt_executor_lots(st.executor_open_lots(ABIERTAS_REALES)) == 0
    assert all(e["action"] == "open_tp" for e in enviados)          # solo pone su TP; no abre ni cierra


def test_no_se_recuperan_las_que_el_bot_ya_tiene_o_ya_cerro(tmp_path, monkeypatch):
    bot, _ = _bot(tmp_path, monkeypatch, ABIERTAS_REALES)
    bot.engines[st.ENGINE_RATIO].positions[1794499106] = _pos(1794499106, sym="PLAYUSDT")
    bot.engines[st.ENGINE_RATIO_REV].closed_trades = [{"trade_id": 1794499107}]
    assert bot._adopt_executor_lots(st.executor_open_lots(ABIERTAS_REALES)) == 0


def test_el_bot_que_guarda_el_executor_manda(tmp_path, monkeypatch):
    bot, _ = _bot(tmp_path, monkeypatch, {})
    lot = dict(st.executor_open_lots(ABIERTAS_REALES)[0], strategy="ratio_rev")
    assert bot._recover_engine(lot) == st.ENGINE_RATIO_REV
    assert bot._recover_engine(dict(lot, strategy="", level=3.2)) == st.ENGINE_CLOSE   # cuerpo de vela


def test_la_recuperacion_reintenta_si_el_executor_esta_dormido(tmp_path, monkeypatch):
    import asyncio
    bot, _ = _bot(tmp_path, monkeypatch, ABIERTAS_REALES)
    intentos = []

    def fetch(conns):
        intentos.append(1)
        if len(intentos) == 1:
            raise OSError("Render despertando")
        return ABIERTAS_REALES, 100
    bot.executor.fetch_state = fetch

    async def rapido(_s):
        return None
    monkeypatch.setattr(st.asyncio, "sleep", rapido)
    assert asyncio.run(bot._recover_from_executor()) == 2 and len(intentos) == 2

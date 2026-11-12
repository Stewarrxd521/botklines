"""Precios reales de entrada y salida pedidos al executor (GET /api/state)."""
import gzip
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import app as st

ESTADO = {
    "open_trades": [{
        "id": 85, "symbol": "AUSDT", "direction": "SHORT", "status": "OPEN", "close_price": 0.0,
        "paper_ids": [101, 102], "lots": {"101": 5.0, "102": 5.0},
        "tranches": [{"trade_id": 101, "fill_price": 0.97, "price_real": True},
                     {"trade_id": 102, "fill_price": 0.975}],
    }],
    "closed_trades": [{
        "id": 75, "symbol": "BUSDT", "direction": "SHORT", "status": "TP", "close_price": 0.88,
        "closed_ts": 0.0, "paper_ids": [7], "lots": {},
        "tranches": [{"trade_id": 7, "fill_price": 0.96, "price_real": True}],
    }, {
        "id": 86, "symbol": "CUSDT", "status": "EXTERNAL", "close_price": 5.0, "tranches": [], "paper_ids": [],
    }],
}


def test_se_leen_entradas_por_lote_y_cierres_por_posicion():
    entradas, cierres = st.parse_executor_fills(ESTADO)
    assert entradas == {101: 0.97, 102: 0.975, 7: 0.96}
    assert cierres == {7: (0.88, 0.0)}                 # la abierta y la EXTERNAL no dan cierre


def test_precio_no_real_no_se_usa():
    e, _ = st.parse_executor_fills({"open_trades": [{"tranches": [{"trade_id": 1, "fill_price": 2.0,
                                                                   "price_real": False}]}]})
    assert e == {}


def _bot(tmp_path, monkeypatch, estado):
    monkeypatch.setattr(st, "SETTINGS_FILE", str(tmp_path / "s.json"))
    monkeypatch.setattr(st, "EXECUTOR_FILLS", True)
    monkeypatch.setattr(st, "EXECUTOR_FILL_FIRST_S", 0.02)
    monkeypatch.setattr(st, "EXECUTOR_FILL_EVERY_S", 0.02)
    monkeypatch.setattr(st, "EXECUTOR_CLOSE_WAIT_S", 0.5)
    bot = st.KlineBot()
    bot.store.can_open = lambda: True
    bot.store.can_act = lambda: True
    bot.store.mark_dirty = lambda *a, **k: None
    bot.store.append_trade = lambda *a, **k: None
    bot.executor.set_target("http://executor.test")
    enviados = []
    bot.executor.send = lambda payload, *a, **k: enviados.append(payload)
    consultas = []

    def fetch(conns):
        consultas.append(time.time())
        return estado, 1000
    bot.executor.fetch_state = fetch
    bot.fee_pct = 0.0
    bot.min_hold_s = 0.0
    bot._price = lambda sym: None
    return bot, enviados, consultas


def _espera(cond, s=3.0):
    t = time.time() + s
    while time.time() < t:
        if cond():
            return True
        time.sleep(0.01)
    return False


def _pos(tid, sym="BUSDT", entry=1.0, eng=st.ENGINE_RATIO):
    return st.Position(trade_id=tid, symbol=sym, direction=st.SHORT, entry_price=entry, qty=5, notional=5.0,
                       tp_price=0.9, sl_price=1.05, signal_pct=-27, candle_open=1.18, candle_close=0.86,
                       candle_time=0, last_price=entry, strategy=eng, entry_src="libro")


def test_la_entrada_real_corrige_entrada_y_tp_sl(tmp_path, monkeypatch):
    bot, enviados, consultas = _bot(tmp_path, monkeypatch, ESTADO)
    eng = bot.engines[st.ENGINE_RATIO]
    p = _pos(101, sym="AUSDT")
    eng.positions[101] = p
    p.ex_tp = 0.9
    bot._refine_entry(eng, p)
    assert _espera(lambda: p.entry_src == "executor")
    assert p.entry_price == 0.97 and p.notional == pytest.approx(5 * 0.97)
    tp, sl = bot._ratio_exit_prices(0.97, st.SHORT, st.ENGINE_RATIO)
    assert p.tp_price == pytest.approx(tp) and p.sl_price == pytest.approx(sl)
    assert any(e["action"] == "open_tp" and e["trigger_price"] == pytest.approx(tp) for e in enviados)
    assert _espera(lambda: bot.fills.pending() == 0) and len(consultas) == 1   # una consulta y se duerme
    time.sleep(0.1)
    assert len(consultas) == 1


def test_el_cierre_se_contabiliza_con_el_precio_del_executor(tmp_path, monkeypatch):
    bot, enviados, _ = _bot(tmp_path, monkeypatch, ESTADO)
    eng = bot.engines[st.ENGINE_RATIO]
    eng.positions[7] = _pos(7)
    bot._books["BUSDT"] = (time.time(), [["0.89", "10"]], [["0.90", "10"]], "libro")
    assert bot._close(7, 0.9, "TP")
    assert enviados[0]["action"] == "close"
    assert _espera(lambda: eng.closed_trades)
    rec = eng.closed_trades[0]
    assert rec["close_price"] == 0.88 and rec["close_src"] == "executor"
    assert rec["pnl"] == pytest.approx((1.0 - 0.88) * 5)


def test_sin_precio_del_executor_a_tiempo_queda_el_del_libro(tmp_path, monkeypatch):
    bot, _, consultas = _bot(tmp_path, monkeypatch, {"open_trades": [], "closed_trades": []})
    eng = bot.engines[st.ENGINE_RATIO]
    eng.positions[9] = _pos(9)
    bot._books["BUSDT"] = (time.time(), [["0.89", "10"]], [["0.90", "10"]], "libro")
    assert bot._close(9, 0.9, "TP")
    assert eng.closed_trades == []                     # espera al executor
    assert _espera(lambda: eng.closed_trades)
    rec = eng.closed_trades[0]
    assert rec["close_price"] == pytest.approx(0.90) and "executor sin precio" in rec["close_src"]
    assert len(consultas) >= 2


def test_las_simuladas_no_preguntan_al_executor(tmp_path, monkeypatch):
    bot, _, consultas = _bot(tmp_path, monkeypatch, ESTADO)
    eng = bot.engines[st.ENGINE_RATIO]
    p = _pos(7)
    p.sent = False
    eng.positions[7] = p
    bot._books["BUSDT"] = (time.time(), [["0.89", "10"]], [["0.90", "10"]], "libro")
    bot._close(7, 0.9, "TP")
    assert eng.closed_trades and eng.closed_trades[0]["close_src"] == "libro" and consultas == []


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

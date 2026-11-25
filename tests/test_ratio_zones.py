"""Filtro por zonas de ratio_entry (bots "ratio" y "ratio_rev")."""
import time

import pytest

import app as st  # noqa: E402

from test_strategy import _bot_sin_red  # noqa: E402


def _bot(tmp_path, monkeypatch, filtro=True, enviados=None, **zona):
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot.filters = {"AUSDT": {"stepSize": 0.001, "minQty": 0, "minNotional": 5}}
    bot.ratio_cfg.update(tp_pct=10.0, sl_pct=5.0, ratio_exit=True)
    bot.rev_cfg.update(tp_pct=10.0, sl_pct=5.0, ratio_exit=True, revenge=False)
    for n in st.ZONE_ENGINES:
        bot.zone_cfg[n] = st.load_zone_cfg({"filtro_ratio_activo": filtro, **zona})
    for n in (st.ENGINE_RATIO_TP, st.ENGINE_RATIO_REV):
        bot.engines[n].paused = True                    # salvo que la prueba lo pida, solo el bot ratio TP/SL
    for e in bot.engines.values():
        e.blocked = {st.LONG: False, st.SHORT: False}
    if enviados is not None:
        bot.executor_paused = False
        bot.executor.send = lambda payload, *a, **k: enviados.append(payload)
    bot._price = lambda sym: 100.0
    return bot


def _senal(bot, side, ratio_entry, ts=None):
    """Una señal del ratio: SHORT = el ratio cae 20 % en un ganador; LONG = sube 20 % en un perdedor."""
    ts = ts or int(time.time() * 1000)
    bot._tick = {"AUSDT": (25.0 if side == st.SHORT else -25.0, 100.0, time.time())}
    prev = ratio_entry / 0.8 if side == st.SHORT else ratio_entry / 1.2
    bot._on_ratio_points("AUSDT", [(ts - 300_000, prev), (ts, ratio_entry)])
    return ts


def _abiertas(bot, name=st.ENGINE_RATIO):
    return list(bot.engines[name].positions.values())


# ── Reglas puras ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("ratio,zona", [(0.5, "INVERTIR"), (1.3, "INVERTIR"), (1.3001, "NORMAL"),
                                        (2.0, "NORMAL"), (2.0001, "ALTA"), (5, "ALTA")])
def test_zonas_y_limites(ratio, zona):
    assert st.ratio_zone(ratio, 1.3, 2.0) == zona


def test_plan_invertido_intercambia_tp_y_sl_y_los_ceros():
    cfg = st.load_zone_cfg({"filtro_ratio_activo": True})
    p = st.zone_plan(st.LONG, 1.1, 10, 5, cfg)
    assert p == {"zona": "INVERTIR", "direction": st.SHORT, "tp_pct": 5, "sl_pct": 10, "inverted": True}
    assert st.zone_plan(st.LONG, 1.1, 10, 0, cfg)["tp_pct"] == 0      # sin SL → la invertida sin TP
    assert st.zone_plan(st.LONG, 1.1, 0, 5, cfg)["sl_pct"] == 0       # sin TP → la invertida sin SL
    assert st.zone_plan(st.SHORT, 3, 10, 5, {**cfg, "zona_alta_modo": "no_operar"}) is None
    assert st.zone_plan(st.SHORT, 3, 10, 5, cfg)["direction"] == st.SHORT


def test_valores_por_defecto():
    cfg = st.load_zone_cfg({})
    assert cfg == {"filtro_ratio_activo": True, "limite_invertir": 1.3, "limite_normal": 2.0,
                   "zona_alta_modo": "normal", "tp_ratio_en_invertidas": True}


# ── Casos pedidos ────────────────────────────────────────────────────────────

def test_long_ratio_110_abre_short_invertida(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    _senal(bot, st.LONG, 1.10)
    (p,) = _abiertas(bot)
    assert p.direction == st.SHORT and p.direccion_senal == st.LONG and p.inverted
    assert p.tp_price == pytest.approx(95) and p.sl_price == pytest.approx(110)
    assert (p.tp_pct_aplicado, p.sl_pct_aplicado, p.zona_ratio) == (5, 10, "INVERTIR")
    # TP por ratio activo por defecto en las invertidas, con la dirección ORIGINAL (LONG: el ratio baja).
    assert bot._ratio_tp_by_ratio(p)
    assert p.ratio_target < p.ratio_entry


def test_long_ratio_110_sin_tp_por_ratio_si_se_apaga_en_invertidas(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch, tp_ratio_en_invertidas=False)
    ts = _senal(bot, st.LONG, 1.10)
    (p,) = _abiertas(bot)
    assert not bot._ratio_tp_by_ratio(p)
    assert "AUSDT" not in bot._ratio_symbols()                  # no gasta REST esperando un TP por ratio
    bot._ratio_check_tp("AUSDT", [(ts + 300_000, 0.5)])
    assert p.trade_id in bot.engines[st.ENGINE_RATIO].positions


def test_invertida_cierra_cuando_la_original_cerraria_por_ratio(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    ts = _senal(bot, st.LONG, 1.10)
    (p,) = _abiertas(bot)
    bot._price = lambda sym: 101.0                              # la invertida (SHORT) va en pérdida
    bot._ratio_check_tp("AUSDT", [(ts + 300_000, p.ratio_target * 1.01)])
    assert p.trade_id in bot.engines[st.ENGINE_RATIO].positions  # la original aún no habría cerrado
    bot._ratio_check_tp("AUSDT", [(ts + 600_000, p.ratio_target * 0.99)])
    assert p.trade_id not in bot.engines[st.ENGINE_RATIO].positions
    rec = bot.engines[st.ENGINE_RATIO].closed_trades[0]
    assert rec["reason"] == st.RATIO_INV_REASON and rec["pnl"] < 0   # cierra aunque pierda (no es un TP)


def test_short_ratio_130_abre_long_invertida(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    _senal(bot, st.SHORT, 1.30)
    (p,) = _abiertas(bot)
    assert p.direction == st.LONG and p.inverted and p.zona_ratio == "INVERTIR"
    assert p.tp_price == pytest.approx(105) and p.sl_price == pytest.approx(90)


def test_long_ratio_175_normal(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    _senal(bot, st.LONG, 1.75)
    (p,) = _abiertas(bot)
    assert p.direction == st.LONG and not p.inverted and p.zona_ratio == "NORMAL"
    assert p.tp_price == pytest.approx(110) and p.sl_price == pytest.approx(95)
    assert bot._ratio_tp_by_ratio(p)


def test_short_ratio_200_normal(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    _senal(bot, st.SHORT, 2.00)
    (p,) = _abiertas(bot)
    assert p.direction == st.SHORT and not p.inverted and p.zona_ratio == "NORMAL"
    assert p.tp_price == pytest.approx(90) and p.sl_price == pytest.approx(105)


def test_short_ratio_240_no_operar(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch, zona_alta_modo="no_operar")
    _senal(bot, st.SHORT, 2.40)
    assert _abiertas(bot) == []
    assert bot.engines[st.ENGINE_RATIO].signals[0]["action"].startswith("no_operar")


def test_short_ratio_240_zona_alta_normal_por_defecto(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    _senal(bot, st.SHORT, 2.40)
    (p,) = _abiertas(bot)
    assert p.direction == st.SHORT and p.zona_ratio == "ALTA" and not p.inverted


@pytest.mark.parametrize("side,ratio", [(st.LONG, 1.10), (st.SHORT, 1.30), (st.LONG, 1.75), (st.SHORT, 2.40)])
def test_filtro_apagado_todo_igual_que_antes(tmp_path, monkeypatch, side, ratio):
    bot = _bot(tmp_path, monkeypatch, filtro=False, zona_alta_modo="no_operar")
    _senal(bot, side, ratio)
    (p,) = _abiertas(bot)
    tp, sl = st.tp_sl_prices(100.0, side, 10, 5)
    assert p.direction == side and not p.inverted and p.zona_ratio == ""
    assert p.tp_price == pytest.approx(tp) and p.sl_price == pytest.approx(sl)


# ── Condiciones ──────────────────────────────────────────────────────────────

def test_cambiar_tp_sl_en_caliente_llega_intercambiado_a_las_invertidas(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    _senal(bot, st.LONG, 1.10)                       # invertida SHORT
    _senal(bot, st.LONG, 1.75, ts=int(time.time() * 1000) + 1)   # normal LONG
    inv, nor = sorted(_abiertas(bot), key=lambda p: p.inverted, reverse=True)
    bot.set_ratio_config(tp_pct=8, sl_pct=3)
    assert inv.direction == st.SHORT and inv.tp_price == pytest.approx(97) and inv.sl_price == pytest.approx(108)
    assert (inv.tp_pct_aplicado, inv.sl_pct_aplicado) == (3, 8)
    assert nor.tp_price == pytest.approx(108) and nor.sl_price == pytest.approx(97)


def test_el_filtro_solo_afecta_a_senales_nuevas(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch, filtro=False)
    _senal(bot, st.LONG, 1.10)
    (p,) = _abiertas(bot)
    bot.set_zone_config(st.ENGINE_RATIO, filtro_ratio_activo=True)
    assert p.direction == st.LONG and not p.inverted
    bot.set_ratio_config(tp_pct=8)                   # y sus TP/SL siguen sin intercambiarse
    assert p.tp_price == pytest.approx(108)


def test_la_invertida_manda_su_tp_a_binance(tmp_path, monkeypatch):
    enviados = []
    bot = _bot(tmp_path, monkeypatch, enviados=enviados)
    _senal(bot, st.LONG, 1.10)
    (p,) = _abiertas(bot)
    (op,) = [e for e in enviados if e["action"] == "open"]
    assert op["direction"] == st.SHORT and op["tp"] == pytest.approx(p.tp_price)


def test_cerrar_todas_cierra_tambien_las_invertidas(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    _senal(bot, st.LONG, 1.10)
    (p,) = _abiertas(bot)
    assert bot._close(p.trade_id, 100.0, "CERRAR_TODAS")
    assert bot.engines[st.ENGINE_RATIO].closed_trades[0]["reason"] == "CERRAR_TODAS"


def test_bot_venganza_tiene_su_propio_filtro(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    bot.engines[st.ENGINE_RATIO_REV].paused = False
    bot.set_zone_config(st.ENGINE_RATIO, filtro_ratio_activo=False)
    _senal(bot, st.LONG, 1.10)
    (normal,) = _abiertas(bot)
    (rev,) = _abiertas(bot, st.ENGINE_RATIO_REV)
    assert normal.direction == st.LONG and not normal.inverted
    assert rev.direction == st.SHORT and rev.inverted and rev.tp_price == pytest.approx(95)


def test_config_de_zonas_valida_y_se_guarda(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    with pytest.raises(ValueError):
        bot.set_zone_config(st.ENGINE_RATIO, limite_invertir=2.5)          # ≥ limite_normal
    with pytest.raises(ValueError):
        bot.set_zone_config(st.ENGINE_RATIO_TP, filtro_ratio_activo=True)  # ese bot no lo tiene
    bot.set_zone_config(st.ENGINE_RATIO_REV, limite_invertir=1.1, limite_normal=1.8, zona_alta_modo="no_operar")
    nuevo = st.KlineBot()
    assert nuevo.zone_cfg[st.ENGINE_RATIO_REV]["limite_invertir"] == 1.1
    assert nuevo.zone_cfg[st.ENGINE_RATIO_REV]["zona_alta_modo"] == "no_operar"


def test_csv_lleva_las_columnas_del_filtro():
    for col in ("zona_ratio", "direccion_senal", "direction", "tp_pct_aplicado", "sl_pct_aplicado", "inverted"):
        assert col in st.TRADE_CSV_COLS


def test_api_de_zonas(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    monkeypatch.setattr(st, "bot", bot)
    monkeypatch.setattr(st, "_guard", lambda: None)
    c = st.app.test_client()
    r = c.post("/api/ratio_zones", json={"engine": "ratio_rev", "filtro_ratio_activo": False, "limite_normal": 2.5})
    assert r.status_code == 200 and r.get_json()["filtro_ratio_activo"] is False
    assert bot.zone_cfg[st.ENGINE_RATIO_REV]["limite_normal"] == 2.5
    assert c.post("/api/ratio_zones", json={"engine": "ratio", "zona_alta_modo": "x"}).status_code == 400

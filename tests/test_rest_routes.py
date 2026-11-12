import email.message
import io
import os
import time
import urllib.error

os.environ.setdefault("BOTKLINE_AUTOSTART", "false")

import app as st  # noqa: E402

from test_strategy import _bot_sin_red  # noqa: E402


def _http_error(code, retry_after=None, body=""):
    hdrs = email.message.Message()
    if retry_after is not None:
        hdrs["Retry-After"] = str(retry_after)
    return urllib.error.HTTPError("https://fapi.binance.com/x", code, "err", hdrs, io.BytesIO(body.encode()))


def _bot(tmp_path, monkeypatch, proxies=("http://u:p@px1:80", "http://u:p@px2:80")):
    monkeypatch.setattr(st, "REST_PROXIES", list(proxies))
    bot = _bot_sin_red(tmp_path, monkeypatch)
    bot._rest_get = st.KlineBot._rest_get.__get__(bot)      # el de verdad, con http_get_json simulado
    bot.log = lambda *a, **k: None
    return bot


def test_descanso_tras_418_limitado_a_dos_ciclos():
    cap = 2 * 300
    assert st.route_ban_s("86400", cap) == cap           # Binance pide 1 día: se esperan 10 min, no más
    assert st.route_ban_s("120", cap) == 120
    assert st.route_ban_s("1", cap) == 30                # mínimo 30 s
    assert st.route_ban_s(None, cap) == cap
    assert st.route_ban_s("basura", cap) == cap
    assert st.ban_until_from_body('{"code":-1003,"msg":"Way too much request weight used; IP banned until '
                                  '1731370000000. Please use WebSocket"}') == 1731370000.0
    assert st.ban_until_from_body("otro error") is None


def test_ip_publica_vetada_pasa_a_proxies_y_vuelve_tras_la_prueba(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    rutas = []
    baneada = {"": True}

    def fake(url, route="", timeout=15.0, headers_out=None):
        rutas.append(route)
        if route == "" and baneada[""]:
            raise _http_error(418, retry_after=1_200_000, body="IP banned until 9999999999999")
        return [{"timestamp": 1, "longShortRatio": "1.0"}], 10

    monkeypatch.setattr(st, "http_get_json", fake)
    assert bot._rest_get("/x")                              # 418 por la IP pública → sale por un proxy
    assert rutas[0] == "" and rutas[1] != ""
    blocked = bot._route_block[""] - time.time()
    assert 590 < blocked <= 600                             # 2 ciclos, no 1 200 000 s
    assert "" in bot._route_probe

    rutas.clear()
    bot._rest_get("/x")
    assert rutas == [rutas[0]] and rutas[0] != ""           # mientras descansa no se toca la IP pública

    bot._route_block[""] = time.time() - 1                  # pasan los 2 ciclos
    baneada[""] = False
    order = bot._rest_routes()
    assert order[0] == ""                                   # esta petición se queda con la prueba…
    assert "" not in bot._rest_routes()                     # …y las concurrentes no la ven
    bot._route_block[""] = time.time() - 1
    rutas.clear()
    bot._rest_get("/x")
    assert rutas == [""]                                    # respondió: IP pública rehabilitada
    assert "" not in bot._route_probe
    rutas.clear()
    bot._rest_get("/x")
    assert rutas == [""]


def test_prueba_fallida_vuelve_a_descansar(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)

    def fake(url, route="", timeout=15.0, headers_out=None):
        if route == "":
            raise _http_error(418, retry_after=99999)
        return [], 10

    monkeypatch.setattr(st, "http_get_json", fake)
    bot._rest_get("/x")
    bot._route_block[""] = time.time() - 1
    bot._rest_get("/x")
    assert bot._route_block[""] - time.time() > 590
    assert bot._route_bans[""] == 2


def test_todas_vetadas_esperan_como_mucho_dos_ciclos(tmp_path, monkeypatch):
    bot = _bot(tmp_path, monkeypatch)
    monkeypatch.setattr(st, "http_get_json",
                        lambda *a, **k: (_ for _ in ()).throw(_http_error(418, retry_after=10 ** 7)))
    try:
        bot._rest_get("/x")
    except RuntimeError as exc:
        assert "418" in str(exc)
    assert 0 < bot._rest_block_until - time.time() <= 600
    assert bot._rest_routes() == []


def test_reloj_se_re_mide_solo_si_envejece(tmp_path, monkeypatch):
    import asyncio
    bot = _bot(tmp_path, monkeypatch)
    monkeypatch.setattr(st, "FUNDING_CLOCK_RESYNC_S", 600.0)
    llamadas = []

    async def sync(full=True):
        llamadas.append(full)
        bot.funding_clock_ts = time.time()
        bot.running = False
        return True

    real_sleep = asyncio.sleep

    async def rapido(s):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", rapido)
    bot._funding_clock_sync = sync
    bot.running = True
    bot.funding_clock_ts = time.time() - 700                 # medición de hace 11 min
    bot._funding_target_ts = int(bot._srv_now()) + 1800       # cobro lejos
    asyncio.run(bot._clock_keeper_loop())
    assert llamadas == [False]                                # verificación rápida, solo Binance
    assert st.FUNDING_CLOCK_SYNC_BEFORE_S == 300              # la completa, 5 min antes del cobro


def test_reloj_sync_completa_tambien_2_min_antes(tmp_path, monkeypatch):
    import asyncio
    bot = _bot(tmp_path, monkeypatch)
    llamadas = []

    async def sync(full=True):
        llamadas.append(full)
        bot.funding_clock_ts = time.time()
        bot.running = False
        return True

    real_sleep = asyncio.sleep

    async def rapido(s):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", rapido)
    bot._funding_clock_sync = sync
    bot.running = True
    bot.funding_clock_ts = time.time()                      # medición reciente (la de 5 min)
    bot._funding_target_ts = int(bot._srv_now()) + 110       # faltan 110 s para el cobro
    asyncio.run(bot._clock_keeper_loop())
    assert llamadas == [True]
    assert st.FUNDING_CLOCK_RECHECK_BEFORE_S == 120

"""
app.py — Dos bots de velas 1m (Binance USDⓈ-M Futures) que siguen la tendencia,
en paralelo y con operaciones e historial separados, sobre la misma web.

Estrategia (común)
  • Velas 1m de TODOS los perpetuos USDT por WebSocket (kline_ws.py, sin REST).
  • Cuerpo de la vela = (cierre o último precio - apertura) / apertura.
      ≥ +umbral (3 %) → abre LONG   |   ≤ -umbral (-3 %) → abre SHORT
  • Cada entrada usa un notional fijo de NOTIONAL_USDT (5 USDT). Cada señal
    abre su propia posición aunque ya haya otras en el par, en cualquier
    dirección (se identifican por trade_id).
  • Cierre cuando el precio avanza el TP (2 %) o más a favor desde la entrada.
    SIN stop loss (solo cierre manual desde la web).
  • Modo invertido (un botón para los dos bots): la entrada va AL REVÉS de la
    vela (verde → SHORT, roja → LONG) y el mismo % funciona como STOP LOSS; no
    hay TP. Cada posición guarda el modo con el que se abrió.
  • Objetivo global: si PnL realizado + PnL abierto (desde el último objetivo)
    llega a +PROFIT_TARGET_USD (12 USD), se cierran TODAS las posiciones de los
    dos bots y el contador vuelve a cero.
  • El PnL es NETO: resta la comisión de entrada y la de salida (FEE_PCT).

Los dos bots
  • Bot al cierre ("close"): mira la vela cuando CIERRA.
  • Bot en vivo ("live"): mira la vela EN CURSO (Binance la manda cada ~250 ms)
    y entra en cuanto el cuerpo supera su umbral, sin esperar al cierre. Una
    sola entrada por símbolo y vela.
  Cada uno tiene su umbral, su TP, sus bloqueos LONG/SHORT, sus posiciones y
  su historial. Comparten los WebSockets, el executor y la web.

  Las órdenes NO salen de aquí: se mandan como señales al Executor externo
  (POST <executor>/signal) con "strategy": "close" | "live". El link del
  Executor se puede cambiar en caliente desde la web o con POST /api/executor.

Bots de ratio de top traders (tres bots con la MISMA entrada y salidas distintas)
  "ratio"      → TP y SL por precio + TP por ratio (configurables en caliente).
  "ratio_tp"   → sin TP/SL de precio: sale SOLO cuando el ratio recupera el % que se movió.
  "ratio_rev"  → TP/SL propios + TRADE DE VENGANZA: cuando una de sus operaciones cierra en
                 pérdida (por SL, TP por ratio, cierre manual…), abre otra en el mismo par con
                 el notional cerrado × REVENGE_MULT para recuperarla. TP = REVENGE_RECOVER_PCT %
                 de la pérdida y SL = REVENGE_SL_PCT % de (pérdida × mult), ambos en USD netos.
                 La venganza no encadena otra venganza, y no se dispara con el objetivo global
                 ni con «Cerrar todas».
  • Vigila SOLO los perpetuos con variación 24 h ≥ +RATIO_WATCH_MIN_PCT (20 %). El 24 h se
    lee por WebSocket (!miniTicker@arr) con una exploración de TICKER_SCAN_S (30 s) de todo
    el mercado en cada ciclo de 5 min, en vez de tener el stream abierto todo el día. Esa
    misma exploración descubre los símbolos: la lista se rehace a diario y se guarda en disco.
  • Cada 5 min consulta el Top Trader Long/Short Ratio (Accounts) de esa lista. (Binance
    NO ofrece este dato por WebSocket: es REST, 1 petición por símbolo vigilado cada 5 min,
    por el proxy, con freno automático ante HTTP 429/418.)
  • Si el ratio cae entre drop_min % y drop_max % de un punto de 5 m al siguiente → SHORT.
  • Salidas (lo que llegue antes, todo configurable en caliente desde la web):
      - TP por precio: +RATIO_TP_PCT % a favor (10 %).
      - SL por precio: -RATIO_SL_PCT % en contra (5 %).
      - TP por ratio (desactivable): el ratio RECUPERA el mismo % que cayó, medido desde
        el ratio de entrada (cae 27,1 % de 1,18 a 0,86 → TP cuando sube 27,1 % desde 0,86).
  • LONG (activo por defecto, RATIO_LONG_DEFAULT=false lo bloquea): espejo para los PERDEDORES
    (24 h ≤ -20 %): el ratio SUBE entre drop_min % y drop_max % → LONG; TP cuando el
    ratio baja ese mismo % desde el ratio de entrada.

Precios para TP/SL: WS.py, SOLO de los símbolos con posición abierta (aggTrade + markPrice@1s
con el bot de ratio; bookTicker en tiempo real si están activos los bots de velas).

Ancho de banda: sin streams de todo el mercado, REST con gzip, la web pide solo el
detalle del bot que se está viendo, las respuestas van comprimidas y el panel deja
de consultar cuando la pestaña del navegador está oculta.
Recuperación tras reinicio: state_store.py, solo con copia en disco local (Upstash
desactivado por ahora: en Render free el disco se borra en cada reinicio).
"""

from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import csv
import gzip
import hashlib
import io
import json
import http.client
import os
import queue
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urlencode, urlsplit
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from math import ceil
from typing import Deque, Dict, List, Optional, Tuple

from flask import Flask, jsonify, make_response, request

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from WS import SymbolWebSocketPriceCache                       # noqa: E402
from kline_ws import Candle, KlineWebSocketStream              # noqa: E402
from state_store import StateStore, make_owner_id                    # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL    = os.getenv("BASE_URL", "https://fapi.binance.com")
QUOTE_ASSET = os.getenv("QUOTE_ASSET", "USDT")
# Proxy HTTP opcional SOLO para la única consulta REST del arranque (exchangeInfo).
REST_PROXY_URL = os.getenv("REST_PROXY_URL", "").strip()   # http://usuario:clave@host:puerto (variable de entorno)
INITIAL_SYMBOLS = [s.strip().upper() for s in os.getenv("INITIAL_SYMBOLS", "").split(",") if s.strip()]
SYMBOLS_CACHE_FILE = os.getenv(
    "SYMBOLS_CACHE_FILE", os.path.join(tempfile.gettempdir(), "futures_symbols_cache.json"))
# exchangeInfo (filtros de cantidad) se pide cada 10 días como mucho; los nombres de los
# símbolos se descubren por WebSocket (ver SYMBOL_SCAN_HOURS).
SYMBOL_REFRESH_HOURS = float(os.getenv("SYMBOL_REFRESH_HOURS", "240"))

# ── Estrategia ────────────────────────────────────────────────────────────────
SIGNAL_THRESHOLD_PCT = float(os.getenv("SIGNAL_THRESHOLD_PCT", "3"))    # cuerpo mínimo de la vela
TAKE_PROFIT_PCT      = float(os.getenv("TAKE_PROFIT_PCT", "2"))         # avance a favor para cerrar
NOTIONAL_USDT        = float(os.getenv("NOTIONAL_USDT", "5"))           # tamaño de cada entrada
# Si el stepSize obliga a una cantidad mayor que NOTIONAL × esto, el par se salta.
MAX_NOTIONAL_OVER    = float(os.getenv("MAX_NOTIONAL_OVER", "1.5"))
# Una vela cuyo cierre llega más tarde que esto (reconexión) no genera señal.
SIGNAL_MAX_DELAY_S   = float(os.getenv("SIGNAL_MAX_DELAY_S", "10"))
# Pausa (s) antes de volver a operar un símbolo tras cerrarlo. 0 = sin pausa.
COOLDOWN_SECONDS     = float(os.getenv("COOLDOWN_SECONDS", "0"))
PRICE_MAX_AGE_S      = float(os.getenv("PRICE_MAX_AGE_S", "5"))         # precio más viejo no se usa
SAFETY_TICK_SECS     = float(os.getenv("SAFETY_TICK_SECS", "1"))        # revisión periódica del TP
# Modo invertido: entra contra la vela y el % del TP pasa a ser stop loss (sin TP).
INVERT_SIGNALS       = os.getenv("INVERT_SIGNALS", "false").lower() in ("1", "true", "yes")
# Comisión por lado en % del notional (taker de Binance USDⓈ-M: 0,05 %).
FEE_PCT              = float(os.getenv("FEE_PCT", "0.05"))
# Cierra todo cuando realizado + abierto (desde el último objetivo) llega a esto. 0 = apagado.
PROFIT_TARGET_USD    = float(os.getenv("PROFIT_TARGET_USD", "12"))
# Máximo de posiciones abiertas a la vez, sumando los dos bots. 0 = sin límite.
MAX_OPEN_POSITIONS   = int(os.getenv("MAX_OPEN_POSITIONS", "0"))
# Bot en vivo (vela en curso): por defecto los mismos % que el bot al cierre.
LIVE_SIGNAL_THRESHOLD_PCT = float(os.getenv("LIVE_SIGNAL_THRESHOLD_PCT", str(SIGNAL_THRESHOLD_PCT)))
LIVE_TAKE_PROFIT_PCT      = float(os.getenv("LIVE_TAKE_PROFIT_PCT", str(TAKE_PROFIT_PCT)))

# ── Bot de ratio de top traders ───────────────────────────────────────────────
# Los dos bots de velas 1m quedan apagados por defecto (no se abre el stream de velas).
# Valor inicial: luego se encienden/apagan en caliente desde la web (se guarda en SETTINGS_FILE).
KLINE_BOTS_ENABLED  = os.getenv("KLINE_BOTS_ENABLED", "false").lower() in ("1", "true", "yes")
# Stream de 24 h de TODOS los símbolos (un solo WebSocket). Ajustable si Binance cambia la ruta.
# Desde 2026-04-23 Binance retiró las URLs /ws y /stream de futuros: los streams de mercado van por /market.
# Se prueban en orden; si una no entrega datos en TICKER_SILENCE_S se pasa a la siguiente.
_t_env = os.getenv("TICKER_WS_URL", "").strip()
TICKER_WS_URLS      = ([_t_env] if _t_env else []) + [
    "wss://fstream.binance.com/market/ws/!miniTicker@arr",
    "wss://fstream.binance.com/market/ws/!ticker@arr",
]
TICKER_SILENCE_S    = float(os.getenv("TICKER_SILENCE_S", "20"))
RATIO_WATCH_MIN_PCT = float(os.getenv("RATIO_WATCH_MIN_PCT", "10"))   # |24 h| mínimo para vigilar
RATIO_DROP_MIN_PCT  = float(os.getenv("RATIO_DROP_MIN_PCT", "13"))    # caída mínima del ratio (5 m → 5 m)
RATIO_DROP_MAX_PCT  = float(os.getenv("RATIO_DROP_MAX_PCT", "100"))    # caída máxima (1,18→0,86 = 27 %)
# LONG = subida del ratio en perdedores (24 h ≤ -X %). Activo por defecto, igual que el SHORT.
RATIO_LONG_DEFAULT  = os.getenv("RATIO_LONG_DEFAULT", "true").strip().lower() in ("1", "true", "yes", "on")
RATIO_PERIOD        = "5m"
RATIO_REFRESH_S     = 300          # lista de vigilados y consulta del ratio, cada 5 min
RATIO_POLL_DELAY_S  = float(os.getenv("RATIO_POLL_DELAY_S", "15"))    # espera tras el minuto :00/:05
RATIO_RETRIES       = int(os.getenv("RATIO_RETRIES", "9"))            # reintentos de los rezagados dentro del ciclo
RATIO_RETRY_GAP_S   = float(os.getenv("RATIO_RETRY_GAP_S", "25"))
RATIO_REQ_BUDGET    = int(os.getenv("RATIO_REQ_BUDGET", "700"))       # máx. peticiones REST por ventana de 5 min (límite Binance: 1000)
RATIO_MAX_AGE_S     = float(os.getenv("RATIO_MAX_AGE_S", "720"))      # dato de ratio más viejo no dispara
RATIO_WATCH_MAX     = int(os.getenv("RATIO_WATCH_MAX", "100"))        # tope de símbolos vigilados
RATIO_CONCURRENCY   = max(1, int(os.getenv("RATIO_CONCURRENCY", "4")))
# Salidas por precio del bot de ratio (0 = apagado). Editables en caliente desde la web.
RATIO_TP_PCT        = float(os.getenv("RATIO_TP_PCT", "10"))           # take profit: +10 % a favor
RATIO_SL_PCT        = abs(float(os.getenv("RATIO_SL_PCT", "4.8")))     # stop loss: -4,8 % en contra
RATIO_EXIT_BY_RATIO = os.getenv("RATIO_EXIT_BY_RATIO", "true").lower() in ("1", "true", "yes")
# Trade de venganza (bot "ratio + venganza"): tras cerrar en pérdida abre otra operación para recuperarla.
REVENGE_ENABLED     = os.getenv("REVENGE_ENABLED", "true").lower() in ("1", "true", "yes")
REVENGE_MULT        = float(os.getenv("REVENGE_MULT", "11"))          # notional = notional cerrado × N
REVENGE_RECOVER_PCT = float(os.getenv("REVENGE_RECOVER_PCT", "64"))   # TP = este % de la pérdida (USD)
REVENGE_SL_PCT      = float(os.getenv("REVENGE_SL_PCT", "150"))       # SL = este % de (pérdida × N) en USD
REVENGE_DIR         = os.getenv("REVENGE_DIR", "opposite")            # opposite | same (respecto a la perdedora)
REVENGE_SKIP_REASONS = ("OBJETIVO", "CERRAR_TODAS")                   # cierres que no disparan venganza
# Fuente del 24 h, siempre por WebSocket: "scan" = exploración de TICKER_SCAN_S por ciclo de
# 5 min (~10 % del tiempo conectado); "ws" = !miniTicker@arr abierto siempre (~80 KB/s).
TICKER_MODE         = os.getenv("TICKER_MODE", "scan").strip().lower()
TICKER_SCAN_S       = float(os.getenv("TICKER_SCAN_S", "30"))         # duración máxima de cada exploración
# Las exploraciones de cada ciclo se cortan cuando un mensaje ya casi no trae símbolos nuevos
# (~2-4 s en vez de 30 s); la diaria, que rehace la lista de símbolos, dura TICKER_SCAN_S entero.
TICKER_SCAN_QUICK   = os.getenv("TICKER_SCAN_QUICK", "true").lower() in ("1", "true", "yes")
SYMBOL_SCAN_HOURS   = float(os.getenv("SYMBOL_SCAN_HOURS", "24"))     # la lista de símbolos se rehace a diario
KNOWN_SYMBOLS_FILE  = os.getenv("KNOWN_SYMBOLS_FILE", os.path.join(_HERE, "known_symbols.json"))

# ── Velas ─────────────────────────────────────────────────────────────────────
KLINE_INTERVAL = "1m"
KLINE_HISTORY  = max(2, int(os.getenv("KLINE_HISTORY", "2")))           # última y penúltima vela
KLINE_DTYPE    = os.getenv("KLINE_DTYPE", "float64")

# ── Executor externo ──────────────────────────────────────────────────────────
EXECUTOR_URL    = os.getenv("EXECUTOR_URL", "https://executor-5lu0.onrender.com")
EXECUTOR_SECRET = os.getenv("EXECUTOR_SECRET", "clave-secreta-aleatoria")
# Si se define, los POST de la web (cambiar executor, cerrar) exigen este token.
ADMIN_TOKEN     = os.getenv("ADMIN_TOKEN", "").strip()
EXECUTOR_WORKERS = max(1, int(os.getenv("EXECUTOR_WORKERS", "4")))      # envíos en paralelo
EXECUTOR_WARM_S  = float(os.getenv("EXECUTOR_WARM_S", "20"))            # HEAD para mantener viva la conexión

# ── Persistencia ──────────────────────────────────────────────────────────────
SETTINGS_FILE     = os.getenv("SETTINGS_FILE", os.path.join(_HERE, "bot_settings.json"))
RECOVERY_FILE     = os.getenv("RECOVERY_FILE", os.path.join(tempfile.gettempdir(), "botkline_recovery.json"))
STATE_KEY_PREFIX  = "botkline"
STATE_TRADES_MAX  = int(os.getenv("STATE_TRADES_MAX", "5000"))
STATE_BOOT_WAIT_S = float(os.getenv("STATE_BOOT_WAIT_S", "20"))
STATE_PERSIST_SECS = float(os.getenv("STATE_PERSIST_SECS", "30"))
SHUTDOWN_DRAIN_S  = 5.0

EVAL_MINUTES_KEPT = int(os.getenv("EVAL_MINUTES_KEPT", "30"))   # minutos de evaluaciones en la web
EVAL_TOP_N        = int(os.getenv("EVAL_TOP_N", "15"))          # mayores cuerpos mostrados

STATUS_POLL_MS = int(os.getenv("STATUS_POLL_MS", "3000"))
# Stream de precio por símbolo con posición: aggTrade (último precio operado, Binance lo
# agrupa cada 100 ms: ≤10 msg/s y nada si no hay operaciones) + markPrice@1s de respaldo.
# Los bots de velas usan bookTicker (tiempo real, mucho más tráfico).
PRICE_STREAM_ENV = os.getenv("WS_SYMBOL_STREAM", "").strip()
# Vigilados del bot de ratio (24 h ≥ ±X %) sin posición: markPrice cada 3 s (WATCH_PRICE_STREAM).
WATCH_LIVE_PRICE = os.getenv("WATCH_LIVE_PRICE", "true").lower() in ("1", "true", "yes")


def price_stream_for(klines_on: bool) -> str:
    return PRICE_STREAM_ENV or ("bookTicker" if klines_on else "aggTrade")


def http_get_json(url: str, proxy: str = "", timeout: float = 15.0) -> Tuple[object, int]:
    """GET JSON pidiendo gzip (Binance lo admite: ~5-10× menos bytes). (datos, bytes recibidos)."""
    handlers = [urllib.request.ProxyHandler({"http": proxy, "https": proxy})] if proxy else []
    opener = urllib.request.build_opener(*handlers)
    req = urllib.request.Request(url, headers={"User-Agent": "BOTKLINE/1.0", "Accept-Encoding": "gzip"})
    with opener.open(req, timeout=timeout) as resp:
        raw = resp.read()
        if (resp.headers.get("Content-Encoding") or "").lower() == "gzip":
            body = gzip.decompress(raw)
        else:
            body = raw
    return json.loads(body), len(raw)


def _utc(ts: float, pattern: str = "%Y-%m-%d %H:%M:%S") -> str:
    try:
        return datetime.fromtimestamp(float(ts), timezone.utc).strftime(pattern)
    except (TypeError, ValueError, OverflowError, OSError):
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# ESTRATEGIA (reglas puras, sin red ni estado: se prueban en tests/test_strategy.py)
#   Señal   cuerpo de la vela 1m = (cierre - apertura) / apertura: ≥ +umbral → LONG,
#           ≤ -umbral → SHORT. Modo invertido: al revés y el % pasa a ser stop loss.
#   Salida  TP en % a favor (bots de velas); TP y SL fijos en % (bot de ratio).
#   Fees    el PnL neto resta la comisión de entrada y la de salida.
#   Tamaño  notional fijo, cantidad redondeada HACIA ARRIBA al stepSize.
# ─────────────────────────────────────────────────────────────────────────────

LONG = "LONG"
SHORT = "SHORT"


def body_change_pct(open_: float, close: float) -> float:
    """% de cambio del cuerpo de una vela (cierre contra apertura)."""
    if open_ <= 0:
        return 0.0
    return (close - open_) / open_ * 100.0


def signal_direction(open_: float, close: float, threshold_pct: float) -> Optional[str]:
    """LONG si la vela subió ≥ threshold_pct, SHORT si bajó ≥ threshold_pct, si no None."""
    body = body_change_pct(open_, close)
    if body >= threshold_pct:
        return LONG
    if body <= -threshold_pct:
        return SHORT
    return None


def entry_direction(open_: float, close: float, threshold_pct: float,
                    inverted: bool = False) -> Optional[str]:
    """Dirección de la entrada: la de la vela, o la contraria en modo invertido."""
    d = signal_direction(open_, close, threshold_pct)
    if d is None or not inverted:
        return d
    return SHORT if d == LONG else LONG


def favorable_move_pct(price: float, entry: float, direction: str) -> float:
    """Cuánto se movió el precio a favor de la posición, en % de la entrada."""
    if entry <= 0 or price <= 0:
        return 0.0
    move = (price - entry) / entry * 100.0
    return move if direction == LONG else -move


def take_profit_price(entry: float, direction: str, tp_pct: float) -> float:
    factor = 1.0 + tp_pct / 100.0 if direction == LONG else 1.0 - tp_pct / 100.0
    return entry * factor


def stop_loss_price(entry: float, direction: str, sl_pct: float) -> float:
    factor = 1.0 - sl_pct / 100.0 if direction == LONG else 1.0 + sl_pct / 100.0
    return entry * factor


def exit_price(entry: float, direction: str, pct: float, inverted: bool = False) -> float:
    """Precio de salida: TP en modo normal, SL en modo invertido."""
    return stop_loss_price(entry, direction, pct) if inverted else take_profit_price(entry, direction, pct)


def exit_hit(price: float, target: float, direction: str, inverted: bool = False) -> bool:
    """¿El precio tocó la salida? TP (a favor) en normal, SL (en contra) en invertido."""
    if price <= 0 or target <= 0:
        return False
    up = price >= target
    down = price <= target
    if inverted:
        return down if direction == LONG else up
    return up if direction == LONG else down


def tp_sl_prices(entry: float, direction: str, tp_pct: float, sl_pct: float) -> Tuple[float, float]:
    """(precio de TP, precio de SL) con TP/SL fijos en % de la entrada. 0 % = apagado (precio 0)."""
    tp = take_profit_price(entry, direction, tp_pct) if tp_pct > 0 and entry > 0 else 0.0
    sl = stop_loss_price(entry, direction, sl_pct) if 0 < sl_pct < 100 and entry > 0 else 0.0
    return tp, sl


def price_exit(price: float, direction: str, tp_price: float, sl_price: float) -> Optional[str]:
    """"TP", "SL" o None según el precio haya tocado el take profit o el stop loss."""
    if exit_hit(price, sl_price, direction, inverted=True):
        return "SL"
    if exit_hit(price, tp_price, direction):
        return "TP"
    return None


def take_profit_hit(price: float, entry: float, direction: str, tp_pct: float) -> bool:
    if price <= 0 or entry <= 0:
        return False
    target = take_profit_price(entry, direction, tp_pct)
    return price >= target if direction == LONG else price <= target


def pnl_usd(price: float, entry: float, qty: float, direction: str) -> float:
    if price <= 0:
        return 0.0
    diff = price - entry
    return diff * qty if direction == LONG else -diff * qty


def fees_usd(entry: float, price: float, qty: float, fee_pct: float) -> float:
    """Comisión de entrada + salida (fee_pct % de cada notional)."""
    return (entry * qty + max(price, 0.0) * qty) * fee_pct / 100.0


def net_pnl_usd(price: float, entry: float, qty: float, direction: str, fee_pct: float) -> float:
    """PnL descontando la comisión de entrada y la de salida."""
    if price <= 0:
        return 0.0
    return pnl_usd(price, entry, qty, direction) - fees_usd(entry, price, qty, fee_pct)


def _decimals(step: float) -> int:
    text = f"{step:.12f}".rstrip("0")
    return len(text.split(".")[1]) if "." in text else 0


def size_order(notional: float, price: float, step: float = 0.001, min_qty: float = 0.0,
               min_notional: float = 5.0, max_over: float = 1.5) -> Tuple[float, float, str]:
    """(qty, notional_efectivo, "") o (0, 0, motivo) si el par no admite ese tamaño.

    La cantidad se redondea HACIA ARRIBA al stepSize: 5 USDT nunca queda en
    4,99 (Binance la rechazaría). Si el mínimo del par es mayor que el
    notional pedido, o el redondeo lo infla por encima de notional × max_over
    (pares caros con stepSize grueso), se descarta."""
    if price <= 0:
        return 0.0, 0.0, "sin precio"
    if min_notional > notional + 1e-9:
        return 0.0, 0.0, f"mínimo del par {min_notional:g} USDT > {notional:g} USDT"
    step = step if step > 0 else 0.001
    decs = _decimals(step)
    steps = ceil(notional / price / step - 1e-9)
    qty = round(max(steps * step, min_qty), decs)
    if qty * price < min_notional - 1e-9:
        qty = round(qty + step, decs)
    eff = qty * price
    if eff > notional * max_over:
        return 0.0, 0.0, f"stepSize {step:g} obliga a {eff:.2f} USDT (> {notional * max_over:g})"
    return qty, eff, ""


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTOR (señales de apertura/cierre)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ExecutorTarget:
    url:    str = ""
    secret: str = ""


class ExecutorBridge:
    """Envía señales al Executor. El destino se puede cambiar en caliente:
    cada envío toma el destino vigente en ese momento (lectura atómica)."""

    def __init__(self, url: str, secret: str, logger=print, timeout: float = 8.0) -> None:
        self.target = ExecutorTarget(self.clean_url(url), secret)
        self.logger = logger
        self.timeout = timeout
        self.sent = 0
        self.errors = 0
        self.last_error = ""
        self.last_ok_ts = 0.0
        self.last_http_ms = 0.0
        self.last_total_ms = 0.0
        # Una cola por hilo y cada símbolo siempre a la misma: las señales de un par llegan al
        # executor en el orden en que se mandaron (parcial antes que total, cierre antes que
        # la venganza en el mismo par); pares distintos siguen saliendo en paralelo.
        self._queues: List["queue.Queue[tuple]"] = [queue.Queue() for _ in range(EXECUTOR_WORKERS)]
        self._workers: List[threading.Thread] = []
        self._start_lock = threading.Lock()

    @staticmethod
    def clean_url(url: str) -> str:
        return (url or "").strip().rstrip("/")

    def set_target(self, url: str, secret: Optional[str] = None) -> ExecutorTarget:
        old = self.target
        self.target = ExecutorTarget(self.clean_url(url), old.secret if secret is None else secret)
        return self.target

    def _log(self, msg: str) -> None:
        try:
            self.logger(msg)
        except Exception:
            pass

    # ── Envío con conexiones persistentes ────────────────────────────────────
    # Abrir TCP+TLS por cada señal cuesta 100-300 ms hasta Render. Cada hilo
    # trabajador mantiene su conexión abierta (keep-alive) y la "calienta" con
    # un HEAD cada EXECUTOR_WARM_S si está ociosa, así una señal sale en el RTT.

    def _ensure_workers(self) -> None:
        if self._workers:
            return
        with self._start_lock:
            if self._workers:
                return
            for i, q in enumerate(self._queues):
                t = threading.Thread(target=self._worker, args=(q,), daemon=True, name=f"executor-{i}")
                t.start()
                self._workers.append(t)

    def _warm_conn(self, conns: dict) -> None:
        target = self.target
        if not target.url:
            return
        try:
            # HEAD: mantiene viva la conexión sin descargar el estado del executor.
            self._request(conns, target, "HEAD", "/api/state", None, retry=False)
        except Exception:
            pass                        # la próxima señal abrirá una conexión nueva

    def _worker(self, q: "queue.Queue[tuple]") -> None:
        conns: Dict[Tuple[str, str], http.client.HTTPConnection] = {}
        self._warm_conn(conns)          # conexión lista antes de la primera señal
        while True:
            try:
                item = q.get(timeout=EXECUTOR_WARM_S)
            except queue.Empty:
                self._warm_conn(conns)
                continue
            payload, target, t0 = item
            self._deliver(conns, payload, target, t0)

    def _request(self, conns: dict, target: ExecutorTarget, method: str, path: str,
                 body: Optional[bytes], retry: bool = True) -> Tuple[int, bytes]:
        u = urlsplit(target.url)
        key = (u.scheme, u.netloc)
        full = (u.path or "") + path
        headers = {"Content-Type": "application/json", "X-Signal-Secret": target.secret,
                   "Connection": "keep-alive"}
        for attempt in range(2):
            conn = conns.get(key)
            reused = conn is not None
            if conn is None:
                cls = http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection
                conn = cls(u.hostname, u.port, timeout=self.timeout)
                conns[key] = conn
            try:
                conn.request(method, full, body=body, headers=headers)
                resp = conn.getresponse()
                return resp.status, resp.read()
            except (http.client.HTTPException, OSError):
                conn.close()
                conns.pop(key, None)
                # Una conexión reutilizada que el servidor ya cerró falla antes de
                # entregar la petición: se reintenta UNA vez con conexión nueva.
                # Una conexión nueva que falla no se reintenta (evita duplicados).
                if attempt == 0 and reused and retry:
                    continue
                raise
        raise RuntimeError("sin conexión")

    def _deliver(self, conns: dict, payload: dict, target: ExecutorTarget, t0: float) -> bool:
        what = f"{payload.get('action')} {payload.get('symbol')} {payload.get('direction')}"
        t_send = time.perf_counter()
        try:
            status, data = self._request(conns, target, "POST", "/signal",
                                         json.dumps(payload).encode("utf-8"))
            if status >= 400:
                raise RuntimeError(f"HTTP {status}: {data[:200].decode('utf-8', 'replace')}")
        except Exception as exc:
            self.errors += 1
            self.last_error = f"{what}: {exc}"
            self._log(f"[executor] ✗ error enviando {self.last_error} → {target.url}")
            return False
        now = time.perf_counter()
        http_ms = (now - t_send) * 1000
        total_ms = (now - t0) * 1000 if t0 else http_ms
        self.sent += 1
        self.last_ok_ts = time.time()
        self.last_http_ms = http_ms
        self.last_total_ms = total_ms
        since = f" | desde el mensaje de Binance {total_ms:.0f} ms" if t0 else ""
        self._log(f"[executor] ✓ {what} → {target.url} | HTTP {http_ms:.0f} ms{since}")
        return True

    def send(self, payload: dict, t0: float = 0.0) -> None:
        """No bloquea: encola la señal con el destino vigente AHORA.
        t0 = perf_counter() del mensaje de Binance que la originó (para medir)."""
        target = self.target
        if not target.url:
            self._log(f"[executor] sin URL: no se envía {payload.get('action')} {payload.get('symbol')}")
            return
        self._ensure_workers()
        self._queues[hash(payload.get("symbol") or "") % len(self._queues)].put((payload, target, t0))

    def warm(self) -> None:
        """Arranca los hilos y abre las conexiones antes de la primera señal."""
        self._ensure_workers()

    def probe(self, url: str) -> Tuple[bool, str]:
        """GET <url>/api/state para comprobar que el Executor responde."""
        url = self.clean_url(url)
        try:
            with urllib.request.urlopen(f"{url}/api/state", timeout=self.timeout) as resp:
                return True, f"HTTP {resp.status}"
        except urllib.error.HTTPError as exc:
            return True, f"HTTP {exc.code}"      # responde, aunque no tenga /api/state
        except Exception as exc:
            return False, str(exc)


# ─────────────────────────────────────────────────────────────────────────────
# MODELO
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Position:
    trade_id:    int
    symbol:      str
    direction:   str            # LONG | SHORT
    entry_price: float
    qty:         float
    notional:    float
    tp_price:    float
    signal_pct:  float          # cuerpo de la vela que disparó la entrada
    candle_open: float
    candle_close: float
    candle_time: int            # open_time (ms) de esa vela
    opened_ts:   float = field(default_factory=time.time)
    last_price:  float = 0.0
    # ── Excursiones (MFE = máximo a favor, MAE = máximo en contra) ──
    mfe_pct:     float = 0.0    # mejor avance a favor visto (≥ 0)
    mae_pct:     float = 0.0    # peor retroceso en contra visto (≤ 0)
    mfe_usd:     float = 0.0
    mae_usd:     float = 0.0
    mfe_ts:      float = 0.0    # cuándo se marcó cada extremo
    mae_ts:      float = 0.0
    high_price:  float = 0.0    # precio máximo y mínimo vistos
    low_price:   float = 0.0
    strategy:    str = "close"  # bot que la abrió: close | live
    candle_age_s: float = 0.0   # segundos de vela transcurridos al entrar (bot en vivo)
    inverted:    bool = False   # abierta en modo invertido: tp_price es un STOP LOSS
    sent:        bool = True    # se mandó al executor (False = simulada, envíos pausados)
    # ── Solo bot de ratio: la salida es por ratio, no por precio ──
    ratio_trigger: float = 0.0  # ratio ANTES de la caída/subida: al recuperarlo se cierra (TP)
    ratio_entry:   float = 0.0  # ratio que disparó la entrada
    ratio_change_pct: float = 0.0
    ratio_ts:      int = 0      # timestamp (ms) del punto de ratio que disparó
    ratio_target:  float = 0.0  # ratio de salida (TP): recupera el mismo % que cayó/subió
    sl_price:      float = 0.0  # stop loss por precio (bot de ratio). 0 = sin SL
    revenge:       bool = False # trade de venganza (sale solo por TP/SL en USD; si pierde no encadena otro)
    rev_parent:    int = 0      # trade_id de la operación perdedora que la disparó
    rev_tp_usd:    float = 0.0  # objetivo de la venganza en USD (neto) y su stop en USD
    rev_sl_usd:    float = 0.0

    def update(self, price: float) -> float:
        """Actualiza precio y excursiones. Llamar con el lock del bot tomado."""
        self.last_price = price
        move = favorable_move_pct(price, self.entry_price, self.direction)
        now = time.time()
        if move > self.mfe_pct:
            self.mfe_pct, self.mfe_ts = move, now
            self.mfe_usd = pnl_usd(price, self.entry_price, self.qty, self.direction)
        if move < self.mae_pct:
            self.mae_pct, self.mae_ts = move, now
            self.mae_usd = pnl_usd(price, self.entry_price, self.qty, self.direction)
        if price > self.high_price:
            self.high_price = price
        if self.low_price <= 0 or price < self.low_price:
            self.low_price = price
        return move


ENGINE_CLOSE = "close"
ENGINE_LIVE  = "live"
ENGINE_RATIO = "ratio"
ENGINE_RATIO_TP = "ratio_tp"       # mismas entradas; sale SOLO por el TP del ratio (sin TP/SL de precio)
ENGINE_RATIO_REV = "ratio_rev"     # mismas entradas; TP/SL propios + trade de venganza tras una pérdida
RATIO_ENGINES = (ENGINE_RATIO, ENGINE_RATIO_TP, ENGINE_RATIO_REV)
ENGINE_LABELS = {ENGINE_CLOSE: "Bot al cierre", ENGINE_LIVE: "Bot en vivo",
                 ENGINE_RATIO: "Bot ratio TP/SL", ENGINE_RATIO_TP: "Bot ratio solo TP ratio",
                 ENGINE_RATIO_REV: "Bot ratio + venganza"}
_RATIO_ENTRY_HINT = ("SHORT cuando el ratio de los top traders CAE en un ganador (24 h ≥ +X %); LONG cuando SUBE en "
                     "un perdedor (24 h ≤ −X %)")
ENGINE_HINTS = {
    ENGINE_RATIO: _RATIO_ENTRY_HINT + ". Sale por TP/SL de precio o cuando el ratio recupera el % que se movió",
    ENGINE_RATIO_TP: _RATIO_ENTRY_HINT + ". Sale SOLO cuando el ratio recupera el % que se movió (sin TP/SL de precio)",
    ENGINE_RATIO_REV: _RATIO_ENTRY_HINT + ". TP/SL propios; si cierra en pérdida abre un trade de venganza para recuperarla",
    ENGINE_CLOSE: "entra cuando la vela 1m CIERRA con cuerpo ≥ umbral",
    ENGINE_LIVE:  "entra en cuanto la vela 1m EN CURSO supera el umbral, sin esperar al cierre",
}


class Engine:
    """Estado de UN bot. Los dos bots comparten los WebSockets, el executor y
    la web, pero cada uno tiene sus parámetros, bloqueos, posiciones, señales
    e historial. Todo se toca con el lock de KlineBot."""

    def __init__(self, name: str, cfg: dict, threshold_default: float, tp_default: float) -> None:
        self.name  = name
        self.label = ENGINE_LABELS[name]
        self.threshold_pct = float(cfg.get("threshold_pct") or threshold_default)
        self.tp_pct = float(cfg.get("tp_pct") or tp_default)
        self.blocked = {LONG: bool(cfg.get("block_long")),
                        SHORT: bool(cfg.get("block_short"))}
        # Pausado: no abre entradas nuevas; sus posiciones abiertas siguen su curso.
        self.paused = bool(cfg.get("paused"))
        # trade_id → posición. Puede haber varias por símbolo y en ambas direcciones.
        self.positions: Dict[int, Position] = {}
        self.closed_trades: List[dict] = []
        self.signals: Deque[dict] = deque(maxlen=200)
        self.cooldown: Dict[str, float] = {}
        self.total_realized_pnl = 0.0
        self.signal_count = 0
        self.opened_count = 0
        self._last_candle: Dict[str, int] = {}      # dedupe de señales por símbolo (loop del bot)
        self._decide_ms: Deque[float] = deque(maxlen=200)

    def settings(self) -> dict:
        return {"threshold_pct": self.threshold_pct, "tp_pct": self.tp_pct,
                "block_long": self.blocked[LONG], "block_short": self.blocked[SHORT],
                "paused": self.paused}


# ─────────────────────────────────────────────────────────────────────────────
# BOT
# ─────────────────────────────────────────────────────────────────────────────

async def _ws_messages(url: str):
    """Mensajes de un WebSocket (generador asíncrono). Usa `websockets`; si no
    está instalado, `aiohttp`. Termina cuando el servidor cierra la conexión."""
    try:
        import websockets                                   # type: ignore
    except ImportError:
        websockets = None
    if websockets is not None:
        async with websockets.connect(url, ping_interval=20, ping_timeout=30, max_size=None) as ws:
            async for msg in ws:
                yield msg
        return
    import aiohttp                                          # type: ignore
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url, heartbeat=20, max_msg_size=0) as ws:
            async for m in ws:
                if m.type == aiohttp.WSMsgType.TEXT:
                    yield m.data
                elif m.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break


class KlineBot:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.events: Deque[str] = deque(maxlen=200)
        self.trade_id_seq = 0
        self.last_error = ""

        self.filters: Dict[str, Dict[str, float]] = {}
        self.symbols: List[str] = []

        self.price_cache: Optional[SymbolWebSocketPriceCache] = None
        self.kline_stream: Optional[KlineWebSocketStream] = None

        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None
        self.running = False
        self.started_at = time.time()

        self._pos_syms: frozenset = frozenset()     # copia para el hilo del WS (sin lock)
        self._price_hold: set = set()               # símbolos con precio pedido antes de una entrada
        self._pending: set = set()                  # símbolos con tick de precio pendiente
        self._pending_lock = threading.Lock()
        self._wake_scheduled = False
        self._tick_event: Optional[asyncio.Event] = None
        self._closing: set = set()
        self._inflight = 0
        # Bot en vivo: vela (open_time) en la que ya disparó cada símbolo. Solo
        # lo toca el hilo del WS de velas, así cada vela genera UNA señal.
        self._live_fired: Dict[str, int] = {}
        self.live_updates = 0

        settings = self._load_settings()
        self.executor = ExecutorBridge(settings.get("executor_url", EXECUTOR_URL),
                                       settings.get("executor_secret", EXECUTOR_SECRET),
                                       logger=self.log)
        self.executor_changed_ts = float(settings.get("executor_changed_ts") or 0.0)
        # Parámetros de cada bot, editables en caliente desde la web. Los
        # ajustes de la versión de un solo bot (claves sueltas) pasan al de cierre.
        saved = settings.get("engines") if isinstance(settings.get("engines"), dict) else {}
        legacy = {k: settings.get(k) for k in ("threshold_pct", "tp_pct", "block_long", "block_short")}
        self.engines: Dict[str, Engine] = {
            ENGINE_CLOSE: Engine(ENGINE_CLOSE, saved.get(ENGINE_CLOSE) or {**legacy, "paused": True},
                                 SIGNAL_THRESHOLD_PCT, TAKE_PROFIT_PCT),
            ENGINE_LIVE:  Engine(ENGINE_LIVE, saved.get(ENGINE_LIVE) or {"paused": True},
                                 LIVE_SIGNAL_THRESHOLD_PCT, LIVE_TAKE_PROFIT_PCT),
        }
        self.live = self.engines[ENGINE_LIVE]
        # Bot de ratio: SHORT y LONG activos por defecto (RATIO_LONG_DEFAULT=false bloquea el LONG).
        for name in RATIO_ENGINES:
            self.engines[name] = Engine(name, saved.get(name) or {"block_long": not RATIO_LONG_DEFAULT}, 1.0, 1.0)
        rs = settings.get("ratio") if isinstance(settings.get("ratio"), dict) else {}

        def _pos(v, d):
            try:
                v = float(v)
                return v if v > 0 else d
            except (TypeError, ValueError):
                return d
        def _nonneg(v, d):
            try:
                return abs(float(v))
            except (TypeError, ValueError):
                return d
        self.ratio_cfg = {"watch_min_pct": _pos(rs.get("watch_min_pct"), RATIO_WATCH_MIN_PCT),
                          "drop_min_pct":  _pos(rs.get("drop_min_pct"), RATIO_DROP_MIN_PCT),
                          "drop_max_pct":  _pos(rs.get("drop_max_pct"), RATIO_DROP_MAX_PCT),
                          "tp_pct":        _nonneg(rs.get("tp_pct"), RATIO_TP_PCT),
                          "sl_pct":        _nonneg(rs.get("sl_pct"), RATIO_SL_PCT),
                          "ratio_exit":    bool(rs.get("ratio_exit", RATIO_EXIT_BY_RATIO))}
        if self.ratio_cfg["drop_min_pct"] >= self.ratio_cfg["drop_max_pct"]:
            self.ratio_cfg["drop_min_pct"], self.ratio_cfg["drop_max_pct"] = RATIO_DROP_MIN_PCT, RATIO_DROP_MAX_PCT
        # Bot ratio + venganza: su salida (TP/SL/ratio) y la venganza; la entrada es la común (ratio_cfg).
        rv = settings.get("ratio_rev") if isinstance(settings.get("ratio_rev"), dict) else {}
        self.rev_cfg = {"tp_pct":          _nonneg(rv.get("tp_pct"), RATIO_TP_PCT),
                        "sl_pct":          _nonneg(rv.get("sl_pct"), RATIO_SL_PCT),
                        "ratio_exit":      bool(rv.get("ratio_exit", RATIO_EXIT_BY_RATIO)),
                        "revenge":         bool(rv.get("revenge", REVENGE_ENABLED)),
                        "rev_mult":        _pos(rv.get("rev_mult"), REVENGE_MULT),
                        "rev_recover_pct": _pos(rv.get("rev_recover_pct"), REVENGE_RECOVER_PCT),
                        "rev_sl_pct":      _pos(rv.get("rev_sl_pct"), REVENGE_SL_PCT),
                        "rev_dir":         rv.get("rev_dir") if rv.get("rev_dir") in ("opposite", "same") else
                                           (REVENGE_DIR if REVENGE_DIR in ("opposite", "same") else "opposite")}
        self.revenge_count = 0
        # Estado del bot de ratio (se toca solo desde el loop del bot).
        self._tick: Dict[str, Tuple[float, float, float]] = {}     # símbolo → (24 h %, último precio, ts)
        self.ticker_connected = False
        self.ticker_last_ts = 0.0
        self.ticker_error = ""
        self.watch: List[str] = []
        self.watch_side: Dict[str, str] = {}
        self.watch_ts = 0.0
        self._ratio: Dict[str, dict] = {}                          # símbolo → último estado del ratio
        self.ratio_polls = 0
        self.ratio_errors = 0
        self.ratio_last_error = ""
        self.ratio_last_cycle_ts = 0.0
        self.ratio_next_cycle_ts = 0.0
        self._rest_block_until = 0.0
        self._rest_route = 0
        self._rest_times: Deque[float] = deque()      # peticiones de los últimos 5 min
        self._scan_seen: Optional[set] = None         # símbolos vistos en la exploración en curso
        self._filters_ts = 0.0                        # cuándo se obtuvieron los filtros de cantidad
        self._load_known_symbols()
        # Tráfico de entrada propio (bytes): REST a Binance y snapshots/stream del 24 h.
        self.rx_rest_bytes = 0
        self.rx_ticker_bytes = 0
        self.tx_web_bytes = 0                         # respuestas de la web (comprimidas)
        # Ajustes comunes a los dos bots (botón y campos de la web).
        self.inverted = bool(settings.get("inverted", INVERT_SIGNALS))
        self.fee_pct = float(settings["fee_pct"]) if settings.get("fee_pct") is not None else FEE_PCT
        self.profit_target_usd = (float(settings["profit_target_usd"])
                                  if settings.get("profit_target_usd") is not None else PROFIT_TARGET_USD)
        # Objetivo global: PnL realizado acumulado en el último reinicio del contador.
        # Modo prueba: los bots siguen operando en simulación pero NO mandan
        # entradas nuevas al executor (los cierres de lo ya enviado sí salen).
        self.executor_paused = bool(settings.get("executor_paused"))
        self.klines_enabled = bool(settings.get("klines_enabled", KLINE_BOTS_ENABLED))
        self.max_open = (int(settings["max_open"]) if settings.get("max_open") is not None
                         else MAX_OPEN_POSITIONS)
        self.session_base = 0.0
        self.session_started_ts = time.time()
        self.target_hits = 0
        self.last_target = ""
        self._target_closing = False

        # Evaluaciones: cada cierre de vela 1m se mide, pase o no el umbral.
        # minuto (open_time ms) → [(cuerpo %, símbolo, apertura, cierre), ...]
        self._evals: Dict[int, List[Tuple[float, str, float, float]]] = {}
        self._evals_lock = threading.Lock()
        self.eval_count = 0

        # Upstash desactivado por ahora: solo copia local (backend=None).
        self._store_desc = f"solo disco local ({RECOVERY_FILE})"
        self.store = StateStore(
            None, RECOVERY_FILE, make_owner_id(),
            build_doc=self._recovery_doc, log=self.log, on_fenced=self._on_fenced,
            key_prefix=STATE_KEY_PREFIX, max_trades=STATE_TRADES_MAX,
        )

    # ── Utilidades ────────────────────────────────────────────────────────────

    def log(self, msg: str) -> None:
        line = f"{_utc(time.time())} | {msg}"
        print(line, flush=True)
        with self.lock:
            self.events.appendleft(line)

    def engine(self, name: Optional[str]) -> Optional[Engine]:
        return self.engines.get(str(name or ENGINE_CLOSE).lower())

    def _next_trade_id(self) -> int:
        with self.lock:
            # Arranca en la hora Unix para no repetir ids que el Executor ya vio
            # de versiones anteriores del bot. Secuencia común a los dos bots.
            self.trade_id_seq = max(self.trade_id_seq + 1, int(time.time()))
            return self.trade_id_seq

    def _refresh_pos_syms(self, force: bool = False) -> None:
        """Actualiza los símbolos con posición y alinea con ellos las suscripciones
        de precio en vivo (se dan de baja al cerrar: nada de streams olvidados)."""
        with self.lock:
            syms = frozenset(p.symbol for e in self.engines.values() for p in e.positions.values())
            changed = syms != self._pos_syms
            self._pos_syms = syms
            wanted = syms | self._price_hold
        pc = self.price_cache
        if (changed or force) and pc is not None:
            pc.update_symbols(sorted(wanted))

    def _find(self, trade_id: int) -> Tuple[Optional[Engine], Optional[Position]]:
        with self.lock:
            for eng in self.engines.values():
                pos = eng.positions.get(trade_id)
                if pos is not None:
                    return eng, pos
        return None, None

    # ── Ajustes locales (link del executor y parámetros) ──────────────────────

    @staticmethod
    def _load_settings() -> dict:
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save_settings(self) -> None:
        t = self.executor.target
        data = {"executor_url": t.url, "executor_secret": t.secret,
                "executor_changed_ts": self.executor_changed_ts,
                "engines": {n: e.settings() for n, e in self.engines.items()},
                "inverted": self.inverted, "fee_pct": self.fee_pct,
                "profit_target_usd": self.profit_target_usd, "max_open": self.max_open,
                "executor_paused": self.executor_paused, "ratio": self.ratio_cfg, "ratio_rev": self.rev_cfg,
                "klines_enabled": self.klines_enabled}
        tmp = f"{SETTINGS_FILE}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh)
            os.replace(tmp, SETTINGS_FILE)
        except Exception as exc:
            self.log(f"No pude guardar ajustes locales: {exc}")

    def set_config(self, eng: Engine, threshold_pct: Optional[float] = None,
                   tp_pct: Optional[float] = None) -> dict:
        """Cambia en caliente el % de la señal y/o el % del TP de un bot. El
        umbral aplica a la siguiente señal; el TP también a sus posiciones abiertas."""
        changes = []
        if threshold_pct is not None and threshold_pct != eng.threshold_pct:
            changes.append(f"señal {eng.threshold_pct:g}% → {threshold_pct:g}%")
            eng.threshold_pct = threshold_pct
        if tp_pct is not None and tp_pct != eng.tp_pct:
            changes.append(f"TP {eng.tp_pct:g}% → {tp_pct:g}%")
            eng.tp_pct = tp_pct
            with self.lock:
                for p in eng.positions.values():
                    p.tp_price = exit_price(p.entry_price, p.direction, tp_pct, p.inverted)
                syms = {p.symbol for p in eng.positions.values()}
            for sym in syms:
                self._on_price_event(sym, "config")      # revisa ya el TP nuevo
        if changes:
            self._save_settings()
            self.store.mark_dirty(True)
            self.log(f"[config] {eng.label}: " + " | ".join(changes))
        return {"engine": eng.name, "threshold_pct": eng.threshold_pct, "tp_pct": eng.tp_pct}

    def _start_klines(self) -> bool:
        """Abre el WebSocket de velas 1m de todos los símbolos (bots al cierre y en vivo)."""
        if self.kline_stream is not None:
            return True
        if not self.symbols:
            self.log("[velas] sin lista de símbolos todavía: no se pueden abrir las velas")
            return False
        self.kline_stream = KlineWebSocketStream(
            self.symbols, intervals=(KLINE_INTERVAL,), history=KLINE_HISTORY,
            dtype=KLINE_DTYPE, on_close=self._on_kline_close, on_update=self._on_kline_live,
        )
        self.kline_stream.start()
        self._set_price_stream()
        self.log(f"Velas 1m por WebSocket: {len(self.symbols)} símbolos, "
                 f"{KLINE_HISTORY} velas cerradas por símbolo + vela en curso para el bot en vivo")
        return True

    def _stop_klines(self) -> None:
        ks, self.kline_stream = self.kline_stream, None
        if ks is not None:
            ks.stop()
            self.log("Velas 1m cerradas: los bots al cierre y en vivo no abren entradas nuevas")
        self._set_price_stream()

    def _set_price_stream(self) -> None:
        """bookTicker (tiempo real) con los bots de velas; aggTrade (≤100 ms) si solo operan los de ratio."""
        pc = self.price_cache
        want = price_stream_for(self.kline_stream is not None)
        if pc is not None and pc.symbol_stream != want:
            pc.symbol_stream = want
            pc._schedule_sync()

    def set_klines(self, enabled: bool) -> bool:
        """Enciende o apaga en caliente el stream de velas (mucho tráfico: ~0,5 MB/s)."""
        if enabled == self.klines_enabled and (self.kline_stream is not None) == enabled:
            return enabled
        if enabled:
            if self.loop is None:
                return False
            ok = self._start_klines()
            if not ok:
                return False
        else:
            self._stop_klines()
        self.klines_enabled = enabled
        self._save_settings()
        return enabled

    def set_global(self, inverted: Optional[bool] = None, fee_pct: Optional[float] = None,
                   profit_target_usd: Optional[float] = None, max_open: Optional[int] = None,
                   executor_paused: Optional[bool] = None) -> dict:
        """Ajustes comunes a los dos bots. Invertir aplica a las entradas
        nuevas; las abiertas siguen con el modo (TP o SL) con el que se abrieron."""
        changes = []
        if inverted is not None and inverted != self.inverted:
            self.inverted = inverted
            changes.append("modo INVERTIDO (contra la vela, % = stop loss, sin TP)" if inverted
                           else "modo NORMAL (sigue la vela, % = take profit)")
        if fee_pct is not None and fee_pct != self.fee_pct:
            changes.append(f"fee {self.fee_pct:g}% → {fee_pct:g}% por lado")
            self.fee_pct = fee_pct
        if profit_target_usd is not None and profit_target_usd != self.profit_target_usd:
            changes.append(f"objetivo {self.profit_target_usd:g} → {profit_target_usd:g} USD")
            self.profit_target_usd = profit_target_usd
        if max_open is not None and max_open != self.max_open:
            changes.append(f"límite de abiertas {self.max_open or 'sin límite'} → {max_open or 'sin límite'}")
            self.max_open = max_open
        if executor_paused is not None and executor_paused != self.executor_paused:
            self.executor_paused = executor_paused
            changes.append("envíos al executor PAUSADOS (entradas nuevas solo simuladas)" if executor_paused
                           else "envíos al executor reanudados")
        if changes:
            self._save_settings()
            self.store.mark_dirty(True)
            self.log("[config] ambos bots: " + " | ".join(changes))
        return self.global_settings()

    def global_settings(self) -> dict:
        return {"inverted": self.inverted, "fee_pct": self.fee_pct, "klines_enabled": self.klines_enabled,
                "profit_target_usd": self.profit_target_usd, "max_open": self.max_open,
                "executor_paused": self.executor_paused}

    def open_count(self) -> int:
        with self.lock:
            return sum(len(e.positions) for e in self.engines.values())

    def realized_total(self) -> float:
        with self.lock:
            return sum(e.total_realized_pnl for e in self.engines.values())

    def _net_pnl(self, p: Position, price: float) -> float:
        return net_pnl_usd(price, p.entry_price, p.qty, p.direction, self.fee_pct)

    def session_pnl(self) -> Tuple[float, float]:
        """(realizado, abierto) desde el último objetivo o reinicio, netos de fees."""
        with self.lock:
            unreal = sum(self._net_pnl(p, self._price(p.symbol) or p.last_price)
                         for e in self.engines.values() for p in e.positions.values())
        return self.realized_total() - self.session_base, unreal

    def reset_session(self, why: str = "manual") -> None:
        self.session_base = self.realized_total()
        self.session_started_ts = time.time()
        self.store.mark_dirty(True)
        self.log(f"[objetivo] contador de la sesión a cero ({why})")

    def reset_realized(self, eng: Engine) -> dict:
        """Pone a cero el PnL realizado de UN bot (el historial no se borra). El contador
        del objetivo global no cambia: la base de la sesión se corrige en lo mismo."""
        with self.lock:
            old = eng.total_realized_pnl
            eng.total_realized_pnl = 0.0
            self.session_base -= old
        self.store.mark_dirty(True)
        self.log(f"[config] {eng.label}: PnL realizado a cero (era {old:+.4f} USDT)")
        return {"engine": eng.name, "realized_pnl": 0.0, "was": old}

    def _check_profit_target(self) -> None:
        """Cierra TODAS las posiciones de los dos bots cuando realizado + abierto
        (desde el último objetivo) llega al objetivo en USD."""
        target = self.profit_target_usd
        if target <= 0 or self._target_closing or not self.store.can_act():
            return
        realized, unreal = self.session_pnl()
        if realized + unreal < target:
            return
        self._target_closing = True
        try:
            with self.lock:
                targets = [p for e in self.engines.values() for p in e.positions.values()]
            self.log(f"[objetivo] 🎯 realizado {realized:+.4f} + abierto {unreal:+.4f} = "
                     f"{realized + unreal:+.4f} USD ≥ {target:g}: cierro {len(targets)} posición(es)")
            for pos in targets:
                price = self._price(pos.symbol) or pos.last_price or pos.entry_price
                self._close(pos.trade_id, price, "OBJETIVO")
            self.target_hits += 1
            final = self.realized_total() - self.session_base
            self.last_target = f"{_utc(time.time(), '%m-%d %H:%M:%S')} · {final:+.4f} USD"
            self.reset_session(f"objetivo alcanzado con {final:+.4f} USD")
        finally:
            self._target_closing = False

    def set_paused(self, eng: Engine, paused: bool) -> dict:
        """Pausa o reanuda un bot: pausado no abre entradas nuevas; sus
        posiciones abiertas siguen hasta su TP/SL o cierre manual."""
        if eng.paused != paused:
            eng.paused = paused
            self._save_settings()
            self.log(f"[config] {eng.label}: {'PAUSADO' if paused else 'en marcha'}")
        return {"engine": eng.name, "paused": eng.paused}

    def close_all(self, eng: Optional[Engine] = None, reason: str = "CERRAR_TODAS") -> Tuple[int, int]:
        """Cierra todas las posiciones (de un bot o de todos). (cerradas, total)."""
        engines = [eng] if eng is not None else list(self.engines.values())
        with self.lock:
            targets = [p for e in engines for p in e.positions.values()]
        closed = 0
        for pos in targets:
            sym = pos.symbol
            price = self._price(sym) or (self.price_cache.get_mark_price(sym) if self.price_cache else None) \
                or pos.last_price or pos.entry_price
            closed += self._close(pos.trade_id, price, reason)
        if targets:
            self.log(f"[cierre] {'todas' if eng is None else eng.label}: {closed}/{len(targets)} cerradas ({reason})")
        return closed, len(targets)

    def set_block(self, eng: Engine, direction: str, blocked: bool) -> dict:
        """Bloquea o desbloquea las entradas nuevas de una dirección en un bot.
        Las posiciones ya abiertas siguen su curso (TP o cierre manual)."""
        if eng.blocked[direction] != blocked:
            eng.blocked[direction] = blocked
            self._save_settings()
            self.log(f"[config] {eng.label} · {direction}: {'BLOQUEADOS' if blocked else 'permitidos'}")
        return {"engine": eng.name, "block_long": eng.blocked[LONG],
                "block_short": eng.blocked[SHORT]}

    def set_executor(self, url: str, secret: Optional[str] = None) -> ExecutorTarget:
        """Cambia el destino de las señales en caliente (sin reiniciar)."""
        old = self.executor.target.url
        target = self.executor.set_target(url, secret)
        self.executor_changed_ts = time.time()
        self._save_settings()
        self.store.mark_dirty(True)
        self.log(f"[executor] link cambiado: {old or '(vacío)'} → {target.url or '(vacío)'}"
                 + (" (secreto actualizado)" if secret is not None else ""))
        return target

    # ── Arranque / parada ─────────────────────────────────────────────────────

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True, name="BotLoop")
        self.thread.start()

    def _run_loop(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._tick_event = asyncio.Event()
        try:
            self.loop.run_until_complete(self._main())
        except Exception as exc:
            self.running = False
            self.last_error = str(exc)
            self.log(f"Bot detenido por error no controlado: {exc!r}")

    async def _main(self) -> None:
        for eng in self.engines.values():
            if eng.name in RATIO_ENGINES or not self.klines_enabled:
                continue
            self.log(f"{eng.label} | {ENGINE_HINTS[eng.name]} | señal ≥ {eng.threshold_pct:g}% | "
                     f"{'SL' if self.inverted else 'TP'} {eng.tp_pct:g}% | {NOTIONAL_USDT:g} USDT")
        self.log(f"Modo {'INVERTIDO (contra la vela, sin TP)' if self.inverted else 'normal (sigue la vela, sin SL)'} | "
                 f"fee {self.fee_pct:g}% por lado | objetivo global "
                 f"{f'+{self.profit_target_usd:g} USD' if self.profit_target_usd > 0 else 'apagado'}")
        self.log(f"Executor → {self.executor.target.url or '(vacío)'}")
        rc = self.ratio_cfg
        self.log(f"Bot ratio top traders | vigila 24 h ≥ ±{rc['watch_min_pct']:g}% | caída (SHORT) o subida (LONG) "
                 f"del ratio {rc['drop_min_pct']:g}–{rc['drop_max_pct']:g}% | SHORT "
                 f"{'bloqueado' if self.engines[ENGINE_RATIO].blocked[SHORT] else 'activo'} | LONG "
                 f"{'bloqueado' if self.engines[ENGINE_RATIO].blocked[LONG] else 'activo'} | "
                 f"{NOTIONAL_USDT:g} USDT | TP +{rc['tp_pct']:g}% · SL -{rc['sl_pct']:g}% por precio"
                 f"{' · TP por ratio (recupera el % que se movió)' if rc['ratio_exit'] else ''}")
        self.log(f"{ENGINE_LABELS[ENGINE_RATIO_TP]} | sale solo por el TP del ratio")
        self.log(f"{ENGINE_LABELS[ENGINE_RATIO_REV]} | {self._rev_desc()}")
        self.log(f"Red: 24 h por {f'WebSocket, exploración de {TICKER_SCAN_S:g} s cada 5 min' if TICKER_MODE != 'ws' else 'WebSocket continuo'} · "
                 f"precio en vivo: con posición {price_stream_for(self.klines_enabled)}"
                 f"{', vigilados markPrice cada 3 s' if WATCH_LIVE_PRICE else ''}")
        self.log(f"[estado] Persistencia: {self._store_desc}")
        self.store.start()
        self.executor.warm()
        await self._init_symbols()

        # El estado (posiciones abiertas) se restaura ANTES de abrir los WebSockets.
        if await asyncio.to_thread(self.store.acquired.wait, STATE_BOOT_WAIT_S):
            self._restore_from_store()
        else:
            self.log("[estado] ⚠️ Aún no recupero el estado local: sin entradas ni cierres hasta recuperarlo")

        self.price_cache = SymbolWebSocketPriceCache(sorted(self._pos_syms),
                                                     symbol_stream=price_stream_for(self.klines_enabled))
        self.price_cache.set_update_callback(self._on_price_event)
        self.price_cache.start()

        if self.klines_enabled:
            self._start_klines()
        else:
            self.log("Velas 1m apagadas: los bots al cierre y en vivo no reciben señales "
                     "(se encienden desde la web, bloque Bots)")

        await asyncio.gather(
            self._supervised(self._engine_loop, "engine"),
            self._supervised(self._maintenance_loop, "maintenance"),
            *([self._supervised(self._ticker_loop, "ticker")] if TICKER_MODE == "ws" else []),
            self._supervised(self._ratio_loop, "ratio"),
            self._supervised(self._symbols_retry_loop, "symbols"),
        )

    async def _supervised(self, factory, name: str) -> None:
        while self.running:
            try:
                await factory()
            except asyncio.CancelledError:
                if not self.running:
                    break
            except Exception as exc:
                self.last_error = f"{name}: {exc!r}"
                self.log(f"[supervisor] {name} falló: {exc!r} — reinicio en 2 s")
            await asyncio.sleep(2.0)

    def shutdown(self) -> None:
        """Apagado ordenado (atexit al recibir SIGTERM): deja de operar, espera
        las órdenes en vuelo, guarda por última vez y libera el control."""
        self.store.begin_stop()
        deadline = time.time() + SHUTDOWN_DRAIN_S
        while time.time() < deadline and (self._inflight or self._closing):
            time.sleep(0.1)
        if self.store.close():
            print("[estado] guardado final enviado", flush=True)

    # ── Símbolos y filtros (UNA petición REST como mucho) ─────────────────────

    def _fetch_exchange_info(self) -> Dict[str, Dict[str, float]]:
        routes = [("proxy", REST_PROXY_URL), ("IP directa", "")] if REST_PROXY_URL else [("IP directa", "")]
        last_exc: Optional[Exception] = None
        for label, proxy in routes:
            try:
                data, nbytes = http_get_json(f"{BASE_URL}/fapi/v1/exchangeInfo", proxy, 20.0)
                self.rx_rest_bytes += nbytes
                self.log(f"REST exchangeInfo por {label} ({nbytes / 1024:.0f} KB comprimido)")
                break
            except Exception as exc:
                last_exc = exc
                self.log(f"[REST] exchangeInfo por {label} falló: {exc}")
        else:
            raise RuntimeError(f"exchangeInfo falló: {last_exc}")
        out: Dict[str, Dict[str, float]] = {}
        for s in data.get("symbols", []):
            if (s.get("quoteAsset") != QUOTE_ASSET or s.get("contractType") != "PERPETUAL"
                    or s.get("status") != "TRADING"):
                continue
            row = {"stepSize": 0.001, "minQty": 0.0, "minNotional": 5.0}
            for f in s.get("filters", []):
                if f.get("filterType") == "LOT_SIZE":
                    row["stepSize"] = float(f.get("stepSize", row["stepSize"]))
                    row["minQty"] = float(f.get("minQty", row["minQty"]))
                elif f.get("filterType") == "MIN_NOTIONAL":
                    row["minNotional"] = float(f.get("notional", row["minNotional"]))
            out[s["symbol"]] = row
        return out

    async def _init_symbols(self) -> None:
        """Filtros de cantidad (stepSize, mínimos) de exchangeInfo, cacheados en disco
        SYMBOL_REFRESH_HOURS (10 días). Es la única petición REST aparte de la del ratio."""
        cached = None
        try:
            with open(SYMBOLS_CACHE_FILE, "r", encoding="utf-8") as fh:
                cached = json.load(fh)
        except Exception:
            pass
        age_h = (time.time() - float(cached.get("saved_at", 0))) / 3600 if cached else None
        if cached and cached.get("filters") and age_h is not None and age_h < SYMBOL_REFRESH_HOURS:
            self.filters = dict(cached["filters"])
            self._filters_ts = float(cached.get("saved_at", 0))
            self.log(f"Filtros de cantidad desde caché en disco: {len(self.filters)} (hace {age_h / 24:.1f} días)")
        else:
            try:
                self.filters = await asyncio.to_thread(self._fetch_exchange_info)
                self._filters_ts = time.time()
                tmp = f"{SYMBOLS_CACHE_FILE}.tmp"
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump({"symbols": sorted(self.filters), "filters": self.filters,
                               "saved_at": time.time()}, fh)
                os.replace(tmp, SYMBOLS_CACHE_FILE)
            except Exception as exc:
                self.last_error = str(exc)
                if cached and cached.get("filters"):
                    self.filters = dict(cached["filters"])
                    self._filters_ts = time.time() - max(SYMBOL_REFRESH_HOURS - 1, 0) * 3600   # reintento en 1 h
                    self.log(f"Usando caché viejo de símbolos ({len(self.filters)})")
                elif self.known_symbols:
                    self.log(f"Sin filtros de cantidad: uso los {len(self.known_symbols)} símbolos "
                             "descubiertos por WebSocket (las entradas esperan a tener filtros)")
                elif INITIAL_SYMBOLS:
                    self.filters = {s: {"stepSize": 0.001, "minQty": 0.0, "minNotional": 5.0}
                                    for s in INITIAL_SYMBOLS}
                    self.log(f"Usando INITIAL_SYMBOLS ({len(self.filters)}) sin filtros reales")
                else:
                    self.log("⚠️ Sin lista de símbolos: el bot no puede suscribir velas")
        self.symbols = sorted(self.filters) or sorted(self.known_symbols)
        self.log(f"{len(self.symbols)} perpetuos {QUOTE_ASSET} operables")

    # ── Entradas (hilo del WS de velas: solo filtrar y pasar al loop) ─────────

    def _on_kline_live(self, symbol: str, interval: str, candle: Candle) -> None:
        """Vela EN CURSO (cada ~250 ms por símbolo) → bot en vivo. Dispara en
        cuanto el cuerpo supera el umbral, una sola vez por vela."""
        self.live_updates += 1
        o = candle.open
        if o <= 0:
            return
        thr = self.live.threshold_pct
        body = (candle.close - o) / o * 100.0
        if -thr < body < thr or self._live_fired.get(symbol) == candle.open_time:
            return
        self._live_fired[symbol] = candle.open_time
        loop = self.loop
        if loop is not None:
            inv = self.inverted
            direction = LONG if (body >= thr) != inv else SHORT
            loop.call_soon_threadsafe(self._handle_signal, self.live, symbol, direction, candle,
                                      time.time(), time.perf_counter(), inv)

    def _on_kline_close(self, symbol: str, interval: str, candle: Candle) -> None:
        """Cierre de vela → bot al cierre. El bot en vivo también lo mira por si
        el umbral se cruzó justo en el último tick (si ya disparó, no repite)."""
        self._record_eval(symbol, candle)
        loop = self.loop
        if loop is None:
            return
        arrived, t0 = time.time(), time.perf_counter()
        inv = self.inverted
        for eng in (self.engines[ENGINE_CLOSE], self.live):     # el de ratio no opera por velas
            direction = entry_direction(candle.open, candle.close, eng.threshold_pct, inv)
            if direction is None:
                continue
            if eng is self.live:
                if self._live_fired.get(symbol) == candle.open_time:
                    continue
                self._live_fired[symbol] = candle.open_time
            loop.call_soon_threadsafe(self._handle_signal, eng, symbol, direction, candle, arrived, t0, inv)

    def _record_eval(self, symbol: str, candle: Candle) -> None:
        body = body_change_pct(candle.open, candle.close)
        with self._evals_lock:
            self.eval_count += 1
            rows = self._evals.get(candle.open_time)
            if rows is None:
                rows = self._evals[candle.open_time] = []
                for old in sorted(self._evals)[:-EVAL_MINUTES_KEPT]:
                    del self._evals[old]
            rows.append((body, symbol, candle.open, candle.close))

    def evaluations(self, thr: float) -> dict:
        """Resumen por minuto y los mayores cuerpos del último minuto cerrado,
        contados con el umbral `thr` (el de cada bot)."""
        with self._evals_lock:
            buckets = [(t, list(rows)) for t, rows in sorted(self._evals.items(), reverse=True)]
        minutes = []
        for t, rows in buckets:
            bodies = [r[0] for r in rows]
            minutes.append({
                "minute":  _utc(t / 1000, "%H:%M"),
                "evaluated": len(rows),
                "long":    sum(1 for b in bodies if b >= thr),
                "short":   sum(1 for b in bodies if b <= -thr),
                "max_up":  max(bodies) if bodies else 0.0,
                "max_down": min(bodies) if bodies else 0.0,
            })
        top = []
        if buckets:
            t, rows = buckets[0]
            for body, sym, o, c in sorted(rows, key=lambda r: abs(r[0]), reverse=True)[:EVAL_TOP_N]:
                top.append({"symbol": sym, "body_pct": body, "open": o, "close": c,
                            "signal": signal_direction(o, c, thr) or ""})
        return {"total": self.eval_count, "minutes": minutes, "top": top,
                "top_minute": minutes[0]["minute"] if minutes else ""}

    def _record_signal(self, eng: Engine, symbol: str, direction: str, candle: Candle,
                       action: str, candle_age_s: float) -> None:
        now = time.time()
        with self.lock:
            eng.signals.appendleft({
                "ts":        now,
                "time":      _utc(now, "%H:%M:%S"),
                "symbol":    symbol,
                "direction": direction,
                "body_pct":  body_change_pct(candle.open, candle.close),
                "open":      candle.open,
                "close":     candle.close,
                "candle":    _utc(candle.open_time / 1000, "%H:%M"),
                "candle_age_s": candle_age_s,
                "action":    action,
            })

    def _handle_signal(self, eng: Engine, symbol: str, direction: str, candle: Candle,
                       arrived: float, t0: float = 0.0, inverted: bool = False) -> None:
        """Corre en el loop del bot. Decide si `eng` abre y lanza la apertura.
        `direction` ya viene invertida si `inverted` (modo con el que se decidió)."""
        if eng.name in RATIO_ENGINES or eng._last_candle.get(symbol, -1) >= candle.open_time:
            return                                          # duplicado tras reconexión
        eng._last_candle[symbol] = candle.open_time
        eng.signal_count += 1
        body = body_change_pct(candle.open, candle.close)
        # Segundos de vela transcurridos (60 = al cierre).
        age = min(max(arrived - candle.open_time / 1000, 0.0), (candle.close_time + 1 - candle.open_time) / 1000)

        def skip(action: str) -> None:
            self._record_signal(eng, symbol, direction, candle, action, age)

        delay = arrived - candle.close_time / 1000          # negativo con la vela abierta
        if delay > SIGNAL_MAX_DELAY_S:
            return skip(f"ignorada: llegó {delay:.0f}s tarde")
        if eng.paused:
            return skip("pausado")
        if eng.blocked.get(direction):
            return skip(f"bloqueada: {direction} desactivados")
        if self.max_open > 0 and self.open_count() >= self.max_open:
            return skip(f"ignorada: límite de {self.max_open} abiertas")
        if not self.store.can_open():
            return skip("ignorada: estado sin confirmar")
        # Sin límite por símbolo: cada señal abre su propia posición aunque ya
        # haya otras abiertas en ese par, en la misma o en la otra dirección.
        with self.lock:
            until = eng.cooldown.get(symbol, 0.0)
        if until > time.time():
            return skip("ignorada: en pausa tras cierre")

        # El último precio de la vela es el último trade: más fresco que el mark
        # price (1-3 s) que hay para los símbolos sin bookTicker suscrito.
        price = candle.close
        f = self.filters.get(symbol, {})
        qty, eff, why = size_order(
            NOTIONAL_USDT, price, f.get("stepSize", 0.001), f.get("minQty", 0.0),
            f.get("minNotional", 5.0), MAX_NOTIONAL_OVER)
        if qty <= 0:
            self.log(f"[{eng.label}] señal {direction} {symbol} ({body:+.2f}%) saltada: {why}")
            return skip(f"saltada: {why}")

        pos = Position(
            trade_id=self._next_trade_id(), symbol=symbol, direction=direction,
            entry_price=price, qty=qty, notional=eff,
            tp_price=exit_price(price, direction, eng.tp_pct, inverted),
            signal_pct=body, candle_open=candle.open, candle_close=candle.close,
            candle_time=candle.open_time, last_price=price,
            strategy=eng.name, candle_age_s=round(age, 2), inverted=inverted,
            sent=not self.executor_paused,
        )
        with self.lock:
            eng.positions[pos.trade_id] = pos
            eng.opened_count += 1
        self._refresh_pos_syms()                            # suscribe el precio en vivo del par
        self.store.mark_dirty(True)
        decide_ms = (time.perf_counter() - t0) * 1000 if t0 else 0.0
        skip(f"abierta #{pos.trade_id} en {decide_ms:.1f} ms")
        kind = "SL" if inverted else "TP"
        self.log(f"[{eng.label}] ABRE {direction} {symbol}{' (invertido)' if inverted else ''}"
                 f"{'' if pos.sent else ' [SIMULADA: envíos al executor pausados]'} | vela {candle.open:g}→{candle.close:g} "
                 f"({body:+.2f}% a los {age:.0f}s) | px={price:g} qty={qty:g} ({eff:.2f} USDT) | "
                 f"{kind} {pos.tp_price:g} | trade #{pos.trade_id}")
        if pos.sent:
            self.executor.send({
                "action":    "open",
                "trade_id":  pos.trade_id,
                "symbol":    symbol,
                "direction": direction,
                "price":     price,
                "quantity":  qty,
                "notional":  round(eff, 4),
                "level":     round(body, 4),
                "strategy":  eng.name,
            }, t0)
        eng._decide_ms.append(decide_ms)

    # ── Salidas: ticks de precio (hilo del WS de precios) ─────────────────────

    def _on_price_event(self, symbol: str, kind: str) -> None:
        if symbol not in self._pos_syms:
            return
        with self._pending_lock:
            self._pending.add(symbol)
            if self._wake_scheduled:
                return
            self._wake_scheduled = True
        loop, ev = self.loop, self._tick_event
        if loop is None or ev is None:
            with self._pending_lock:
                self._wake_scheduled = False
            return
        loop.call_soon_threadsafe(ev.set)

    async def _engine_loop(self) -> None:
        ev = self._tick_event
        while self.running:
            await ev.wait()
            ev.clear()
            with self._pending_lock:
                batch, self._pending = self._pending, set()
                self._wake_scheduled = False
            for sym in batch:
                self._check_take_profit(sym)
            self._check_profit_target()
            await asyncio.sleep(0)

    def _price(self, symbol: str) -> Optional[float]:
        pc = self.price_cache
        if pc is None:
            return None
        return pc.get_price(symbol, max_age_s=PRICE_MAX_AGE_S)

    def _check_take_profit(self, symbol: str) -> None:
        """Revisa la salida (TP/SL por precio) de TODAS las posiciones abiertas del
        símbolo, de todos los bots."""
        price = self._price(symbol)
        if not price:
            return
        hits = []
        with self.lock:
            for eng in self.engines.values():
                for tid, pos in eng.positions.items():
                    if pos.symbol != symbol:
                        continue
                    pos.update(price)
                    if pos.strategy in RATIO_ENGINES:
                        # TP/SL fijos por precio (la salida por ratio se revisa en el ciclo de 5 min)
                        reason = price_exit(price, pos.direction, pos.tp_price, pos.sl_price)
                        if reason:
                            hits.append((tid, reason))
                        continue
                    if exit_hit(price, pos.tp_price, pos.direction, pos.inverted):
                        hits.append((tid, "SL" if pos.inverted else "TP"))
        if hits and self.store.can_act():
            for tid, reason in hits:
                self._close(tid, price, reason)

    def _close(self, trade_id: int, price: float, reason: str) -> bool:
        with self.lock:
            eng, pos = self._find(trade_id)
            if pos is None or trade_id in self._closing:
                return False
            self._closing.add(trade_id)
        symbol = pos.symbol
        try:
            with self.lock:
                pos = eng.positions.pop(trade_id, None)
                if pos is None:
                    return False
                pos.update(price)
                gross = pnl_usd(price, pos.entry_price, pos.qty, pos.direction)
                fees = fees_usd(pos.entry_price, price, pos.qty, self.fee_pct)
                pnl = gross - fees                          # neto: fees de entrada y salida
                now = time.time()
                rec = {
                    **asdict(pos),
                    "close_price": price,
                    "closed_at_ts": now,
                    "duration_s": now - pos.opened_ts,
                    "move_pct": favorable_move_pct(price, pos.entry_price, pos.direction),
                    "pnl": pnl,
                    "pnl_gross": gross,
                    "fees": fees,
                    "fee_pct": self.fee_pct,
                    "pnl_pct": pnl / pos.notional * 100.0 if pos.notional else 0.0,
                    "reason": reason,
                    "time_to_mfe_s": (pos.mfe_ts - pos.opened_ts) if pos.mfe_ts else 0.0,
                    "time_to_mae_s": (pos.mae_ts - pos.opened_ts) if pos.mae_ts else 0.0,
                }
                # El executor suma en UNA posición las de todos los bots en el mismo par y
                # dirección: si queda otra enviada, se cierra solo la cantidad de esta.
                shared = pos.sent and any(p.sent and p.symbol == symbol and p.direction == pos.direction
                                          for e in self.engines.values() for p in e.positions.values())
                eng.total_realized_pnl += pnl
                eng.closed_trades.insert(0, rec)
                del eng.closed_trades[500:]
                if COOLDOWN_SECONDS > 0:
                    eng.cooldown[symbol] = now + COOLDOWN_SECONDS
                # Misma sección crítica que saca la posición: el guardado lleva ambas cosas.
                self.store.append_trade(rec)
            self._refresh_pos_syms()
            self.store.mark_dirty(True)
            self.log(f"[{eng.label}] CIERRA {pos.direction} {symbol} ({reason}) | entrada {pos.entry_price:g} → "
                     f"{price:g} ({rec['move_pct']:+.2f}%) | PnL neto {pnl:+.4f} USDT (fees {fees:.4f}) | trade #{pos.trade_id}")
            if pos.sent:                        # simulada: el executor nunca la abrió
                payload = {
                    "action":      "close",
                    "trade_id":    pos.trade_id,
                    "symbol":      symbol,
                    "direction":   pos.direction,
                    "reason":      reason,
                    "close_price": price,
                    "pnl":         round(pnl, 6),
                    "strategy":    eng.name,
                }
                if shared:
                    payload["quantity"] = pos.qty   # cierre parcial; la última cierra todo lo real
                self.executor.send(payload)
            if pnl < 0 and eng.name == ENGINE_RATIO_REV and not pos.revenge and reason not in REVENGE_SKIP_REASONS:
                self._open_revenge(eng, pos, price, -pnl, reason)
            return True
        finally:
            with self.lock:
                self._closing.discard(trade_id)

    def close_manual(self, ref: str, eng: Optional[Engine] = None) -> Tuple[bool, str]:
        """Cierre manual desde la web (corre en el loop del bot). `ref` es un
        trade_id (cierra esa posición) o un símbolo (cierra todas las de ese
        par, solo del bot `eng` si se indica)."""
        engines = [eng] if eng is not None else list(self.engines.values())
        with self.lock:
            if ref.isdigit():
                targets = [p for e in engines for t, p in e.positions.items() if t == int(ref)]
            else:
                targets = [p for e in engines for p in e.positions.values() if p.symbol == ref.upper()]
        if not targets:
            return False, "No hay posición abierta con ese trade o símbolo"
        closed = 0
        for pos in targets:
            sym = pos.symbol
            price = self._price(sym) or (self.price_cache.get_mark_price(sym) if self.price_cache else None) \
                or pos.last_price or pos.entry_price
            closed += self._close(pos.trade_id, price, "MANUAL")
        if not closed:
            return False, "No se pudo cerrar (ya se estaba cerrando)"
        return True, f"{closed} posición(es) cerrada(s)"

    # ── Bot de ratio de top traders ───────────────────────────────────────────

    def set_ratio_config(self, watch_min_pct: Optional[float] = None, drop_min_pct: Optional[float] = None,
                         drop_max_pct: Optional[float] = None, tp_pct: Optional[float] = None,
                         sl_pct: Optional[float] = None, ratio_exit: Optional[bool] = None) -> dict:
        """Ajusta en caliente el bot de ratio: % 24 h para vigilar, rango de caída del ratio,
        TP/SL por precio (también a las posiciones abiertas) y la salida por ratio."""
        old = self.ratio_cfg
        new = dict(old)
        for key, v in (("watch_min_pct", watch_min_pct), ("drop_min_pct", drop_min_pct),
                       ("drop_max_pct", drop_max_pct), ("tp_pct", tp_pct), ("sl_pct", sl_pct)):
            if v is not None:
                new[key] = abs(float(v))
        if ratio_exit is not None:
            new["ratio_exit"] = bool(ratio_exit)
        if new["drop_min_pct"] >= new["drop_max_pct"]:
            raise ValueError("el cambio mínimo del ratio debe ser menor que el máximo")
        if new["sl_pct"] >= 100:
            raise ValueError("el stop loss debe ser menor que 100 %")
        if new == old:
            return dict(old)
        changes = []
        for key, label in (("watch_min_pct", "24 h ≥"), ("drop_min_pct", "cambio mín. del ratio"),
                           ("drop_max_pct", "cambio máx. del ratio"), ("tp_pct", "TP +"), ("sl_pct", "SL -")):
            if new[key] != old[key]:
                changes.append(f"{label}{old[key]:g}% → {new[key]:g}%")
        if new["ratio_exit"] != old["ratio_exit"]:
            changes.append(f"salida por ratio {'activada' if new['ratio_exit'] else 'desactivada'}")
        self.ratio_cfg = new
        if new["tp_pct"] != old["tp_pct"] or new["sl_pct"] != old["sl_pct"]:
            self._apply_ratio_exits((ENGINE_RATIO,))
        self._save_settings()
        self.store.mark_dirty(True)
        self.log("[config] ratio: " + " | ".join(changes))
        return dict(self.ratio_cfg)

    def _exit_cfg(self, name: str) -> dict:
        """TP/SL de precio y salida por ratio de cada bot de ratio."""
        if name == ENGINE_RATIO_TP:
            return {"tp_pct": 0.0, "sl_pct": 0.0, "ratio_exit": True}
        c = self.rev_cfg if name == ENGINE_RATIO_REV else self.ratio_cfg
        return {"tp_pct": c["tp_pct"], "sl_pct": c["sl_pct"], "ratio_exit": c["ratio_exit"]}

    def _ratio_exit_prices(self, entry: float, direction: str, name: str = ENGINE_RATIO) -> Tuple[float, float]:
        cfg = self._exit_cfg(name)
        return tp_sl_prices(entry, direction, cfg["tp_pct"], cfg["sl_pct"])

    def _apply_ratio_exits(self, names: Tuple[str, ...] = RATIO_ENGINES) -> None:
        """Recalcula TP/SL por precio de las posiciones abiertas de los bots de ratio con la
        configuración vigente y las revisa ya (por si el nivel nuevo ya se tocó). Las de
        venganza conservan su TP/SL en USD."""
        syms = set()
        with self.lock:
            for name in names:
                for p in self.engines[name].positions.values():
                    if not p.revenge:
                        p.tp_price, p.sl_price = self._ratio_exit_prices(p.entry_price, p.direction, name)
                        syms.add(p.symbol)
        for sym in syms:
            self._on_price_event(sym, "config")

    def _rev_desc(self, c: Optional[dict] = None) -> str:
        c = c or self.rev_cfg
        return (f"TP +{c['tp_pct']:g}% · SL -{c['sl_pct']:g}% por precio{' · TP por ratio' if c['ratio_exit'] else ''} | "
                f"venganza {'ON' if c['revenge'] else 'OFF'}: ×{c['rev_mult']:g} notional, "
                f"{'contraria' if c['rev_dir'] == 'opposite' else 'misma dirección'}, TP {c['rev_recover_pct']:g}% "
                f"de la pérdida, SL {c['rev_sl_pct']:g}% de (pérdida × {c['rev_mult']:g})")

    def set_rev_config(self, **kw) -> dict:
        """Ajusta en caliente el bot ratio + venganza: TP/SL de precio (también a sus abiertas),
        salida por ratio y la venganza (activa, multiplicador, % a recuperar, SL, dirección)."""
        old = self.rev_cfg
        new = dict(old)
        for key in ("tp_pct", "sl_pct", "rev_mult", "rev_recover_pct", "rev_sl_pct"):
            if kw.get(key) is not None:
                new[key] = abs(float(kw[key]))
        for key in ("ratio_exit", "revenge"):
            if kw.get(key) is not None:
                new[key] = bool(kw[key])
        if kw.get("rev_dir") is not None:
            if kw["rev_dir"] not in ("opposite", "same"):
                raise ValueError("rev_dir debe ser opposite o same")
            new["rev_dir"] = kw["rev_dir"]
        if new["sl_pct"] >= 100:
            raise ValueError("el stop loss debe ser menor que 100 %")
        if not 0 < new["rev_mult"] <= 100:
            raise ValueError("el multiplicador de la venganza debe estar entre 0 y 100")
        if not 0 < new["rev_recover_pct"] <= 1000 or not 0 < new["rev_sl_pct"] <= 1000:
            raise ValueError("los % de la venganza deben estar entre 0 y 1000")
        if new == old:
            return dict(old)
        self.rev_cfg = new
        if new["tp_pct"] != old["tp_pct"] or new["sl_pct"] != old["sl_pct"]:
            self._apply_ratio_exits((ENGINE_RATIO_REV,))
        self._save_settings()
        self.store.mark_dirty(True)
        self.log(f"[config] {ENGINE_LABELS[ENGINE_RATIO_REV]}: {self._rev_desc()}")
        return dict(self.rev_cfg)

    def _open_revenge(self, eng: Engine, lost: Position, price: float, loss: float, reason: str) -> None:
        """Trade de venganza: tras cerrar `lost` con pérdida `loss` (USD, > 0) abre otra operación
        con notional × N. TP = rev_recover_pct % de la pérdida; SL = rev_sl_pct % de (pérdida × N),
        ambos en USD netos de fees. Si la venganza pierde no se encadena otra."""
        c = self.rev_cfg
        if not c["revenge"]:
            return
        sym = lost.symbol
        side = (SHORT if lost.direction == LONG else LONG) if c["rev_dir"] == "opposite" else lost.direction
        now = time.time()

        def note(action: str) -> None:
            with self.lock:
                eng.signals.appendleft({
                    "ts": now, "time": _utc(now, "%H:%M:%S"), "symbol": sym, "direction": side,
                    "body_pct": -loss, "open": lost.entry_price, "close": price,
                    "candle": f"#{lost.trade_id}", "candle_age_s": 0.0, "action": action,
                })

        if eng.paused:
            return note("venganza: bot pausado")
        if self.max_open > 0 and self.open_count() >= self.max_open:
            return note(f"venganza ignorada: límite de {self.max_open} abiertas")
        if not self.store.can_open():
            return note("venganza ignorada: estado sin confirmar")
        px = self._price(sym) or price
        f = self.filters.get(sym, {})
        qty, eff, why = size_order(
            lost.notional * c["rev_mult"], px, f.get("stepSize", 0.001), f.get("minQty", 0.0),
            f.get("minNotional", 5.0), MAX_NOTIONAL_OVER)
        if qty <= 0:
            return note(f"venganza saltada: {why}")
        tp_usd = loss * c["rev_recover_pct"] / 100.0
        sl_usd = loss * c["rev_mult"] * c["rev_sl_pct"] / 100.0
        fees = 2 * eff * self.fee_pct / 100.0                 # entrada + salida (aprox.)
        tp_move = (tp_usd + fees) / qty
        sl_move = max(sl_usd - fees, sl_usd * 0.1) / qty
        if side == LONG:
            tp_px, sl_px = px + tp_move, max(px - sl_move, 0.0)
        else:
            tp_px, sl_px = max(px - tp_move, 0.0), px + sl_move
        pos = Position(
            trade_id=self._next_trade_id(), symbol=sym, direction=side,
            entry_price=px, qty=qty, notional=eff, tp_price=tp_px, sl_price=sl_px,
            signal_pct=-loss, candle_open=lost.entry_price, candle_close=price, candle_time=int(now * 1000),
            # Una perdedora simulada nunca dispara una venganza real (aunque se reanude el executor).
            last_price=px, strategy=eng.name, inverted=False, sent=lost.sent and not self.executor_paused,
            revenge=True, rev_parent=lost.trade_id, rev_tp_usd=tp_usd, rev_sl_usd=sl_usd,
        )
        with self.lock:
            eng.positions[pos.trade_id] = pos
            eng.opened_count += 1
            self.revenge_count += 1
        self._refresh_pos_syms()
        self.store.mark_dirty(True)
        note(f"venganza abierta #{pos.trade_id}")
        self.log(f"[{eng.label}] VENGANZA {side} {sym}{'' if pos.sent else ' [SIMULADA]'} tras #{lost.trade_id} "
                 f"({reason}, {-loss:+.4f} USDT) | px={px:g} qty={qty:g} ({eff:.2f} USDT, ×{c['rev_mult']:g}) | "
                 f"TP {tp_px:g} (+{tp_usd:.4f} USD) · SL {sl_px:g} (-{sl_usd:.4f} USD) | trade #{pos.trade_id}")
        if pos.sent:
            self.executor.send({
                "action": "open", "trade_id": pos.trade_id, "symbol": sym, "direction": side,
                "price": px, "quantity": qty, "notional": round(eff, 4),
                "level": round(-loss, 4), "strategy": eng.name,
            }, time.perf_counter())

    async def _refresh_tickers(self) -> None:
        """Exploración de todo el mercado por WebSocket (TICKER_SCAN_S segundos): 24 h de cada
        símbolo para la lista de vigilados y descubrimiento de símbolos nuevos."""
        daily = time.time() - self.symbols_scanned_at >= SYMBOL_SCAN_HOURS * 3600
        seen = await self._ticker_ws_snapshot(TICKER_SCAN_S, quick=TICKER_SCAN_QUICK and not daily)
        self.ticker_connected = bool(seen)
        if seen:
            self._update_known_symbols(seen)

    async def _ticker_ws_snapshot(self, seconds: float, quick: bool = False) -> set:
        """Abre el WebSocket del 24 h `seconds` segundos, guarda lo recibido y lo cierra.
        quick: se cierra antes, en cuanto un mensaje casi no añade símbolos (ya llegó todo
        el mercado activo; los que no se mueven conservan su último 24 h). Devuelve los vistos."""
        for url in TICKER_WS_URLS:
            it = None
            self._scan_seen = set()
            try:
                it = _ws_messages(url).__aiter__()
                msgs = 0
                end = time.monotonic() + seconds
                while (left := end - time.monotonic()) > 0:
                    try:
                        raw = await asyncio.wait_for(it.__anext__(), timeout=left)
                    except asyncio.TimeoutError:
                        break
                    self.rx_ticker_bytes += len(raw)
                    before = len(self._scan_seen)
                    self._on_tickers(raw)
                    msgs += 1
                    added = len(self._scan_seen) - before
                    if quick and msgs >= 2 and added <= max(2, before // 200):
                        break
            except asyncio.CancelledError:
                raise
            except StopAsyncIteration:
                self.ticker_error = f"el servidor cerró la exploración ({url})"
            except Exception as exc:
                self.ticker_error = f"exploración 24 h: {exc!r}"
            finally:
                if it is not None:
                    try:
                        await it.aclose()
                    except Exception:
                        pass
            seen, self._scan_seen = self._scan_seen, None
            if seen:
                self.ticker_error = ""
                return seen
        self.log(f"[ticker] exploración sin datos: {self.ticker_error or 'ninguna URL respondió'}")
        return set()

    # ── Símbolos descubiertos por WebSocket ───────────────────────────────────

    def _load_known_symbols(self) -> None:
        try:
            with open(KNOWN_SYMBOLS_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            self.known_symbols = {str(x) for x in data.get("symbols") or []}
            self.symbols_scanned_at = float(data.get("scanned_at") or 0.0)
        except Exception:
            self.known_symbols, self.symbols_scanned_at = set(), 0.0

    def _save_known_symbols(self) -> None:
        tmp = f"{KNOWN_SYMBOLS_FILE}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"symbols": sorted(self.known_symbols), "scanned_at": self.symbols_scanned_at}, fh)
            os.replace(tmp, KNOWN_SYMBOLS_FILE)
        except Exception as exc:
            self.log(f"No pude guardar la lista de símbolos: {exc}")

    def _update_known_symbols(self, seen: set) -> None:
        """Una vez al día la exploración REEMPLAZA la lista (salen los deslistados); el resto
        de exploraciones solo añaden los símbolos nuevos que aparezcan."""
        now = time.time()
        had = bool(self.known_symbols)
        new = seen - self.known_symbols
        if now - self.symbols_scanned_at >= SYMBOL_SCAN_HOURS * 3600:
            gone = self.known_symbols - seen
            self.known_symbols, self.symbols_scanned_at = set(seen), now
            self._save_known_symbols()
            self.log(f"[símbolos] exploración diaria: {len(seen)} perpetuos {QUOTE_ASSET}"
                     + (f" | nuevos: {', '.join(sorted(new)[:10])}" if had and new else "")
                     + (f" | ya no aparecen: {', '.join(sorted(gone)[:10])}" if gone else ""))
        elif new:
            self.known_symbols |= new
            self._save_known_symbols()
            self.log(f"[símbolos] nuevos: {', '.join(sorted(new)[:10])}")
        if not self.symbols and self.known_symbols:
            self.symbols = sorted(self.known_symbols)

    async def _ticker_loop(self) -> None:
        """TICKER_MODE=ws: WebSocket de 24 h de todos los símbolos abierto siempre."""
        idx, backoff = 0, 1.0
        while self.running:
            url = TICKER_WS_URLS[idx % len(TICKER_WS_URLS)]
            got = False
            it = None
            try:
                self.log(f"[ticker] conectando a {url}")
                it = _ws_messages(url).__aiter__()
                while True:
                    raw = await asyncio.wait_for(it.__anext__(), timeout=TICKER_SILENCE_S)
                    if not got:
                        got = True
                        self.log("[ticker] recibiendo datos")
                    self.ticker_connected = True
                    self.ticker_error = ""
                    backoff = 1.0
                    self.rx_ticker_bytes += len(raw)
                    self._on_tickers(raw)
            except StopAsyncIteration:
                self.ticker_error = "conexión cerrada por el servidor"
            except asyncio.TimeoutError:
                self.ticker_error = f"sin datos en {TICKER_SILENCE_S:g}s ({url})"
                self.log(f"[ticker] {self.ticker_error}")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.ticker_error = repr(exc)
                self.log(f"[ticker] error: {exc!r}")
            finally:
                if it is not None:
                    try:
                        await it.aclose()
                    except Exception:
                        pass
            self.ticker_connected = False
            if not got:
                idx += 1                                    # esta URL no dio datos: probar la siguiente
            await asyncio.sleep(min(backoff, 30.0))
            backoff *= 2

    async def _symbols_retry_loop(self) -> None:
        """Si exchangeInfo falló al arrancar (proxy/región), reintenta cada 60 s; luego lo
        renueva cada SYMBOL_REFRESH_HOURS (10 días)."""
        while self.running:
            await asyncio.sleep(60.0)
            if not self.filters:
                self.log("[REST] reintentando exchangeInfo…")
                await self._init_symbols()
            elif time.time() - self._filters_ts >= SYMBOL_REFRESH_HOURS * 3600:
                await self._init_symbols()

    def _on_tickers(self, raw) -> None:
        data = json.loads(raw)
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        if isinstance(data, dict):
            data = [data]
        if isinstance(data, list):
            self._apply_tickers(data)

    def _apply_tickers(self, data: list) -> None:
        """Filas {s, c, P|o} (formato del WebSocket) → self._tick."""
        now = time.time()
        seen = self._scan_seen
        for t in data:
            try:
                sym = t["s"]
                if seen is not None and sym.endswith(QUOTE_ASSET) and "_" not in sym:
                    seen.add(sym)                   # descubrimiento de símbolos
                if self.filters:
                    if sym not in self.filters:
                        continue
                elif not sym.endswith(QUOTE_ASSET) or "_" in sym:
                    continue                    # sin exchangeInfo: solo perpetuos USDT por nombre
                last = float(t["c"])
                if t.get("P") is not None:
                    pct = float(t["P"])
                else:
                    o = float(t["o"])
                    pct = (last - o) / o * 100.0 if o > 0 else 0.0
            except (KeyError, TypeError, ValueError, AttributeError):
                continue
            self._tick[sym] = (pct, last, now)
        self.ticker_last_ts = now

    def _refresh_watchlist(self) -> None:
        """Recalcula la lista de vigilados (24 h ≥ +X %; con LONG activo, también ≤ -X %)
        y suscribe los nuevos a precios en vivo."""
        wmin = self.ratio_cfg["watch_min_pct"]
        engs = [self.engines[n] for n in RATIO_ENGINES]
        want_short = any(not e.blocked[SHORT] for e in engs)
        want_long = any(not e.blocked[LONG] for e in engs)
        picked = []
        for sym, (pct, _px, _ts) in list(self._tick.items()):
            if pct >= wmin and want_short:
                picked.append((abs(pct), sym, SHORT))
            elif pct <= -wmin and want_long:
                picked.append((abs(pct), sym, LONG))
        picked.sort(reverse=True)
        picked = picked[:RATIO_WATCH_MAX]
        old = set(self.watch)
        self.watch = [s for _, s, _ in picked]
        self.watch_side = {s: side for _, s, side in picked}
        self.watch_ts = time.time()
        added = sorted(set(self.watch) - old)
        removed = sorted(old - set(self.watch))
        if added or removed or not old:
            self.log(f"[ratio] vigilados (24 h ≥ ±{wmin:g}%): {len(self.watch)} "
                     f"(+{len(added)} −{len(removed)}) {', '.join(self.watch[:12])}{'…' if len(self.watch) > 12 else ''}")
        self._sync_watch_prices()

    def _sync_watch_prices(self) -> None:
        """Vigilados: precio en vivo lento (markPrice cada 3 s). Los que tienen posición van
        además por el stream rápido (aggTrade, ver _refresh_pos_syms)."""
        pc = self.price_cache
        if pc is not None:
            pc.set_slow_symbols(self.watch if WATCH_LIVE_PRICE else [])

    def _ratio_open_syms(self) -> set:
        with self.lock:
            return {p.symbol for n in RATIO_ENGINES for p in self.engines[n].positions.values()}

    def _ratio_symbols(self) -> List[str]:
        """Vigilados + abiertas que esperan el TP por ratio. Las venganzas y los bots con la
        salida por ratio apagada cierran por precio: no gastan REST."""
        names = [n for n in RATIO_ENGINES if self._exit_cfg(n)["ratio_exit"]]
        with self.lock:
            open_syms = {p.symbol for n in names for p in self.engines[n].positions.values() if not p.revenge}
        return list(dict.fromkeys(list(self.watch) + sorted(open_syms)))

    def _rest_get(self, path: str, params: Optional[dict] = None, timeout: float = 15.0):
        """GET público a Binance (gzip) por el proxy (y por IP directa si el proxy falla).
        Un 429/418 NO se reintenta por otra ruta: se pausa todo el REST."""
        routes = [REST_PROXY_URL, ""] if REST_PROXY_URL else [""]
        order = routes[self._rest_route:] + routes[:self._rest_route]
        url = f"{BASE_URL}{path}" + (f"?{urlencode(params)}" if params else "")
        last_exc: Optional[Exception] = None
        for proxy in order:
            try:
                data, nbytes = http_get_json(url, proxy, timeout)
                self.rx_rest_bytes += nbytes
                self._rest_route = routes.index(proxy)
                return data
            except urllib.error.HTTPError as exc:
                if exc.code in (418, 429):
                    try:
                        wait = float(exc.headers.get("Retry-After") or 60)
                    except (TypeError, ValueError):
                        wait = 60.0
                    self._rest_block_until = time.time() + max(wait, 30.0)
                raise RuntimeError(f"HTTP {exc.code}")
            except Exception as exc:
                last_exc = exc
        raise RuntimeError(f"sin respuesta: {last_exc}")

    def _fetch_ratio(self, symbol: str) -> List[Tuple[int, float]]:
        data = self._rest_get("/futures/data/topLongShortAccountRatio",
                              {"symbol": symbol, "period": RATIO_PERIOD, "limit": 4})
        pts: List[Tuple[int, float]] = []
        for r in data if isinstance(data, list) else []:
            try:
                pts.append((int(r["timestamp"]), float(r["longShortRatio"])))
            except (KeyError, TypeError, ValueError):
                continue
        pts.sort()
        return pts

    async def _ratio_loop(self) -> None:
        for _ in range(30 if TICKER_MODE == "ws" else 0):    # espera los primeros tickers
            if self._tick:
                break
            await asyncio.sleep(1.0)
        first = True
        while self.running:
            if not first:
                now = time.time()
                nxt = (int(now // RATIO_REFRESH_S) + 1) * RATIO_REFRESH_S + RATIO_POLL_DELAY_S
                self.ratio_next_cycle_ts = nxt
                await asyncio.sleep(max(1.0, nxt - now))
            first = False
            await self._ratio_cycle()

    def _lagging(self, syms: List[str]) -> List[str]:
        """Símbolos cuyo último punto de ratio es más viejo que el más nuevo visto."""
        ts = {s: (self._ratio.get(s) or {}).get("last_ts", 0) for s in syms}
        ref = max(ts.values(), default=0)
        return [s for s in syms if ts[s] < ref]

    async def _ratio_cycle(self) -> None:
        if TICKER_MODE != "ws":
            await self._refresh_tickers()
        self._refresh_watchlist()
        self.ratio_last_cycle_ts = time.time()
        syms = self._ratio_symbols()
        deadline = (int(time.time() // RATIO_REFRESH_S) + 1) * RATIO_REFRESH_S - 10   # antes del siguiente ciclo
        fresh = await self._poll_ratios(syms)
        for attempt in range(1, RATIO_RETRIES + 1):
            # Rezagados: Binance publica el punto de 5 m a cada símbolo en un momento distinto.
            pending = set(self._lagging(syms))
            if attempt <= 2:
                pending |= {s for s in syms if not fresh.get(s, False)}     # aún sin dato nuevo
            pending = [s for s in syms if s in pending]
            if not pending or time.time() < self._rest_block_until:
                break
            if time.time() + RATIO_RETRY_GAP_S > deadline:
                break
            await asyncio.sleep(RATIO_RETRY_GAP_S)
            got = await self._poll_ratios(pending)
            fresh.update(got)
        late = self._lagging(syms)
        if late:
            self.log(f"[ratio] {len(late)} símbolo(s) siguen sin el dato más nuevo de Binance: {', '.join(late[:8])}")
        if time.time() < self._rest_block_until:
            self.log(f"[ratio] ⚠️ REST en pausa {self._rest_block_until - time.time():.0f} s por límite de peticiones")

    async def _poll_ratios(self, symbols: List[str]) -> Dict[str, bool]:
        sem = asyncio.Semaphore(RATIO_CONCURRENCY)
        fresh: Dict[str, bool] = {}

        async def one(sym: str) -> None:
            if time.time() < self._rest_block_until:
                return
            now_t = time.time()
            while self._rest_times and now_t - self._rest_times[0] > 300:
                self._rest_times.popleft()
            if len(self._rest_times) >= RATIO_REQ_BUDGET:
                self.ratio_last_error = f"presupuesto de {RATIO_REQ_BUDGET} peticiones/5 min agotado"
                return
            self._rest_times.append(now_t)
            async with sem:
                try:
                    pts = await asyncio.to_thread(self._fetch_ratio, sym)
                except Exception as exc:
                    self.ratio_errors += 1
                    self.ratio_last_error = f"{sym}: {exc}"
                    return
            self.ratio_polls += 1
            hold = self._entry_candidate(sym, pts)
            try:
                if hold:
                    with self.lock:
                        self._price_hold.add(sym)
                    await self._prefetch_price(sym)
                fresh[sym] = self._on_ratio_points(sym, pts)
            except Exception as exc:
                self.ratio_last_error = f"{sym}: {exc!r}"
            finally:
                if hold:
                    with self.lock:
                        self._price_hold.discard(sym)

        await asyncio.gather(*(one(s) for s in symbols))
        self._refresh_pos_syms(force=True)                  # da de baja los precios pedidos sin entrada
        return fresh

    def _on_ratio_points(self, sym: str, pts: List[Tuple[int, float]]) -> bool:
        """Guarda el ratio, revisa el TP de las abiertas y evalúa el gatillo.
        Devuelve True si había un punto nuevo."""
        if not pts:
            return False
        st = self._ratio.setdefault(sym, {})
        last_ts, last_r = pts[-1]
        new = last_ts != st.get("last_ts")
        prev = pts[-2] if len(pts) >= 2 else None
        st.update(last_ts=last_ts, last=last_r, prev_ts=prev[0] if prev else 0,
                  prev=prev[1] if prev else None, checked=time.time(),
                  chg=((last_r - prev[1]) / prev[1] * 100.0) if prev and prev[1] > 0 else None)
        self._ratio_check_tp(sym, pts)
        if new and prev and st["chg"] is not None:
            self._ratio_check_entry(sym, prev, pts[-1], st["chg"])
        return new

    def _live_price(self, sym: str) -> Optional[float]:
        px = self._price(sym)
        if px:
            return px
        t = self._tick.get(sym)
        if t and time.time() - t[2] <= 10.0:
            return t[1]
        return None

    def _entry_candidate(self, sym: str, pts: List[Tuple[int, float]]) -> bool:
        """¿Estos puntos de ratio pueden abrir una entrada? (para pedir antes el precio en vivo)"""
        if len(pts) < 2 or pts[-1][0] == (self._ratio.get(sym) or {}).get("last_ts") or pts[-2][1] <= 0:
            return False
        chg = (pts[-1][1] - pts[-2][1]) / pts[-2][1] * 100.0
        cfg = self.ratio_cfg
        return cfg["drop_min_pct"] <= abs(chg) <= cfg["drop_max_pct"]

    async def _prefetch_price(self, sym: str) -> None:
        """Suscribe el precio en vivo del símbolo (aggTrade) y espera el primer tick, para
        abrir con precio actual sin REST. Si no se abre, _refresh_pos_syms lo da de baja."""
        pc = self.price_cache
        if pc is None or self._price(sym):
            return
        await asyncio.to_thread(pc.wait_for_price, sym, 5.0, PRICE_MAX_AGE_S)

    def _ratio_check_tp(self, sym: str, pts: List[Tuple[int, float]]) -> None:
        """TP por ratio: el ratio recupera el % que se movió (en los bots que lo tienen activo)."""
        names = [n for n in RATIO_ENGINES if self._exit_cfg(n)["ratio_exit"]]
        with self.lock:
            targets = [p for n in names for p in self.engines[n].positions.values()
                       if p.symbol == sym and not p.revenge and (p.ratio_target or p.ratio_trigger) > 0]
        if not targets or not self.store.can_act():
            return
        for p in targets:
            after = [r for ts, r in pts if ts > p.ratio_ts]
            if not after:
                continue
            goal = p.ratio_target or p.ratio_trigger
            if p.direction == SHORT:
                hit, best = max(after) >= goal, max(after)
            else:
                hit, best = min(after) <= goal, min(after)
            if not hit:
                continue
            price = self._live_price(sym) or p.last_price or p.entry_price
            self.log(f"[ratio] TP {p.direction} {sym} ({ENGINE_LABELS[p.strategy]}): ratio {best:g} alcanzó el objetivo "
                     f"{goal:g} (entrada {p.ratio_entry:g}, recupera {abs(p.ratio_change_pct):.1f}%)")
            self._close(p.trade_id, price, "TP_RATIO")

    def _record_ratio_signal(self, eng: Engine, sym: str, direction: str, prev: Tuple[int, float],
                             last: Tuple[int, float], chg: float, action: str) -> None:
        now = time.time()
        with self.lock:
            eng.signals.appendleft({
                "ts": now, "time": _utc(now, "%H:%M:%S"), "symbol": sym, "direction": direction,
                "body_pct": chg, "open": prev[1], "close": last[1],
                "candle": _utc(last[0] / 1000, "%H:%M"), "candle_age_s": 0.0, "action": action,
            })

    def _ratio_check_entry(self, sym: str, prev: Tuple[int, float], last: Tuple[int, float], chg: float) -> None:
        """Señal común a los bots de ratio: cada uno decide y abre su propia posición."""
        cfg = self.ratio_cfg
        dmin, dmax, wmin = cfg["drop_min_pct"], cfg["drop_max_pct"], cfg["watch_min_pct"]
        if -dmax <= chg <= -dmin:
            side = SHORT
        elif dmin <= chg <= dmax:
            side = LONG
        else:
            return
        t = self._tick.get(sym)
        pct24 = t[0] if t else None
        if pct24 is None or (side == SHORT and pct24 < wmin) or (side == LONG and pct24 > -wmin):
            return                                  # solo se opera lo que está en la lista de vigilados
        age_s = time.time() - last[0] / 1000
        price = self._live_price(sym)
        for name in RATIO_ENGINES:
            self._ratio_open(self.engines[name], sym, side, prev, last, chg, pct24, age_s, price)

    def _ratio_open(self, eng: Engine, sym: str, side: str, prev: Tuple[int, float], last: Tuple[int, float],
                    chg: float, pct24: float, age_s: float, price: Optional[float]) -> None:
        eng.signal_count += 1

        def skip(action: str) -> None:
            self._record_ratio_signal(eng, sym, side, prev, last, chg, action)

        if age_s > RATIO_MAX_AGE_S:
            return skip(f"ignorada: dato de ratio de hace {age_s:.0f}s")
        if eng.paused:
            return skip("pausado")
        if eng.blocked.get(side):
            return skip(f"bloqueada: {side} desactivados")
        if self.max_open > 0 and self.open_count() >= self.max_open:
            return skip(f"ignorada: límite de {self.max_open} abiertas")
        if not self.store.can_open():
            return skip("ignorada: estado sin confirmar")
        with self.lock:
            dup = any(p.symbol == sym and p.direction == side and not p.revenge for p in eng.positions.values())
            until = eng.cooldown.get(sym, 0.0)
        if dup:
            return skip("ya hay una abierta en este par y dirección")
        if until > time.time():
            return skip("ignorada: en pausa tras cierre")
        if not price:
            return skip("sin precio en vivo")
        if sym not in self.filters:
            return skip("saltada: sin filtros del símbolo (exchangeInfo no cargó: revisa el proxy)")
        f = self.filters[sym]
        qty, eff, why = size_order(
            NOTIONAL_USDT, price, f.get("stepSize", 0.001), f.get("minQty", 0.0),
            f.get("minNotional", 5.0), MAX_NOTIONAL_OVER)
        if qty <= 0:
            self.log(f"[{eng.label}] señal {side} {sym} saltada: {why}")
            return skip(f"saltada: {why}")
        # TP: el ratio recupera el mismo % que cayó (SHORT) / subió (LONG), desde el ratio de entrada.
        target = last[1] * (1 + abs(chg) / 100.0) if side == SHORT else last[1] * (1 - abs(chg) / 100.0)
        tp_px, sl_px = self._ratio_exit_prices(price, side, eng.name)
        pos = Position(
            trade_id=self._next_trade_id(), symbol=sym, direction=side,
            entry_price=price, qty=qty, notional=eff, tp_price=tp_px, sl_price=sl_px,
            signal_pct=chg, candle_open=prev[1], candle_close=last[1], candle_time=last[0],
            last_price=price, strategy=eng.name, inverted=False, sent=not self.executor_paused,
            ratio_trigger=prev[1], ratio_entry=last[1], ratio_change_pct=chg, ratio_ts=last[0],
            ratio_target=target,
        )
        with self.lock:
            eng.positions[pos.trade_id] = pos
            eng.opened_count += 1
        self._refresh_pos_syms()                            # suscribe el precio en vivo del par
        self.store.mark_dirty(True)
        skip(f"abierta #{pos.trade_id}")
        by_ratio = (f" · TP ratio {'≥' if side == SHORT else '≤'} {target:g}"
                    if self._exit_cfg(eng.name)["ratio_exit"] else "")
        self.log(f"[{eng.label}] ABRE {side} {sym}{'' if pos.sent else ' [SIMULADA: envíos al executor pausados]'} | "
                 f"ratio {prev[1]:g}→{last[1]:g} ({chg:+.1f}%) | 24 h {pct24:+.1f}% | px={price:g} qty={qty:g} "
                 f"({eff:.2f} USDT) | TP {tp_px or '—'} · SL {sl_px or '—'}{by_ratio} | trade #{pos.trade_id}")
        if pos.sent:
            self.executor.send({
                "action": "open", "trade_id": pos.trade_id, "symbol": sym, "direction": side,
                "price": price, "quantity": qty, "notional": round(eff, 4),
                "level": round(chg, 4), "strategy": eng.name,
            }, time.perf_counter())

    def _ratio_snapshot(self, all_rows: bool = True) -> dict:
        eng = self.engines[ENGINE_RATIO]
        cfg = dict(self.ratio_cfg)
        dmin, dmax = cfg["drop_min_pct"], cfg["drop_max_pct"]
        open_syms = self._ratio_open_syms()
        now = time.time()
        rows = []
        listed = list(dict.fromkeys(list(self.watch) + sorted(open_syms))) if all_rows else sorted(open_syms)
        for sym in listed:
            t = self._tick.get(sym)
            st = self._ratio.get(sym) or {}
            side = self.watch_side.get(sym, "")
            chg = st.get("chg")
            in_range = bool(chg is not None and ((side == SHORT and -dmax <= chg <= -dmin)
                                                 or (side == LONG and dmin <= chg <= dmax)))
            rows.append({
                "symbol": sym, "pct24": t[0] if t else None, "price": self._price(sym) or (t[1] if t else None),
                "prev": st.get("prev"), "last": st.get("last"), "chg": chg,
                "data_time": _utc(st["last_ts"] / 1000, "%H:%M") if st.get("last_ts") else "",
                "side": side, "in_range": in_range, "has_position": sym in open_syms,
            })
        newest = max((st.get("last_ts", 0) for st in self._ratio.values()), default=0)
        for r in rows:
            r["lag"] = bool(r["data_time"] and (self._ratio.get(r["symbol"]) or {}).get("last_ts", 0) < newest)
        rows.sort(key=lambda r: abs(r["pct24"] or 0), reverse=True)
        return {
            "cfg": cfg, "rev": dict(self.rev_cfg), "revenge_count": self.revenge_count,
            "rows": rows, "watch_count": len(self.watch),
            "long_enabled": not eng.blocked[LONG], "short_enabled": not eng.blocked[SHORT],
            "ticker_connected": self.ticker_connected, "ticker_symbols": len(self._tick),
            "ticker_mode": TICKER_MODE, "scan_s": TICKER_SCAN_S,
            "known_symbols": len(self.known_symbols),
            "symbols_scanned": _utc(self.symbols_scanned_at, "%m-%d %H:%M") if self.symbols_scanned_at else "",
            "ticker_age_s": (now - self.ticker_last_ts) if self.ticker_last_ts else None,
            "ticker_error": self.ticker_error,
            "watch_updated": _utc(self.watch_ts, "%H:%M:%S") if self.watch_ts else "",
            "next_cycle_in": max(0.0, self.ratio_next_cycle_ts - now) if self.ratio_next_cycle_ts else 0.0,
            "polls": self.ratio_polls, "errors": self.ratio_errors, "last_error": self.ratio_last_error,
            "rest_blocked_s": max(0.0, self._rest_block_until - now),
            "filters": len(self.filters),
        }

    # ── Mantenimiento ─────────────────────────────────────────────────────────

    async def _maintenance_loop(self) -> None:
        last_persist = time.time()
        while self.running:
            await asyncio.sleep(max(0.2, SAFETY_TICK_SECS))
            now = time.time()
            for sym in list(self._pos_syms):              # red de seguridad del TP/SL
                self._check_take_profit(sym)
            self._check_profit_target()
            with self.lock:
                for eng in self.engines.values():
                    if eng.cooldown:
                        eng.cooldown = {s: t for s, t in eng.cooldown.items() if t > now}
            if now - last_persist >= STATE_PERSIST_SECS:
                last_persist = now
                self.store.mark_dirty(False)              # MFE/MAE con el latido
            if self.store.restore_pending():
                self._restore_from_store(late=True)

    # ── Recuperación tras reinicio ────────────────────────────────────────────

    def _recovery_doc(self) -> Tuple[dict, List[dict]]:
        now = time.time()
        with self.lock:
            doc = {
                "v": 3,
                "trade_id_seq": self.trade_id_seq,
                "positions": {str(t): asdict(p) for e in self.engines.values()
                              for t, p in e.positions.items()},
                "session_base": self.session_base,
                "session_started_ts": self.session_started_ts,
                "target_hits": self.target_hits,
                "engines": {n: {"total_realized_pnl": e.total_realized_pnl,
                                "cooldowns": {s: t for s, t in e.cooldown.items() if t > now}}
                            for n, e in self.engines.items()},
            }
            trades = self.store.peek_trades()
        t = self.executor.target
        if self.executor_changed_ts:
            # Solo si se cambió desde la web: así sobrevive a reinicios en Render.
            doc["executor"] = {"url": t.url, "secret": t.secret, "changed_ts": self.executor_changed_ts}
        return doc, trades

    def _restore_from_store(self, late: bool = False) -> None:
        r = self.store.take_restore()
        if r is None:
            return
        doc, trades, source = r
        doc = doc or {}
        names = {f.name for f in fields(Position)}
        # Lo guardado por la versión de un solo bot (sin "strategy") va al bot al cierre.
        route = lambda d: d.get("strategy") if d.get("strategy") in self.engines else ENGINE_CLOSE  # noqa: E731
        positions: Dict[str, Dict[int, Position]] = {n: {} for n in self.engines}
        for key, d in (doc.get("positions") or {}).items():
            try:
                pos = Position(**{k: v for k, v in d.items() if k in names})
                pos.strategy = route(d)
                positions[pos.strategy][pos.trade_id] = pos
            except Exception as exc:
                self.log(f"[estado] posición {key} ilegible: {exc}")
        ex = doc.get("executor") or {}
        try:
            changed = float(ex.get("changed_ts") or 0.0)
        except (TypeError, ValueError):
            changed = 0.0
        if ex.get("url") is not None and changed > self.executor_changed_ts:
            self.executor.set_target(ex.get("url", ""), ex.get("secret", self.executor.target.secret))
            self.executor_changed_ts = changed
            self._save_settings()
        rows = [t for t in trades if isinstance(t, dict)]
        rows.sort(key=lambda t: float(t.get("closed_at_ts") or 0), reverse=True)
        per_engine = doc.get("engines") or {
            ENGINE_CLOSE: {"total_realized_pnl": doc.get("total_realized_pnl"),
                           "cooldowns": doc.get("cooldowns")}}
        with self.lock:
            for name, eng in self.engines.items():
                saved = per_engine.get(name) or {}
                eng.positions = positions[name]
                eng.cooldown = {s: float(t) for s, t in (saved.get("cooldowns") or {}).items()}
                eng.total_realized_pnl = float(saved.get("total_realized_pnl") or 0.0)
                eng.closed_trades = [t for t in rows if route(t) == name][:500]
            all_pos = [p for ps in positions.values() for p in ps.values()]
            self.session_base = float(doc.get("session_base") or 0.0)
            self.session_started_ts = float(doc.get("session_started_ts") or self.session_started_ts)
            self.target_hits = int(doc.get("target_hits") or 0)
            self.trade_id_seq = max(int(doc.get("trade_id_seq") or 0), *[p.trade_id for p in all_pos], 0)
        self._refresh_pos_syms()
        self._apply_ratio_exits()                   # TP/SL vigentes también para las recuperadas
        if not self.store.enable_writes():
            self.log("[estado] Perdí el control mientras aplicaba el estado; sigo esperando")
            return
        if late and self.price_cache is not None:
            self.price_cache.ensure_symbols(sorted({p.symbol for p in all_pos}))
        detail = ", ".join(f"{ENGINE_LABELS[p.strategy]}: {p.direction} {p.symbol} #{p.trade_id}"
                           for p in all_pos) or "ninguna"
        self.log(f"[estado] Recuperado desde {source}: abiertas: {detail} | "
                 f"historial: {len(rows)} cierres | executor → {self.executor.target.url or '(vacío)'}")

    def _on_fenced(self) -> None:
        self.last_error = "Otra instancia tomó el control del estado: esta queda en standby"

    # ── Vista para la web ─────────────────────────────────────────────────────

    def excursion_stats(self, eng: Engine) -> dict:
        """MFE/MAE agregados de las operaciones cerradas de un bot, total y por dirección."""
        with self.lock:
            rows = list(eng.closed_trades)

        def agg(rs: List[dict]) -> dict:
            n = len(rs)
            if not n:
                return {"n": 0}
            avg = lambda k: sum(float(r.get(k) or 0) for r in rs) / n   # noqa: E731
            return {
                "n": n,
                "wins": sum(1 for r in rs if (r.get("pnl") or 0) > 0),
                "avg_mfe_pct": avg("mfe_pct"), "avg_mae_pct": avg("mae_pct"),
                "worst_mae_pct": min(float(r.get("mae_pct") or 0) for r in rs),
                "best_mfe_pct": max(float(r.get("mfe_pct") or 0) for r in rs),
                "avg_mae_usd": avg("mae_usd"),
                "avg_duration_s": avg("duration_s"),
                "avg_time_to_mae_s": avg("time_to_mae_s"),
            }
        return {
            "all": agg(rows),
            "LONG": agg([r for r in rows if r.get("direction") == LONG]),
            "SHORT": agg([r for r in rows if r.get("direction") == SHORT]),
        }

    # Campos que pinta la web (el resto queda en /api/trades y el CSV): menos bytes por consulta.
    _POS_FIELDS = ("trade_id", "symbol", "direction", "entry_price", "notional", "tp_price", "sl_price",
                   "signal_pct", "candle_age_s", "inverted", "sent", "mfe_pct", "mae_pct", "mfe_usd",
                   "mae_usd", "ratio_trigger", "ratio_entry", "ratio_change_pct", "ratio_target", "revenge",
                   "rev_parent", "rev_tp_usd", "rev_sl_usd")
    _CLOSED_FIELDS = ("symbol", "direction", "entry_price", "close_price", "move_pct", "pnl", "fees",
                      "mfe_pct", "mae_pct", "duration_s", "reason", "sent", "revenge")
    _SIGNAL_FIELDS = ("time", "symbol", "direction", "candle", "open", "close", "body_pct",
                      "candle_age_s", "action")
    WEB_CLOSED_ROWS = 100
    WEB_SIGNAL_ROWS = 60
    # Partes pesadas del estado: la web pide solo las de la vista abierta (?view=).
    ALL_PARTS = frozenset({"signals", "closed", "exc", "eval", "ratio_rows", "events"})
    VIEW_PARTS = {
        "resumen":     frozenset(),
        "operaciones": frozenset({"signals"}),
        "bots":        frozenset(),
        "mercado":     frozenset({"ratio_rows", "eval"}),
        "historial":   frozenset({"closed", "exc"}),
        "sistema":     frozenset({"events"}),
    }

    def _engine_snapshot(self, eng: Engine, now: float, parts: frozenset = ALL_PARTS) -> dict:
        """Estado de un bot para la web. Siempre van las cifras y las posiciones abiertas;
        señales, cierres, MFE/MAE y evaluaciones solo si están en `parts`."""
        detail = bool(parts)
        with self.lock:
            positions = []
            for p in sorted(eng.positions.values(), key=lambda p: p.opened_ts, reverse=True):
                price = self._price(p.symbol) or p.last_price
                row = {k: getattr(p, k) for k in self._POS_FIELDS}
                row.update(price=price,
                           move_pct=favorable_move_pct(price, p.entry_price, p.direction),
                           pnl=self._net_pnl(p, price), age_s=round(now - p.opened_ts),
                           opened=_utc(p.opened_ts, "%m-%d %H:%M:%S"))
                positions.append(row)
            closed = ([{**{k: t.get(k) for k in self._CLOSED_FIELDS},
                        "closed": _utc(t.get("closed_at_ts", 0), "%m-%d %H:%M:%S")}
                       for t in eng.closed_trades[:self.WEB_CLOSED_ROWS]] if "closed" in parts else [])
            signals = ([{k: sg.get(k) for k in self._SIGNAL_FIELDS}
                        for sg in list(eng.signals)[:self.WEB_SIGNAL_ROWS]] if "signals" in parts else [])
            wins = sum(1 for t in eng.closed_trades if t.get("pnl", 0) > 0)
            closed_count = len(eng.closed_trades)
            decide = list(eng._decide_ms)
        empty_eval = {"total": 0, "minutes": [], "top": [], "top_minute": ""}
        return {
            "name":           eng.name,
            "label":          eng.label,
            "hint":           ENGINE_HINTS[eng.name],
            "config":         eng.settings(),
            "detail":         detail,
            "signal_count":   eng.signal_count,
            "opened_count":   eng.opened_count,
            "closed_count":   closed_count,
            "wins":           wins,
            "realized_pnl":   eng.total_realized_pnl,
            "unrealized_pnl": sum(p["pnl"] for p in positions),
            "decide_ms":      round(sum(decide) / len(decide), 2) if decide else None,
            "positions":      positions,
            "closed_trades":  closed,
            "signals":        signals,
            "excursions":     self.excursion_stats(eng) if "exc" in parts else None,
            "evaluations":    (self.evaluations(eng.threshold_pct)
                               if "eval" in parts and eng.name not in RATIO_ENGINES else empty_eval),
        }

    def network_stats(self) -> dict:
        """Bytes recibidos/enviados desde el arranque, para vigilar el ancho de banda."""
        pc = self.price_cache
        ks = self.kline_stream.get_stats() if self.kline_stream else {}
        rx = {"rest": self.rx_rest_bytes, "ticker": self.rx_ticker_bytes,
              "prices": pc.rx_bytes if pc else 0, "klines": int(ks.get("rx_bytes") or 0)}
        up = max(time.time() - self.started_at, 1.0)
        total = sum(rx.values())
        return {"rx": rx, "rx_total": total, "web_tx": self.tx_web_bytes,
                "rx_per_hour": total / up * 3600, "web_per_hour": self.tx_web_bytes / up * 3600,
                "price_stream": pc.symbol_stream if pc else "", "ticker_mode": TICKER_MODE,
                "price_symbols": len(pc.symbols) if pc else 0}

    def _global_snapshot(self) -> dict:
        realized, unreal = self.session_pnl()
        return {
            **self.global_settings(),
            "open_count": self.open_count(),
            "session_realized": realized, "session_unrealized": unreal,
            "session_pnl": realized + unreal,
            "session_since": _utc(self.session_started_ts, "%m-%d %H:%M:%S"),
            "target_hits": self.target_hits, "last_target": self.last_target,
        }

    def snapshot(self, detail: Optional[str] = None, view: Optional[str] = None) -> dict:
        """Estado para la web. `detail` = bot del que van señales, cierres y estadísticas
        (None = todos); `view` = vista abierta del panel: solo se mandan sus partes pesadas
        (None = todas, compatibilidad de la API)."""
        now = time.time()
        parts = self.VIEW_PARTS.get(view or "", self.ALL_PARTS)
        with self.lock:
            events = list(self.events)[:80] if "events" in parts else []
        ks = self.kline_stream.get_stats() if self.kline_stream else {}
        k1 = (ks.get("intervals") or {}).get(KLINE_INTERVAL, {})
        pc = self.price_cache
        return {
            "mode":            "señales al executor",
            "running":         self.running,
            "uptime_s":        now - self.started_at,
            "config": {
                "notional": NOTIONAL_USDT, "stop_loss": None, "cooldown_s": COOLDOWN_SECONDS,
                "admin_token": bool(ADMIN_TOKEN),
            },
            "executor": {
                "url": self.executor.target.url, "sent": self.executor.sent,
                "errors": self.executor.errors, "last_error": self.executor.last_error,
                "last_ok": _utc(self.executor.last_ok_ts, "%H:%M:%S") if self.executor.last_ok_ts else "",
                "http_ms": round(self.executor.last_http_ms, 1),
                "total_ms": round(self.executor.last_total_ms, 1),
            },
            "klines": {
                "enabled": self.klines_enabled, "running": self.kline_stream is not None,
                "symbols": k1.get("symbols", 0), "with_data": k1.get("with_data", 0),
                "closed_candles": k1.get("closed_candles", 0),
                "live_updates": self.live_updates,
                "connections": ks.get("connections", 0), "active": ks.get("active_connections", 0),
                "reconnects": ks.get("reconnects", 0), "last_error": ks.get("last_error", ""),
            },
            "prices_connected": bool(pc and pc.is_connected()),
            "eval_total":      self.eval_count,
            "ratio":           self._ratio_snapshot("ratio_rows" in parts),
            "global":          self._global_snapshot(),
            "engines":         {n: self._engine_snapshot(e, now, parts if detail is None or n == detail
                                                         else frozenset())
                                for n, e in self.engines.items()},
            "view":            view or "",
            "net":             self.network_stats(),
            "events":          events,
            "last_error":      self.last_error,
            "persistence":     self.store.status(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# FLASK
# ─────────────────────────────────────────────────────────────────────────────

bot = KlineBot()
if os.getenv("BOTKLINE_AUTOSTART", "true").lower() not in ("0", "false", "no"):   # las pruebas no lo arrancan
    bot.start()
    atexit.register(bot.shutdown)

app = Flask(__name__, static_folder=None)
app.json.sort_keys = False
app.json.compact = True

_GZIP_TYPES = ("application/json", "text/html", "text/csv", "text/plain")
_GZIP_MIN_BYTES = 1024


@app.after_request
def _compress(resp):
    """gzip para JSON/HTML/CSV (el estado se comprime ~8-10×) y cuenta los bytes enviados."""
    try:
        if (resp.status_code == 200 and not resp.direct_passthrough
                and "gzip" in (request.headers.get("Accept-Encoding") or "").lower()
                and "Content-Encoding" not in resp.headers
                and (resp.mimetype or "") in _GZIP_TYPES):
            data = resp.get_data()
            if len(data) >= _GZIP_MIN_BYTES:
                resp.set_data(gzip.compress(data, compresslevel=6))
                resp.headers["Content-Encoding"] = "gzip"
                resp.headers["Content-Length"] = str(len(resp.get_data()))
            resp.vary.add("Accept-Encoding")
        if not resp.direct_passthrough:
            bot.tx_web_bytes += resp.calculate_content_length() or 0
    except Exception:
        pass
    return resp


def _no_store(resp):
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp




def _guard():
    """401 si hay ADMIN_TOKEN y no viene; 409 si esta instancia no controla el estado."""
    if ADMIN_TOKEN:
        given = request.headers.get("X-Admin-Token") or (request.get_json(silent=True) or {}).get("token")
        if given != ADMIN_TOKEN:
            return jsonify({"ok": False, "error": "Token de administrador inválido"}), 401
    if not bot.store.can_act():
        mode = bot.store.status()["mode"]
        return jsonify({"ok": False, "error": f"Esta instancia no controla el estado ({mode}). "
                                             "Reintenta en unos segundos."}), 409
    return None


def _flag(v) -> bool:
    """Booleano del JSON: también acepta "false", "0", "no" u "off" como texto (curl a mano)."""
    if isinstance(v, str):
        return v.strip().lower() not in ("", "0", "false", "no", "off")
    return bool(v)


def _engine_param(data: Optional[dict] = None, default: Optional[str] = ENGINE_CLOSE):
    """Bot pedido en el body (`engine`) o en la URL (?engine=). (Engine | None, error)."""
    name = (data or {}).get("engine") or request.args.get("engine") or default
    if name is None:
        return None, None
    eng = bot.engine(name)
    if eng is None:
        return None, (jsonify({"ok": False, "error": f"engine debe ser {' o '.join(bot.engines)}"}), 400)
    return eng, None


@app.get("/")
def index():
    """Panel. Con ETag: al recargar, el navegador recibe un 304 vacío si no cambió."""
    if PAGE_ETAG in (request.headers.get("If-None-Match") or ""):
        resp = make_response("", 304)
    else:
        resp = make_response(PAGE)
        resp.mimetype = "text/html"
    resp.set_etag(PAGE_ETAG)
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.get("/api/status")
def api_status():
    """Estado completo. ?engine=close|live|ratio manda el detalle solo de ese bot (la web lo usa)."""
    engine = (request.args.get("engine") or "").lower() or None
    if engine is not None and engine not in bot.engines:
        engine = None
    view = (request.args.get("view") or "").lower() or None
    return _no_store(jsonify(bot.snapshot(engine, view)))


@app.get("/api/executor")
def api_executor_get():
    t = bot.executor.target
    return _no_store(jsonify({"ok": True, "url": t.url, "has_secret": bool(t.secret)}))


@app.post("/api/executor")
def api_executor_set():
    """Cambia el link del Executor en caliente (común a los dos bots). Body JSON:
    {"url": "https://...", "secret": "opcional", "token": "si hay ADMIN_TOKEN"}.
    url vacío = dejar de mandar señales."""
    busy = _guard()
    if busy:
        return busy
    data = request.get_json(silent=True) or {}
    url = ExecutorBridge.clean_url(str(data.get("url", "")))
    if url and not url.startswith(("http://", "https://")):
        return jsonify({"ok": False, "error": "El link debe empezar por http:// o https://"}), 400
    secret = data.get("secret")
    secret = str(secret) if secret not in (None, "") else None
    target = bot.set_executor(url, secret)
    reachable, detail = bot.executor.probe(url) if url else (False, "sin URL: señales desactivadas")
    return jsonify({"ok": True, "url": target.url, "reachable": reachable, "detail": detail})


@app.get("/api/config")
def api_config_get():
    """Parámetros de los dos bots, o de uno con ?engine=close|live."""
    eng, err = _engine_param(default=None)
    if err:
        return err
    engines = [eng] if eng else list(bot.engines.values())
    return _no_store(jsonify({"ok": True, "engines": {e.name: e.settings() for e in engines}}))


@app.post("/api/config")
def api_config_set():
    """Cambia en caliente el % de la señal y/o del TP de un bot. Body JSON:
    {"engine": "close"|"live", "threshold_pct": 3, "tp_pct": 2, "token": "si hay ADMIN_TOKEN"}.
    Sin engine se cambia el bot al cierre."""
    busy = _guard()
    if busy:
        return busy
    data = request.get_json(silent=True) or {}
    eng, err = _engine_param(data)
    if err:
        return err
    if eng.name in RATIO_ENGINES:
        return jsonify({"ok": False, "error": "Los bots de ratio se ajustan en /api/ratio"}), 400
    vals = {}
    for key in ("threshold_pct", "tp_pct"):
        if data.get(key) in (None, ""):
            continue
        try:
            v = float(data[key])
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": f"{key} inválido"}), 400
        if not 0 < v <= 100:
            return jsonify({"ok": False, "error": f"{key} debe estar entre 0 y 100"}), 400
        vals[key] = v
    if not vals:
        return jsonify({"ok": False, "error": "Nada que cambiar"}), 400
    return jsonify({"ok": True, **bot.set_config(eng, **vals)})


@app.get("/api/global")
def api_global_get():
    return _no_store(jsonify({"ok": True, **bot._global_snapshot()}))


@app.post("/api/global")
def api_global_set():
    """Ajustes comunes a los dos bots. Body JSON (todo opcional):
    {"inverted": true, "profit_target_usd": 12, "fee_pct": 0.05}.
    profit_target_usd 0 = sin objetivo global."""
    busy = _guard()
    if busy:
        return busy
    data = request.get_json(silent=True) or {}
    if "klines_enabled" in data:
        want = _flag(data["klines_enabled"])
        if bot.set_klines(want) != want:
            return jsonify({"ok": False, "error": "No se pudieron abrir las velas (sin lista de símbolos todavía)"}), 409
        if len(data) == 1 or set(data) <= {"klines_enabled", "token"}:
            return jsonify({"ok": True, **bot.global_settings()})
    vals = {}
    if "inverted" in data:
        vals["inverted"] = _flag(data["inverted"])
    if "executor_paused" in data:
        vals["executor_paused"] = _flag(data["executor_paused"])
    if data.get("max_open") not in (None, ""):
        try:
            v = int(float(data["max_open"]))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "max_open inválido"}), 400
        if not 0 <= v <= 10_000:
            return jsonify({"ok": False, "error": "max_open debe estar entre 0 y 10000"}), 400
        vals["max_open"] = v
    for key, hi in (("profit_target_usd", 1_000_000), ("fee_pct", 1)):
        if data.get(key) in (None, ""):
            continue
        try:
            v = float(data[key])
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": f"{key} inválido"}), 400
        if not 0 <= v <= hi:
            return jsonify({"ok": False, "error": f"{key} debe estar entre 0 y {hi:g}"}), 400
        vals[key] = v
    if not vals:
        return jsonify({"ok": False, "error": "Nada que cambiar"}), 400
    return jsonify({"ok": True, **bot.set_global(**vals)})


@app.get("/api/ratio")
def api_ratio_get():
    return _no_store(jsonify({"ok": True, **bot._ratio_snapshot()}))


@app.post("/api/ratio")
def api_ratio_set():
    """Ajusta el bot de ratio en caliente. Body JSON (todo opcional): {"watch_min_pct": 20,
    "drop_min_pct": 25, "drop_max_pct": 30, "tp_pct": 10, "sl_pct": 5, "ratio_exit": true}.
    tp_pct/sl_pct = TP y SL por precio en % de la entrada (0 = apagado; el SL se puede
    mandar como -5); también se aplican a las posiciones abiertas. LONG/SHORT se activan
    con /api/block ({"engine": "ratio", "direction": "LONG", "blocked": false})."""
    busy = _guard()
    if busy:
        return busy
    data = request.get_json(silent=True) or {}
    if data.get("engine") == ENGINE_RATIO_REV:
        return _rev_set(data)
    vals = {}
    for key, lo, hi in (("watch_min_pct", 0, 1000), ("drop_min_pct", 0, 100), ("drop_max_pct", 0, 100),
                        ("tp_pct", None, 1000), ("sl_pct", None, 99.9)):
        if data.get(key) in (None, ""):
            continue
        try:
            v = float(data[key])
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": f"{key} inválido"}), 400
        if lo is None:
            v = abs(v)                                      # TP/SL: 0 = apagado, el signo da igual
            if v > hi:
                return jsonify({"ok": False, "error": f"{key} debe estar entre 0 y {hi:g}"}), 400
        elif not lo < v <= hi:
            return jsonify({"ok": False, "error": f"{key} debe estar entre 0 y {hi:g}"}), 400
        vals[key] = v
    if "ratio_exit" in data:
        vals["ratio_exit"] = _flag(data["ratio_exit"])
    if not vals:
        return jsonify({"ok": False, "error": "Nada que cambiar"}), 400
    try:
        cfg = bot.set_ratio_config(**vals)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, **cfg})


def _rev_set(data: dict):
    """Bot ratio + venganza: {"engine": "ratio_rev", "tp_pct": 10, "sl_pct": 5, "ratio_exit": true,
    "revenge": true, "rev_mult": 10, "rev_recover_pct": 65, "rev_sl_pct": 100, "rev_dir": "opposite"|"same"}."""
    vals = {}
    for key in ("tp_pct", "sl_pct", "rev_mult", "rev_recover_pct", "rev_sl_pct"):
        if data.get(key) in (None, ""):
            continue
        try:
            vals[key] = abs(float(data[key]))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": f"{key} inválido"}), 400
    for key in ("ratio_exit", "revenge"):
        if key in data:
            vals[key] = _flag(data[key])
    if data.get("rev_dir"):
        vals["rev_dir"] = str(data["rev_dir"])
    if not vals:
        return jsonify({"ok": False, "error": "Nada que cambiar"}), 400
    try:
        cfg = bot.set_rev_config(**vals)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, **cfg})


@app.post("/api/session/reset")
def api_session_reset():
    """Pone a cero el contador del objetivo global (realizado + abierto)."""
    busy = _guard()
    if busy:
        return busy
    bot.reset_session("desde la web")
    return jsonify({"ok": True, **bot._global_snapshot()})


@app.post("/api/realized/reset")
def api_realized_reset():
    """Pone a cero el PnL realizado de un bot. Body JSON: {"engine": "ratio"}."""
    busy = _guard()
    if busy:
        return busy
    eng, err = _engine_param(request.get_json(silent=True) or {}, default=None)
    if err:
        return err
    if eng is None:
        return jsonify({"ok": False, "error": f"Falta engine ({' o '.join(bot.engines)})"}), 400
    return jsonify({"ok": True, **bot.reset_realized(eng)})


@app.post("/api/pause")
def api_pause():
    """Pausa o reanuda un bot. Body JSON: {"engine": "ratio"|"ratio_tp"|"ratio_rev"|"close"|"live",
    "paused": true|false}."""
    busy = _guard()
    if busy:
        return busy
    data = request.get_json(silent=True) or {}
    eng, err = _engine_param(data, default=None)
    if err:
        return err
    if eng is None:
        return jsonify({"ok": False, "error": f"Falta engine ({' o '.join(bot.engines)})"}), 400
    return jsonify({"ok": True, **bot.set_paused(eng, _flag(data.get("paused")))})


@app.post("/api/close-all")
def api_close_all():
    """Cierra TODAS las posiciones abiertas (?engine= o body engine para un solo bot)."""
    busy = _guard()
    if busy:
        return busy
    eng, err = _engine_param(request.get_json(silent=True) or {}, default=None)
    if err:
        return err
    if not bot.loop or not bot.loop.is_running():
        return jsonify({"ok": False, "error": "El loop del bot no está activo"}), 503

    async def _run():
        return bot.close_all(eng)

    try:
        closed, total = asyncio.run_coroutine_threadsafe(_run(), bot.loop).result(timeout=15)
    except concurrent.futures.TimeoutError:
        return jsonify({"ok": False, "error": "Tiempo de espera agotado"}), 504
    msg = f"{closed} de {total} posición(es) cerrada(s)" if total else "No había posiciones abiertas"
    return jsonify({"ok": True, "closed": closed, "total": total, "msg": msg})


@app.post("/api/block")
def api_block():
    """Bloquea/desbloquea entradas por dirección en un bot. Body JSON:
    {"engine": "ratio"|"ratio_tp"|"ratio_rev"|"close"|"live", "direction": "LONG"|"SHORT", "blocked": true|false}."""
    busy = _guard()
    if busy:
        return busy
    data = request.get_json(silent=True) or {}
    eng, err = _engine_param(data)
    if err:
        return err
    direction = str(data.get("direction", "")).upper()
    if direction not in (LONG, SHORT):
        return jsonify({"ok": False, "error": "direction debe ser LONG o SHORT"}), 400
    return jsonify({"ok": True, **bot.set_block(eng, direction, _flag(data.get("blocked")))})


@app.post("/api/close/<symbol>")
def api_close(symbol: str):
    """Cierre manual: /api/close/<trade_id> cierra esa posición (de cualquier
    bot); /api/close/<SÍMBOLO> cierra todas las del par (?engine= limita a un bot)."""
    busy = _guard()
    if busy:
        return busy
    eng, err = _engine_param(default=None)
    if err:
        return err
    if not bot.loop or not bot.loop.is_running():
        return jsonify({"ok": False, "error": "El loop del bot no está activo"}), 503
    symbol = symbol.upper().strip()

    async def _run():
        return bot.close_manual(symbol, eng)

    try:
        ok, msg = asyncio.run_coroutine_threadsafe(_run(), bot.loop).result(timeout=10)
    except concurrent.futures.TimeoutError:
        return jsonify({"ok": False, "error": "Tiempo de espera agotado"}), 504
    return jsonify({"ok": ok, "symbol": symbol, "msg": msg}), (200 if ok else 404)


def _trade_rows():
    """Cierres del bot pedido con ?engine=, o de los dos (más reciente primero)."""
    eng, err = _engine_param(default=None)
    if err:
        return None, err
    with bot.lock:
        engines = [eng] if eng else list(bot.engines.values())
        rows = [r for e in engines for r in e.closed_trades]
    rows.sort(key=lambda r: float(r.get("closed_at_ts") or 0), reverse=True)
    return rows, None


@app.get("/api/trades")
def api_trades():
    rows, err = _trade_rows()
    return err or _no_store(jsonify(rows))


TRADE_CSV_COLS = [
    "strategy", "trade_id", "symbol", "direction", "reason", "opened_ts", "closed_at_ts", "duration_s",
    "inverted", "signal_pct", "candle_age_s", "entry_price", "close_price", "qty", "notional", "tp_price",
    "sent", "move_pct", "pnl", "pnl_gross", "fees", "pnl_pct", "mfe_pct", "mae_pct", "mfe_usd", "mae_usd", "time_to_mfe_s",
    "time_to_mae_s", "high_price", "low_price", "sl_price", "ratio_target", "ratio_trigger", "ratio_entry", "ratio_change_pct",
    "revenge", "rev_parent", "rev_tp_usd", "rev_sl_usd",
]


@app.get("/api/trades.csv")
def api_trades_csv():
    """Historial con MFE/MAE para analizarlo en Excel o pandas (?engine= para un bot)."""
    rows, err = _trade_rows()
    if err:
        return err
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(TRADE_CSV_COLS)
    for r in reversed(rows):
        w.writerow([r.get(c, "close" if c == "strategy" else "") for c in TRADE_CSV_COLS])
    name = request.args.get("engine") or "todos"
    resp = make_response(buf.getvalue())
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = f"attachment; filename=botkline_trades_{name}.csv"
    return _no_store(resp)


@app.get("/health")
def health():
    """Chequeo ligero (no arma el estado completo)."""
    pc = bot.price_cache
    with bot.lock:
        positions = {n: len(e.positions) for n, e in bot.engines.items()}
    return _no_store(jsonify({
        "ok": True, "running": bot.running, "positions": positions,
        "prices_connected": bool(pc and pc.is_connected()), "ticker_ok": bot.ticker_connected,
        "executor": bot.executor.target.url, "persistence": bot.store.status().get("mode"),
        "last_error": bot.last_error, "net": bot.network_stats(),
    }))


@app.get("/api/recovery")
def api_recovery():
    doc = bot.store.last_doc or bot.store.read_local()
    if isinstance(doc, dict) and isinstance(doc.get("executor"), dict):
        doc = {**doc, "executor": {**doc["executor"], "secret": "***"}}
    return _no_store(jsonify({"status": bot.store.status(), "document": doc}))




# ─────────────────────────────────────────────────────────────────────────────
# WEB (panel: tema claro / oscuro / automático)
# ─────────────────────────────────────────────────────────────────────────────

HTML = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>BOTKLINE · panel</title>
<script>
  // Tema antes de pintar (sin parpadeo): auto (sistema) | light | dark.
  (function () { try { var t = localStorage.getItem('botkline_theme'); if (t === 'light' || t === 'dark') document.documentElement.dataset.theme = t; } catch (e) {} })();
</script>
<style>
  :root {
    --bg:#f3f5fa; --surface:#ffffff; --surface-2:#f8f9fc; --surface-3:#eef1f7; --line:#e2e6ef; --line-strong:#cfd6e3;
    --txt:#101828; --muted:#5f6b7f; --faint:#98a2b3;
    --accent:#4f46e5; --accent-soft:rgba(79,70,229,.10); --accent-txt:#ffffff;
    --green:#059669; --green-soft:rgba(5,150,105,.11); --red:#dc2626; --red-soft:rgba(220,38,38,.10);
    --amber:#b45309; --amber-soft:rgba(217,119,6,.13); --on-solid:#ffffff;
    --shadow:0 1px 2px rgba(16,24,40,.04), 0 4px 14px rgba(16,24,40,.05);
    --radius:16px; --radius-sm:10px;
  }
  @media (prefers-color-scheme: dark) {
    :root:not([data-theme="light"]) {
      --bg:#0a0e16; --surface:#111724; --surface-2:#0e131e; --surface-3:#192131; --line:#222b3d; --line-strong:#2e3a51;
      --txt:#e7ebf3; --muted:#8c98ae; --faint:#5d6880;
      --accent:#818cf8; --accent-soft:rgba(129,140,248,.15); --accent-txt:#0a0e16;
      --green:#34d399; --green-soft:rgba(52,211,153,.13); --red:#f87171; --red-soft:rgba(248,113,113,.13);
      --amber:#fbbf24; --amber-soft:rgba(251,191,36,.14); --on-solid:#0a0e16;
      --shadow:0 1px 2px rgba(0,0,0,.35), 0 8px 24px rgba(0,0,0,.25);
    }
  }
  :root[data-theme="dark"] {
    --bg:#0a0e16; --surface:#111724; --surface-2:#0e131e; --surface-3:#192131; --line:#222b3d; --line-strong:#2e3a51;
    --txt:#e7ebf3; --muted:#8c98ae; --faint:#5d6880;
    --accent:#818cf8; --accent-soft:rgba(129,140,248,.15); --accent-txt:#0a0e16;
    --green:#34d399; --green-soft:rgba(52,211,153,.13); --red:#f87171; --red-soft:rgba(248,113,113,.13);
    --amber:#fbbf24; --amber-soft:rgba(251,191,36,.14); --on-solid:#0a0e16;
    --shadow:0 1px 2px rgba(0,0,0,.35), 0 8px 24px rgba(0,0,0,.25);
  }
  * { box-sizing:border-box; }
  html { -webkit-text-size-adjust:100%; }
  body { margin:0; background:var(--bg); color:var(--txt);
         font:14px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
         font-variant-numeric:tabular-nums; }
  a { color:var(--accent); text-decoration:none; } a:hover { text-decoration:underline; }
  .muted { color:var(--muted); } .faint { color:var(--faint); }
  .pos { color:var(--green); } .neg { color:var(--red); } .warn { color:var(--amber); }
  small { font-size:12px; }
  [hidden] { display:none !important; }

  /* Barra superior + menú */
  .appbar { position:sticky; top:0; z-index:20; background:color-mix(in srgb, var(--surface) 92%, transparent);
            backdrop-filter:blur(10px); border-bottom:1px solid var(--line); }
  .appbar-in { max-width:1320px; margin:0 auto; padding:10px 16px; display:flex; align-items:center; gap:12px; }
  .brand { display:flex; align-items:center; gap:10px; min-width:0; }
  .logo { width:32px; height:32px; border-radius:9px; background:linear-gradient(135deg,var(--accent),#22d3ee);
          display:grid; place-items:center; color:#fff; font-weight:800; font-size:14px; flex:none; }
  .brand b { font-size:16px; letter-spacing:.2px; } .brand small { display:block; color:var(--muted); font-size:11px; margin-top:-2px; }
  nav.menu { display:flex; gap:2px; margin-left:12px; overflow-x:auto; scrollbar-width:none; }
  nav.menu::-webkit-scrollbar { display:none; }
  nav.menu a { padding:7px 12px; border-radius:9px; color:var(--muted); font-weight:600; font-size:13px; white-space:nowrap; text-decoration:none; }
  nav.menu a:hover { background:var(--surface-3); color:var(--txt); }
  nav.menu a.on { background:var(--accent-soft); color:var(--accent); }
  nav.menu a .cnt { margin-left:5px; font-size:11px; background:var(--surface-3); color:var(--muted); border-radius:99px; padding:0 6px; }
  nav.menu a.on .cnt { background:var(--accent); color:var(--accent-txt); }
  .bar-r { margin-left:auto; display:flex; gap:8px; align-items:center; flex:none; }
  .health { display:flex; gap:5px; }
  .dot { width:9px; height:9px; border-radius:50%; background:var(--faint); display:inline-block; }
  .dot.ok { background:var(--green); } .dot.bad { background:var(--red); } .dot.alert { background:var(--amber); }
  .icon-btn { background:var(--surface); color:var(--txt); border:1px solid var(--line); border-radius:9px; padding:6px 10px; font-weight:600; font-size:12px; }
  .icon-btn:hover { background:var(--surface-3); filter:none; }
  #btnMenu { display:none; }
  @media (max-width:900px) {
    .appbar-in { flex-wrap:wrap; }
    #btnMenu { display:inline-block; }
    nav.menu { order:3; width:100%; margin:0; flex-direction:column; display:none; padding-top:6px; }
    nav.menu.open { display:flex; }
    nav.menu a { padding:10px 12px; }
    .brand small { display:none; }
  }

  main { max-width:1320px; margin:0 auto; padding:18px 16px 56px; }
  .view-h { display:flex; align-items:flex-end; justify-content:space-between; gap:12px; flex-wrap:wrap; margin:4px 0 14px; }
  .view-h h2 { margin:0; font-size:20px; letter-spacing:-.2px; }
  .view-h p { margin:2px 0 0; color:var(--muted); font-size:13px; }
  .pills { display:flex; flex-wrap:wrap; gap:6px; }
  .pill { display:inline-flex; align-items:center; gap:6px; border:1px solid var(--line); background:var(--surface);
          border-radius:999px; padding:3px 10px; font-size:12px; color:var(--muted); }
  .pill::before { content:''; width:7px; height:7px; border-radius:50%; background:var(--faint); }
  .pill.ok::before { background:var(--green); } .pill.bad::before { background:var(--red); } .pill.alert::before { background:var(--amber); }
  .pill.ok { color:var(--txt); } .pill.bad { color:var(--red); } .pill.alert { color:var(--amber); }

  /* Cifras */
  .kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; margin-bottom:16px; }
  .kpi { background:var(--surface); border:1px solid var(--line); border-radius:var(--radius); padding:14px 16px; box-shadow:var(--shadow); }
  .kpi .label { color:var(--muted); font-size:12px; font-weight:500; }
  .kpi .v { font-size:22px; font-weight:700; margin-top:3px; letter-spacing:-.3px; }
  .meter { height:6px; background:var(--surface-3); border-radius:99px; overflow:hidden; margin-top:10px; }
  .meter > i { display:block; height:100%; width:0; background:linear-gradient(90deg,var(--green),#22d3ee); transition:width .4s; }

  /* Paneles desplegables */
  details.panel { background:var(--surface); border:1px solid var(--line); border-radius:var(--radius); margin-bottom:12px; box-shadow:var(--shadow); }
  details.panel > summary { list-style:none; cursor:pointer; padding:14px 18px; font-weight:650; font-size:15px;
                            display:flex; gap:8px; align-items:center; flex-wrap:wrap; user-select:none; }
  details.panel > summary::-webkit-details-marker { display:none; }
  details.panel > summary::before { content:''; width:7px; height:7px; border-right:2px solid var(--muted); border-bottom:2px solid var(--muted);
                                    transform:rotate(-45deg); transition:transform .15s; margin-right:4px; flex:none; }
  details.panel[open] > summary::before { transform:rotate(45deg); }
  details.panel > summary .hint { font-weight:400; font-size:12px; color:var(--muted); }
  details.panel > summary .right { margin-left:auto; display:flex; gap:6px; align-items:center; }
  .body { padding:0 18px 16px; }
  .badge { background:var(--surface-3); border-radius:999px; padding:1px 10px; font-size:12px; font-weight:600; color:var(--muted); }
  .sub { font-size:11px; font-weight:700; margin:16px 0 8px; color:var(--muted); text-transform:uppercase; letter-spacing:.6px; }
  .note { font-size:12px; color:var(--muted); margin-top:8px; }

  /* Tarjetas de bots */
  .bots { display:grid; grid-template-columns:repeat(auto-fit,minmax(290px,1fr)); gap:12px; margin-bottom:16px; }
  .bot { background:var(--surface); border:1px solid var(--line); border-radius:var(--radius); padding:16px; box-shadow:var(--shadow);
         display:flex; flex-direction:column; gap:10px; }
  .bot.paused { border-style:dashed; }
  .bot.off { opacity:.75; }
  .bot .top { display:flex; align-items:center; gap:8px; }
  .bot h3 { margin:0; font-size:15px; }
  .bot .top .st { margin-left:auto; }
  .bot .hint { font-size:12px; color:var(--muted); }
  .kv { display:grid; grid-template-columns:repeat(3,1fr); gap:8px; }
  .kv div { font-size:11px; color:var(--muted); text-transform:uppercase; letter-spacing:.3px; }
  .kv b { display:block; color:var(--txt); font-size:14px; text-transform:none; letter-spacing:0; }
  .bot .acts { display:flex; gap:6px; flex-wrap:wrap; margin-top:auto; }

  /* Tarjetas de operaciones activas */
  .trades { display:grid; grid-template-columns:repeat(auto-fill,minmax(300px,1fr)); gap:12px; }
  .trade { background:var(--surface-2); border:1px solid var(--line); border-radius:14px; padding:14px; display:flex; flex-direction:column; gap:10px; }
  .trade.win { border-left:3px solid var(--green); } .trade.loss { border-left:3px solid var(--red); }
  .trade .h { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .trade .sym { font-weight:700; font-size:15px; }
  .trade .bl { margin-left:auto; font-size:11px; color:var(--muted); }
  .trade .pnl { display:flex; align-items:baseline; gap:8px; }
  .trade .pnl b { font-size:22px; letter-spacing:-.3px; }
  .trade .grid { display:grid; grid-template-columns:repeat(3,1fr); gap:6px 8px; font-size:12px; color:var(--muted); }
  .trade .grid span b { display:block; color:var(--txt); font-size:13px; font-weight:600; }
  .range { position:relative; height:8px; border-radius:99px; background:linear-gradient(90deg,var(--red-soft),var(--surface-3) 50%,var(--green-soft)); }
  .range i { position:absolute; top:-3px; width:3px; height:14px; border-radius:2px; background:var(--txt); transform:translateX(-1px); }
  .range-l { display:flex; justify-content:space-between; font-size:11px; color:var(--muted); margin-top:3px; }
  .trade .f { display:flex; align-items:center; gap:8px; font-size:12px; color:var(--muted); }
  .trade .f button { margin-left:auto; }
  .empty { padding:22px; text-align:center; color:var(--muted); border:1px dashed var(--line-strong); border-radius:14px; }

  /* Selector segmentado */
  .seg { display:inline-flex; background:var(--surface-3); border-radius:11px; padding:3px; gap:2px; flex-wrap:wrap; }
  .seg button { background:transparent; color:var(--muted); border:0; padding:6px 12px; border-radius:8px; font-weight:600; font-size:13px; }
  .seg button:hover { color:var(--txt); filter:none; }
  .seg button.on { background:var(--surface); color:var(--txt); box-shadow:var(--shadow); }
  .seg button * { pointer-events:none; }

  /* Tablas */
  .scroll { overflow:auto; max-height:460px; border:1px solid var(--line); border-radius:var(--radius-sm); }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { padding:8px 10px; border-bottom:1px solid var(--line); text-align:right; white-space:nowrap; }
  th:first-child, td:first-child { text-align:left; }
  tbody tr:last-child td { border-bottom:0; }
  thead th { position:sticky; top:0; background:var(--surface-3); color:var(--muted); font-weight:600; font-size:12px; z-index:1; }
  tbody tr:hover td { background:var(--accent-soft); }
  .tag { padding:2px 8px; border-radius:6px; font-weight:700; font-size:11px; letter-spacing:.3px; }
  .LONG { background:var(--green-soft); color:var(--green); } .SHORT { background:var(--red-soft); color:var(--red); }
  .st { padding:2px 8px; border-radius:6px; font-size:11px; font-weight:700; }
  .st.on { background:var(--green-soft); color:var(--green); } .st.off { background:var(--amber-soft); color:var(--amber); }
  .st.idle { background:var(--surface-3); color:var(--muted); }

  /* Formularios y botones */
  form, .row { display:flex; gap:10px; flex-wrap:wrap; align-items:flex-end; }
  .row.c { align-items:center; }
  input { flex:1 1 180px; min-width:0; background:var(--surface-2); color:var(--txt); border:1px solid var(--line-strong); border-radius:10px;
          padding:9px 11px; font:inherit; outline:none; transition:border-color .15s, box-shadow .15s; }
  input:focus { border-color:var(--accent); box-shadow:0 0 0 3px var(--accent-soft); }
  label.f { flex:1 1 180px; display:flex; flex-direction:column; gap:5px; font-size:12px; color:var(--muted); font-weight:500; }
  label.f input { flex:none; }
  button { background:var(--accent); color:var(--accent-txt); border:1px solid transparent; border-radius:10px; padding:9px 15px;
           cursor:pointer; font:inherit; font-weight:650; transition:filter .15s, background .15s; }
  button:hover { filter:brightness(1.07); }
  button.ghost { background:var(--surface-3); color:var(--txt); border-color:var(--line); }
  button.danger { background:var(--red); color:var(--on-solid); }
  button.ok { background:var(--green); color:var(--on-solid); }
  button.warnb { background:var(--amber); color:var(--on-solid); }
  button.small { padding:5px 11px; font-size:12px; }
  .msg { margin-top:8px; min-height:18px; font-size:13px; }
  .cols2 { display:grid; grid-template-columns:repeat(auto-fit,minmax(320px,1fr)); gap:14px; }
  .box { background:var(--surface-2); border:1px solid var(--line); border-radius:14px; padding:14px; }
  .box h4 { margin:0 0 10px; font-size:13px; }
  .rev { display:flex; flex-direction:column; gap:12px; border-color:var(--accent); }
  .rev-h { display:flex; align-items:center; justify-content:space-between; gap:10px; }
  .rev-row { display:flex; align-items:center; flex-wrap:wrap; gap:8px; font-size:13px; }
  .rev-row > span:first-child { color:var(--muted); min-width:170px; }
  input.mini { flex:none; width:84px; padding:6px 8px; text-align:center; font-weight:700; }
  .switch { width:46px; height:26px; border-radius:99px; padding:0; position:relative; background:var(--line-strong); border:0; }
  .switch i { position:absolute; top:3px; left:3px; width:20px; height:20px; border-radius:50%; background:#fff; transition:left .15s; }
  .switch.on { background:var(--accent); }
  .switch.on i { left:23px; }
  .tag.REV { background:var(--amber-soft); color:var(--amber); }
  pre { margin:0; max-height:420px; overflow:auto; font:12px/1.55 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; color:var(--muted);
        white-space:pre-wrap; background:var(--surface-2); border:1px solid var(--line); border-radius:var(--radius-sm); padding:12px; }
  .netgrid { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:8px; }
  .netgrid div { background:var(--surface-2); border:1px solid var(--line); border-radius:10px; padding:10px 12px; font-size:12px; color:var(--muted); }
  .netgrid b { display:block; font-size:15px; color:var(--txt); }
  @media (max-width:640px) { .kv, .trade .grid { grid-template-columns:repeat(2,1fr); } .kpi .v { font-size:19px; } main { padding-top:12px; } }
</style>
</head>
<body>
<header class="appbar">
  <div class="appbar-in">
    <div class="brand"><div class="logo">BK</div><div><b>BOTKLINE</b><small>señales al executor · Binance USDⓈ-M</small></div></div>
    <nav class="menu" id="menu">
      <a href="#resumen" data-view="resumen">Resumen</a>
      <a href="#operaciones" data-view="operaciones">Operaciones <span class="cnt" id="mOpen">0</span></a>
      <a href="#bots" data-view="bots">Bots</a>
      <a href="#mercado" data-view="mercado">Mercado</a>
      <a href="#historial" data-view="historial">Historial</a>
      <a href="#sistema" data-view="sistema">Sistema</a>
    </nav>
    <div class="bar-r">
      <span class="health" id="health" title="Precios · 24 h · executor · estado"></span>
      <button type="button" class="icon-btn" id="btnTheme" title="Tema claro / oscuro">Tema</button>
      <button type="button" class="icon-btn" id="btnMenu" aria-label="Menú">☰ Menú</button>
    </div>
  </div>
</header>

<main>
  <!-- ───────────── RESUMEN ───────────── -->
  <section data-view="resumen">
    <div class="view-h"><div><h2>Resumen</h2><p id="rules"></p></div><div class="pills" id="pills"></div></div>
    <section class="kpis">
      <div class="kpi"><div class="label">PnL de la sesión <span id="sessTarget"></span></div><div class="v" id="sessPnl">—</div>
        <div class="meter"><i id="sessBar"></i></div><small class="muted" id="sessDetail"></small></div>
      <div class="kpi"><div class="label">PnL realizado (total)</div><div class="v" id="sRpnlAll">—</div><small class="muted" id="sWins"></small></div>
      <div class="kpi"><div class="label">PnL abierto (total)</div><div class="v" id="sUpnlAll">—</div><small class="muted" id="sUpnlBy"></small></div>
      <div class="kpi"><div class="label">Posiciones abiertas</div><div class="v" id="sOpen">—</div><small class="muted" id="sOpenLim"></small></div>
      <div class="kpi"><div class="label">Vigilados por el bot de ratio</div><div class="v" id="sWatch">—</div><small class="muted" id="sWatchInfo"></small></div>
    </section>

    <div class="bots" id="botCards"></div>

    <details class="panel" data-key="r_actions" open>
      <summary>Acciones generales <span class="hint">afectan a todos los bots</span></summary>
      <div class="body">
        <div class="row c">
          <button type="button" data-act="invert" id="btnInvert">—</button>
          <button type="button" data-act="exPause" id="btnExPause">—</button>
          <button type="button" class="ghost" data-act="reset">Poner a cero el PnL de la sesión</button>
          <button type="button" class="danger" data-act="closeAll">Cerrar TODAS las posiciones</button>
        </div>
        <div id="actMsg" class="muted msg"></div>
        <div class="note" id="modeHint"></div>
        <div class="note" id="sessHits"></div>
      </div>
    </details>

    <details class="panel" data-key="r_open" open>
      <summary>Operaciones activas <span class="hint">todos los bots</span><span class="right"><span class="badge" id="rOpenN">0</span><a href="#operaciones" onclick="event.stopPropagation()" class="hint">ver todas →</a></span></summary>
      <div class="body"><div class="trades" id="rTrades"></div></div>
    </details>
  </section>

  <!-- ───────────── OPERACIONES ───────────── -->
  <section data-view="operaciones" hidden>
    <div class="view-h">
      <div><h2>Operaciones activas</h2><p id="opSum"></p></div>
      <div class="row c"><div class="seg" id="opSeg"></div>
        <button type="button" class="danger small" data-act="closeEng" id="btnCloseEng">Cerrar todas</button></div>
    </div>
    <div class="trades" id="opTrades"></div>

    <details class="panel" data-key="o_table" style="margin-top:16px">
      <summary>Vista en tabla <span class="hint">las mismas operaciones, con todos los datos</span></summary>
      <div class="body"><div class="scroll"><table>
        <thead><tr><th>Símbolo</th><th>Bot</th><th>Dir.</th><th>Señal</th><th>Entrada</th><th>Precio</th><th>Avance</th><th>Salida</th><th>Notional</th><th>PnL neto</th><th>MFE</th><th>MAE</th><th>Abierta</th><th></th></tr></thead>
        <tbody id="tPos"></tbody></table></div></div>
    </details>

    <details class="panel" data-key="o_sig" open>
      <summary>Señales recientes · <span class="engName"></span> <span class="right"><span class="seg" id="sigSeg"></span><span class="badge" id="nSig">0</span></span></summary>
      <div class="body"><div class="scroll"><table>
        <thead><tr><th>Hora</th><th>Símbolo</th><th>Dir.</th><th id="thSigC">Vela</th><th id="thSigB">Cuerpo</th><th id="thSigS">Segundo</th><th>Acción</th></tr></thead>
        <tbody id="tSig"></tbody></table></div></div>
    </details>
  </section>

  <!-- ───────────── BOTS ───────────── -->
  <section data-view="bots" hidden>
    <div class="view-h"><div><h2>Bots</h2><p>Parámetros de cada bot. Todo se aplica en caliente, sin reiniciar.</p></div></div>

    <details class="panel" data-key="b_ratio" open>
      <summary>Bots de ratio · entrada común <span class="hint" id="rtSum"></span></summary>
      <div class="body">
        <div class="cols2">
          <div class="box">
            <h4>Entrada <span class="muted" style="font-weight:400">· la usan los tres bots de ratio</span></h4>
            <form id="rtForm">
              <label class="f">Vigilar 24 h ≥ ±% <span id="rtCurW"></span>
                <input id="rtWatch" type="number" step="0.1" min="0.1" max="1000" placeholder="ej. 20"></label>
              <label class="f">Cambio mín. del ratio (caída o subida), % <span id="rtCurMin"></span>
                <input id="rtMin" type="number" step="0.1" min="0.1" max="100" placeholder="ej. 25"></label>
              <label class="f">Cambio máx. del ratio (caída o subida), % <span id="rtCurMax"></span>
                <input id="rtMax" type="number" step="0.1" min="0.1" max="100" placeholder="ej. 30"></label>
              <button type="submit">Guardar</button>
            </form>
          </div>
          <div class="box">
            <h4>Cómo funcionan</h4>
            <div class="note" style="margin-top:0"><b>Caída del ratio → SHORT</b> en ganadores (24 h ≥ +X %). <b>Subida del ratio → LONG</b> en perdedores (24 h ≤ −X %).
              Con cada señal, cada bot de ratio abre su propia operación (si no está pausado ni tiene esa dirección bloqueada) y la gestiona con su salida:</div>
            <ul class="note" style="padding-left:18px">
              <li><b>Ratio TP/SL</b>: TP y SL por precio + TP por ratio opcional.</li>
              <li><b>Ratio solo TP ratio</b>: sale solo cuando el ratio recupera el % que se movió.</li>
              <li><b>Ratio + venganza</b>: su propio TP/SL; si cierra en pérdida abre un trade de venganza.</li>
            </ul>
          </div>
        </div>
        <div id="rtMsg" class="muted msg"></div>
      </div>
    </details>

    <details class="panel" data-key="b_ratio1" open>
      <summary>Bot ratio TP/SL <span class="hint" id="r1Sum"></span><span class="right" id="r1State"></span></summary>
      <div class="body">
        <div class="cols2">
          <div class="box">
            <h4>Salida <span class="muted" style="font-weight:400">· también para las abiertas</span></h4>
            <form id="rtExitForm">
              <label class="f">Take profit, % a favor <span id="rtCurTp"></span>
                <input id="rtTp" type="number" step="0.1" min="0" max="1000" placeholder="ej. 10 (0 = sin TP)"></label>
              <label class="f">Stop loss, % en contra <span id="rtCurSl"></span>
                <input id="rtSl" type="number" step="0.1" min="-99.9" max="99.9" placeholder="ej. -5 (0 = sin SL)"></label>
              <button type="submit">Guardar</button>
            </form>
            <div class="row c" style="margin-top:10px">
              <button type="button" class="small" data-act="ratioExit" id="rtBtnExit">—</button>
              <span class="muted" style="font-size:12px">TP por ratio: cierra cuando el ratio recupera el mismo % que cayó (SHORT) o subió (LONG).</span>
            </div>
          </div>
          <div class="box"><h4>Direcciones y estado</h4><div class="row c" id="rb_ratio"></div></div>
        </div>
      </div>
    </details>

    <details class="panel" data-key="b_ratio_tp" open>
      <summary>Bot ratio solo TP ratio <span class="hint">sin TP/SL de precio</span><span class="right" id="r2State"></span></summary>
      <div class="body">
        <div class="cols2">
          <div class="box"><h4>Salida</h4>
            <div class="note" style="margin-top:0">Cierra solo cuando el ratio recupera el mismo % que cayó (SHORT) o subió (LONG), medido desde el ratio de entrada. No tiene take profit ni stop loss por precio.</div></div>
          <div class="box"><h4>Direcciones y estado</h4><div class="row c" id="rb_ratio_tp"></div></div>
        </div>
      </div>
    </details>

    <details class="panel" data-key="b_ratio_rev" open>
      <summary>Bot ratio + venganza <span class="hint" id="r3Sum"></span><span class="right" id="r3State"></span></summary>
      <div class="body">
        <div class="cols2">
          <div class="box">
            <h4>Salida <span class="muted" style="font-weight:400">· también para las abiertas (no las de venganza)</span></h4>
            <form id="rvExitForm">
              <label class="f">Take profit, % a favor <span id="rvCurTp"></span>
                <input id="rvTp" type="number" step="0.1" min="0" max="1000" placeholder="ej. 10 (0 = sin TP)"></label>
              <label class="f">Stop loss, % en contra <span id="rvCurSl"></span>
                <input id="rvSl" type="number" step="0.1" min="-99.9" max="99.9" placeholder="ej. -5 (0 = sin SL)"></label>
              <button type="submit">Guardar</button>
            </form>
            <div class="row c" style="margin-top:10px">
              <button type="button" class="small" data-act="revRatioExit" id="rvBtnExit">—</button>
            </div>
            <h4 style="margin-top:16px">Direcciones y estado</h4><div class="row c" id="rb_ratio_rev"></div>
          </div>
          <div class="box rev" id="revBox">
            <div class="rev-h"><h4 style="margin:0">⚔️ Venganza <span class="st" id="revState">—</span></h4>
              <button type="button" class="switch" data-act="revToggle" id="revSwitch" aria-label="Activar o desactivar la venganza"><i></i></button></div>
            <div class="rev-row"><span>Notional de la venganza</span>
              <span class="seg" id="revMultSeg"></span>
              <input id="revMult" class="mini" type="number" step="0.5" min="0.5" max="100" title="Multiplicador personalizado"></div>
            <div class="rev-row"><span>Recuperar de la pérdida</span>
              <input id="revRecover" class="mini" type="number" step="1" min="1" max="1000"><span class="muted">%</span></div>
            <div class="rev-row"><span>Stop loss de la venganza</span>
              <span class="seg" id="revSlSeg"></span>
              <input id="revSl" class="mini" type="number" step="1" min="1" max="1000"><span class="muted">%</span></div>
            <div class="rev-row"><span>Dirección</span><span class="seg" id="revDirSeg"></span></div>
            <div class="note" id="revNote"></div>
            <div id="revMsg" class="muted msg"></div>
          </div>
        </div>
      </div>
    </details>

    <details class="panel" data-key="b_klines" open>
      <summary>Bots de velas 1m <span class="hint">al cierre y en vivo · comparten velas y modo</span><span class="right" id="klState"></span></summary>
      <div class="body">
        <div class="row c">
          <button type="button" data-act="klines" id="btnKlines">—</button>
          <button type="button" data-act="invert" id="btnInvert2">—</button>
        </div>
        <div class="note" id="klNote"></div>
        <div class="cols2" style="margin-top:14px" id="klBots"></div>
        <div id="cfgMsg" class="muted msg"></div>
      </div>
    </details>
  </section>

  <!-- ───────────── MERCADO ───────────── -->
  <section data-view="mercado" hidden>
    <div class="view-h"><div><h2>Mercado</h2><p id="mkSum"></p></div></div>
    <details class="panel" data-key="m_ratio" open>
      <summary>Vigilados por el bot de ratio <span class="badge" id="rtN">0</span></summary>
      <div class="body">
        <div class="muted" id="rtInfo" style="font-size:13px;margin-bottom:10px"></div>
        <div class="scroll"><table>
          <thead><tr><th>Símbolo</th><th>24 h</th><th>Precio</th><th>Ratio antes</th><th>Ratio ahora</th><th>Cambio</th><th>Dato</th><th>Espera</th><th>Estado</th></tr></thead>
          <tbody id="tRt"></tbody></table></div>
      </div>
    </details>
    <details class="panel" data-key="m_eval">
      <summary>Evaluaciones de velas <span class="hint">cuerpo al cierre, con el umbral de</span> <span class="right"><span class="seg" id="evSeg"></span></span></summary>
      <div class="body">
        <div class="note" id="evNote" style="margin-top:0"></div>
        <div class="sub">Mayores cuerpos de la vela <span id="evMin"></span></div>
        <div class="scroll"><table>
          <thead><tr><th>Símbolo</th><th>Apertura</th><th>Cierre</th><th>Cuerpo</th><th>Señal</th></tr></thead>
          <tbody id="tTop"></tbody></table></div>
        <div class="sub">Por minuto</div>
        <div class="scroll"><table>
          <thead><tr><th>Vela</th><th>Evaluadas</th><th>≥ umbral (LONG)</th><th>≤ −umbral (SHORT)</th><th>Mayor subida</th><th>Mayor caída</th></tr></thead>
          <tbody id="tMin"></tbody></table></div>
      </div>
    </details>
  </section>

  <!-- ───────────── HISTORIAL ───────────── -->
  <section data-view="historial" hidden>
    <div class="view-h"><div><h2>Historial</h2><p id="hiSum"></p></div>
      <div class="row c"><div class="seg" id="hiSeg"></div><a id="csvLink" class="icon-btn" href="/api/trades.csv">Descargar CSV</a></div></div>
    <details class="panel" data-key="h_closed" open>
      <summary>Operaciones cerradas · <span class="engName"></span> <span class="right"><span class="badge" id="nClosed">0</span></span></summary>
      <div class="body"><div class="scroll"><table>
        <thead><tr><th>Cierre</th><th>Símbolo</th><th>Dir.</th><th>Entrada</th><th>Salida</th><th>Avance</th><th>PnL neto</th><th>Fees</th><th>MFE</th><th>MAE</th><th>Duración</th><th>Motivo</th></tr></thead>
        <tbody id="tClosed"></tbody></table></div>
        <div class="note">Se muestran las 100 últimas; el historial completo está en el CSV.</div></div>
    </details>
    <details class="panel" data-key="h_exc" open>
      <summary>MFE / MAE · <span class="engName"></span></summary>
      <div class="body"><div class="scroll"><table>
        <thead><tr><th>Dirección</th><th>Ops</th><th>Ganadas</th><th>MFE medio</th><th>MAE medio</th><th>Mejor MFE</th><th>Peor MAE</th><th>MAE medio USD</th><th>Hasta MAE</th><th>Duración media</th></tr></thead>
        <tbody id="tExc"></tbody></table></div></div>
    </details>
  </section>

  <!-- ───────────── SISTEMA ───────────── -->
  <section data-view="sistema" hidden>
    <div class="view-h"><div><h2>Sistema</h2><p>Ajustes generales, executor, red y eventos.</p></div></div>
    <details class="panel" data-key="s_glb" open>
      <summary>Ajustes generales <span class="hint" id="glbSum"></span></summary>
      <div class="body">
        <form id="glbForm">
          <label class="f">Objetivo en USD para cerrar todo (0 = apagado) <span id="curTarget"></span>
            <input id="glbTarget" type="number" step="0.01" min="0" placeholder="ej. 12"></label>
          <label class="f">Límite de posiciones abiertas (0 = sin límite) <span id="curMax"></span>
            <input id="glbMax" type="number" step="1" min="0" placeholder="ej. 10"></label>
          <label class="f">Fee % por lado <span id="curFee"></span>
            <input id="glbFee" type="number" step="0.001" min="0" max="1" placeholder="ej. 0.05"></label>
          <button type="submit">Guardar</button>
        </form>
        <div class="row" style="margin-top:10px" id="tokenRow">
          <label class="f" style="max-width:340px">Token de administrador
            <input id="exToken" type="password" placeholder="si está configurado" autocomplete="off"></label>
        </div>
        <div id="glbMsg" class="muted msg"></div>
      </div>
    </details>
    <details class="panel" data-key="s_exec" open>
      <summary>Executor <span class="hint" id="exSum"></span></summary>
      <div class="body">
        <div class="muted">Actual: <b id="exUrl">—</b> · enviadas <span id="exSent">0</span> · errores <span id="exErr">0</span></div>
        <div class="muted" id="exLat" style="margin-top:4px"></div>
        <form id="exForm" style="margin-top:12px">
          <input id="exNew" type="url" placeholder="https://mi-executor.onrender.com" autocomplete="off">
          <input id="exSecret" type="password" placeholder="Secreto (opcional, vacío = mantener)" autocomplete="off">
          <button type="submit">Cambiar link</button>
        </form>
        <div id="exMsg" class="muted msg"></div>
      </div>
    </details>
    <details class="panel" data-key="s_net">
      <summary>Red <span class="hint" id="netSum"></span></summary>
      <div class="body">
        <div class="netgrid" id="netGrid"></div>
        <div class="note">Bytes desde el arranque. «Entrada» es lo que el bot recibe de Binance (WebSockets y REST); «Web» es lo que envían el panel y la API, ya comprimido. El panel solo pide los datos de la sección abierta y deja de consultar con la pestaña oculta.</div>
      </div>
    </details>
    <details class="panel" data-key="s_events" open>
      <summary>Eventos <span class="hint">todos los bots</span></summary>
      <div class="body"><pre id="events"></pre></div>
    </details>
  </section>
</main>

<script>
const POLL_MS = __POLL_MS__;
const q = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num = (v, d=4) => (v === null || v === undefined || isNaN(v)) ? '—' : (+v).toLocaleString('es', {maximumFractionDigits:d});
const pct = v => `<span class="${v >= 0 ? 'pos' : 'neg'}">${v >= 0 ? '+' : ''}${(+v).toFixed(2)}%</span>`;
const usd = v => `<span class="${v >= 0 ? 'pos' : 'neg'}">${v >= 0 ? '+' : ''}${(+v).toFixed(4)}</span>`;
const dur = s => { s = Math.max(0, Math.floor(s)); const h = Math.floor(s/3600), m = Math.floor(s%3600/60);
                   return h ? `${h}h ${m}m` : m ? `${m}m ${s%60}s` : `${s}s`; };
const bytes = b => b >= 1073741824 ? (b/1073741824).toFixed(2) + ' GB' : b >= 1048576 ? (b/1048576).toFixed(1) + ' MB' : (b/1024).toFixed(0) + ' KB';
const tag = d => `<span class="tag ${esc(d)}">${esc(d)}</span>`;
const store = {get(k, d) { try { const v = localStorage.getItem(k); return v === null ? d : v; } catch (e) { return d; } },
               set(k, v) { try { localStorage.setItem(k, v); } catch (e) {} }};
const ENGINES = ['ratio', 'ratio_tp', 'ratio_rev', 'close', 'live'];
const RATIO = ['ratio', 'ratio_tp', 'ratio_rev'];
const SHORT_LABEL = {ratio: 'Ratio TP/SL', ratio_tp: 'Ratio solo TP', ratio_rev: 'Ratio venganza', close: 'Al cierre', live: 'En vivo'};
const PANEL_OF = {ratio: 'b_ratio1', ratio_tp: 'b_ratio_tp', ratio_rev: 'b_ratio_rev', close: 'b_klines', live: 'b_klines'};
// Salida de cada bot de ratio: {tp_pct, sl_pct, ratio_exit}.
const exitOf = (d, n) => n === 'ratio_tp' ? {tp_pct: 0, sl_pct: 0, ratio_exit: true} : ((n === 'ratio_rev' ? (d.ratio || {}).rev : (d.ratio || {}).cfg) || {});
const tpsl = c => `TP ${c.tp_pct > 0 ? '+' + c.tp_pct + '%' : 'off'} · SL ${c.sl_pct > 0 ? '−' + c.sl_pct + '%' : 'off'}${c.ratio_exit ? ' · TP ratio' : ''}`;
const VIEWS = ['resumen', 'operaciones', 'bots', 'mercado', 'historial', 'sistema'];
let cur = store.get('botkline_engine', 'ratio'); if (!ENGINES.includes(cur)) cur = 'ratio';
let opFilter = store.get('botkline_opfilter', 'all');
let evEngine = store.get('botkline_evengine', 'close'); if (!['close', 'live'].includes(evEngine)) evEngine = 'close';
let view = 'resumen', last = null;
q('exToken').value = store.get('botkline_token', '');

// ── Tema: auto → claro → oscuro ──
const THEMES = ['auto', 'light', 'dark'], THEME_LABEL = {auto: 'Tema: auto', light: 'Tema: claro', dark: 'Tema: oscuro'};
function applyTheme(t) {
  if (t === 'auto') delete document.documentElement.dataset.theme; else document.documentElement.dataset.theme = t;
  q('btnTheme').textContent = THEME_LABEL[t];
}
let theme = store.get('botkline_theme', 'auto'); if (!THEMES.includes(theme)) theme = 'auto';
applyTheme(theme);
q('btnTheme').addEventListener('click', () => { theme = THEMES[(THEMES.indexOf(theme) + 1) % THEMES.length]; store.set('botkline_theme', theme); applyTheme(theme); });
q('btnMenu').addEventListener('click', () => q('menu').classList.toggle('open'));

// ── Paneles desplegables: se recuerda cuáles están abiertos ──
const folds = JSON.parse(store.get('botkline_folds2', '{}') || '{}');
document.querySelectorAll('details.panel').forEach(d => {
  const k = d.dataset.key;
  if (k in folds) d.open = folds[k];
  d.addEventListener('toggle', () => { folds[k] = d.open; store.set('botkline_folds2', JSON.stringify(folds)); });
});

// ── Menú de secciones (#hash) ──
function showView(v) {
  if (!VIEWS.includes(v)) v = 'resumen';
  view = v;
  document.querySelectorAll('main > section[data-view]').forEach(s => s.hidden = s.dataset.view !== v);
  document.querySelectorAll('nav.menu a').forEach(a => a.classList.toggle('on', a.dataset.view === v));
  q('menu').classList.remove('open');
  store.set('botkline_view', v);
  refresh();                                           // cada sección pide solo sus datos
}
window.addEventListener('hashchange', () => showView(location.hash.slice(1)));

function headers() {
  const h = {'Content-Type': 'application/json'};
  const t = q('exToken').value.trim();
  if (t) h['X-Admin-Token'] = t;
  return h;
}
async function post(url, body) {
  store.set('botkline_token', q('exToken').value.trim());
  const r = await fetch(url, {method: 'POST', headers: headers(), body: JSON.stringify(body || {})});
  return r.json();
}

// ── Consulta del estado: solo con la pestaña visible, sin solapar peticiones ──
let timer = null, inflight = false, again = false;
function detailEngine() {
  if (view === 'mercado') return evEngine;
  return cur;
}
async function refresh() {
  clearTimeout(timer);
  if (inflight) { again = true; return; }
  inflight = true;
  try {
    const r = await fetch(`/api/status?view=${view}&engine=${encodeURIComponent(detailEngine())}`, {cache: 'no-store'});
    last = await r.json();
    render(last);
  } catch (e) { /* sin conexión: se reintenta en el siguiente ciclo */ }
  finally { inflight = false; }
  if (again) { again = false; return refresh(); }
  if (!document.hidden) timer = setTimeout(refresh, POLL_MS);
}
document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); else clearTimeout(timer); });

// ── Piezas reutilizables ──
const seg = (items, on, attr) => items.map(([k, label]) => `<button type="button" data-${attr}="${esc(k)}" class="${k === on ? 'on' : ''}">${label}</button>`).join('');
const allPositions = d => ENGINES.flatMap(n => (d.engines[n] ? d.engines[n].positions.map(p => ({...p, _eng: n})) : []))
                                  .sort((a, b) => a.age_s - b.age_s);
const exitLevels = (p, d) => {
  // [sl, tp] para la barra de progreso; null si no hay ese lado.
  if (RATIO.includes(p._eng)) return [p.sl_price > 0 ? p.sl_price : null, p.tp_price > 0 ? p.tp_price : null];
  return p.inverted ? [p.tp_price, null] : [null, p.tp_price];
};
function rangeBar(p) {
  const [sl, tp] = exitLevels(p), e = p.entry_price, px = p.price;
  if (!sl && !tp) return '';
  const toMove = lvl => lvl ? Math.abs((lvl - e) / e * 100) : null;
  const lo = sl ? -toMove(sl) : -Math.max(1, Math.abs(p.move_pct) * 1.2);    // % en contra
  const hi = tp ? toMove(tp) : Math.max(1, Math.abs(p.move_pct) * 1.2);       // % a favor
  const posPct = Math.min(100, Math.max(0, (p.move_pct - lo) / (hi - lo) * 100));
  return `<div><div class="range"><i style="left:${posPct}%"></i></div>
    <div class="range-l"><span>${sl ? `SL ${num(sl, 8)} (−${toMove(sl).toFixed(1)}%)` : 'sin SL'}</span><span>${tp ? `TP ${num(tp, 8)} (+${toMove(tp).toFixed(1)}%)` : 'sin TP'}</span></div></div>`;
}
function tradeCard(p, d, compact) {
  const isR = RATIO.includes(p._eng), xc = exitOf(d, p._eng);
  const sig = p.revenge ? `venganza de #${p.rev_parent} · TP +${num(p.rev_tp_usd, 4)} / SL −${num(p.rev_sl_usd, 4)} USD`
            : isR ? `ratio ${num(p.ratio_trigger, 4)}→${num(p.ratio_entry, 4)} ${pct(p.ratio_change_pct || 0)}`
                  : `vela ${pct(p.signal_pct)}${p._eng === 'live' ? ` a los ${Math.round(p.candle_age_s || 0)}s` : ''}`;
  const ratioExit = isR && !p.revenge && xc.ratio_exit ? `<span>TP ratio<b>${p.direction === 'SHORT' ? '≥' : '≤'} ${num(p.ratio_target || p.ratio_trigger, 4)}</b></span>` : '';
  return `<div class="trade ${p.pnl >= 0 ? 'win' : 'loss'}">
    <div class="h"><span class="sym">${esc(p.symbol)}</span>${tag(p.direction)}${p.revenge ? '<span class="tag REV">VENGANZA</span>' : ''}${p.sent === false ? '<span class="st off">SIMULADA</span>' : ''}
      <span class="bl">${esc(SHORT_LABEL[p._eng])} · #${p.trade_id}</span></div>
    <div class="pnl"><b>${usd(p.pnl)}</b><span>${pct(p.move_pct)}</span><span class="muted" style="margin-left:auto;font-size:12px">${esc(p.opened)} · ${dur(p.age_s)}</span></div>
    ${rangeBar(p)}
    ${compact ? '' : `<div class="grid">
      <span>Entrada<b>${num(p.entry_price, 8)}</b></span><span>Precio<b>${num(p.price, 8)}</b></span><span>Notional<b>${num(p.notional, 2)} USDT</b></span>
      <span>MFE<b>${pct(p.mfe_pct)}</b></span><span>MAE<b>${pct(p.mae_pct)}</b></span>${p.revenge ? `<span>Venganza de<b>#${p.rev_parent}</b></span>` : ratioExit || `<span>Señal<b>${sig}</b></span>`}
    </div>`}
    <div class="f"><span>${compact ? sig : `${num(p.notional, 2)} USDT · ${p.sent === false ? 'simulada' : 'enviada al executor'}`}</span><button type="button" class="small ghost" data-close="${p.trade_id}" data-label="${esc(p.direction)} ${esc(p.symbol)} #${p.trade_id}">Cerrar</button></div>
  </div>`;
}

// ── Render ──
function render(d) {
  const c = d.config, g = d.global, k = d.klines, r = d.ratio || {}, rc = r.cfg || {}, x = d.executor, p = d.persistence || {};
  const E = d.engines, sum = f => ENGINES.reduce((s, n) => s + (E[n] ? f(E[n]) : 0), 0);
  const xk = g.inverted ? 'SL' : 'TP';
  const pos = allPositions(d);

  // Barra superior
  const health = [
    [d.prices_connected || !pos.length ? 'ok' : 'bad', 'Precios en vivo'],
    [r.ticker_connected ? 'ok' : 'bad', '24 h'],
    [!x.url ? 'bad' : g.executor_paused ? 'alert' : (x.errors && x.last_error ? 'alert' : 'ok'), 'Executor'],
    [p.mode === 'ok' || p.mode === 'local' ? 'ok' : 'alert', 'Estado'],
  ];
  q('health').innerHTML = health.map(([s, t]) => `<span class="dot ${s}" title="${esc(t)}"></span>`).join('');
  q('mOpen').textContent = pos.length;

  // ── Resumen ──
  q('pills').innerHTML = [
    `<span class="pill ${g.inverted ? 'alert' : 'ok'}">${g.inverted ? 'Modo INVERTIDO' : 'Modo normal'}</span>`,
    `<span class="pill ${k.running ? 'ok' : 'alert'}">Velas ${k.running ? (k.active ? 'conectadas' : 'conectando') : 'apagadas'}</span>`,
    `<span class="pill ${r.ticker_connected ? 'ok' : 'bad'}">24 h ${r.ticker_connected ? 'ok' : 'sin datos'}</span>`,
    `<span class="pill ${r.rest_blocked_s > 0 ? 'bad' : 'ok'}">Ratio ${r.rest_blocked_s > 0 ? 'en pausa' : 'ok'}</span>`,
    `<span class="pill ${!x.url ? 'bad' : g.executor_paused ? 'alert' : 'ok'}">Executor ${!x.url ? 'sin link' : g.executor_paused ? 'PAUSADO (prueba)' : 'activo'}</span>`,
    `<span class="pill ${p.mode === 'ok' || p.mode === 'local' ? 'ok' : 'alert'}">Estado ${esc(p.mode)}</span>`,
  ].join('');
  q('rules').textContent = `${c.notional} USDT por entrada · fee ${g.fee_pct}% por lado · ratio: TP ${rc.tp_pct > 0 ? '+' + rc.tp_pct + '%' : 'off'} / SL ${rc.sl_pct > 0 ? '−' + rc.sl_pct + '%' : 'off'} · velas: ${g.inverted ? 'contra la vela, % = SL' : 'sigue la vela, % = TP'}`;
  q('sessPnl').innerHTML = usd(g.session_pnl);
  q('sessTarget').textContent = g.profit_target_usd > 0 ? `· objetivo +${g.profit_target_usd} USD` : '· objetivo apagado';
  q('sessBar').style.width = g.profit_target_usd > 0 ? `${Math.max(0, Math.min(100, g.session_pnl / g.profit_target_usd * 100))}%` : '0';
  q('sessDetail').innerHTML = `realizado ${usd(g.session_realized)} + abierto ${usd(g.session_unrealized)} · desde ${esc(g.session_since)}`;
  q('sRpnlAll').innerHTML = usd(sum(n => n.realized_pnl));
  q('sWins').textContent = `${sum(n => n.wins)}/${sum(n => n.closed_count)} ganadas · netas de fees`;
  q('sUpnlAll').innerHTML = usd(sum(n => n.unrealized_pnl));
  q('sUpnlBy').innerHTML = ENGINES.map(n => `${SHORT_LABEL[n]} ${usd(E[n].unrealized_pnl)}`).join(' · ');
  q('sOpen').textContent = g.open_count;
  q('sOpenLim').textContent = (g.max_open > 0 ? `límite ${g.max_open}` : 'sin límite') + ' · ' + ENGINES.map(n => `${SHORT_LABEL[n]} ${E[n].positions.length}`).join(' · ');
  q('sWatch').textContent = r.watch_count ?? '—';
  q('sWatchInfo').textContent = `de ${r.ticker_symbols} símbolos · lista de ${r.watch_updated || '—'}`;

  q('botCards').innerHTML = ENGINES.map(n => { const b = E[n], bc = b.config, isR = RATIO.includes(n), xc = exitOf(d, n);
    const bl = [bc.block_long && 'LONG', bc.block_short && 'SHORT'].filter(Boolean).join(', ');
    const off = !isR && !k.running;
    const state = bc.paused ? ['off', 'PAUSADO'] : off ? ['idle', 'SIN VELAS'] : ['on', 'EN MARCHA'];
    return `<div class="bot ${bc.paused ? 'paused' : ''} ${off ? 'off' : ''}">
      <div class="top"><h3>${esc(b.label)}</h3><span class="st ${state[0]}">${state[1]}</span></div>
      <div class="hint">${esc(b.hint)}</div>
      <div class="kv">
        ${isR ? `<div>Salida<b>${n === 'ratio_tp' ? 'solo TP ratio' : tpsl(xc)}</b></div><div>${n === 'ratio_rev' ? 'Venganza' : 'Ratio ↓ SHORT / ↑ LONG'}<b>${n === 'ratio_rev' ? ((r.rev || {}).revenge ? `ON ×${r.rev.rev_mult} · ${r.revenge_count || 0} disparadas` : 'OFF') : rc.drop_min_pct + '–' + rc.drop_max_pct + '%'}</b></div>`
              : `<div>Señal<b>±${bc.threshold_pct}%</b></div><div>${xk}<b>${bc.tp_pct}%</b></div>`}
        <div>Bloqueados<b class="${bl ? 'neg' : ''}">${bl || '—'}</b></div>
        <div>Abiertas<b>${b.positions.length}</b></div><div>Señales / entradas<b>${b.signal_count} / ${b.opened_count}</b></div><div>Cerradas<b>${b.closed_count} (${b.wins} gan.)</b></div>
        <div>PnL abierto<b>${usd(b.unrealized_pnl)}</b></div><div>PnL realizado<b>${usd(b.realized_pnl)}</b></div><div>Decisión<b>${b.decide_ms === null ? '—' : b.decide_ms + ' ms'}</b></div>
      </div>
      <div class="acts">
        <button type="button" class="small ${bc.paused ? 'ok' : 'warnb'}" data-pause="${esc(n)}">${bc.paused ? 'Reanudar' : 'Pausar'}</button>
        <button type="button" class="small ghost" data-goto="operaciones" data-eng="${esc(n)}">Operaciones</button>
        <button type="button" class="small ghost" data-goto="bots" data-eng="${esc(n)}">Configurar</button>
        <button type="button" class="small ghost" data-goto="historial" data-eng="${esc(n)}">Historial</button>
        <button type="button" class="small ghost" data-resetpnl="${esc(n)}">PnL realizado a 0</button>
      </div></div>`; }).join('');

  for (const id of ['btnInvert', 'btnInvert2']) {
    const bi = q(id);
    bi.dataset.on = g.inverted ? '1' : '';
    bi.textContent = g.inverted ? 'Modo INVERTIDO · volver a normal' : 'Modo normal · invertir';
    bi.className = g.inverted ? 'warnb' : 'ok';
  }
  const bx = q('btnExPause');
  bx.dataset.on = g.executor_paused ? '1' : '';
  bx.textContent = g.executor_paused ? 'Envíos al executor PAUSADOS · reanudar' : 'Pausar entradas al executor (prueba)';
  bx.className = g.executor_paused ? 'warnb' : 'ghost';
  q('modeHint').textContent = (g.executor_paused ? 'MODO PRUEBA: las entradas nuevas solo se simulan aquí, no se mandan al executor. ' : '') + (g.inverted
    ? 'Invertido (bots de velas): vela verde → SHORT, roja → LONG; el % de cada bot es stop loss, sin TP. Las abiertas siguen con el modo con que se abrieron.'
    : 'Normal (bots de velas): vela verde → LONG, roja → SHORT; el % de cada bot es take profit.');
  q('sessHits').textContent = g.target_hits ? `Objetivo alcanzado ${g.target_hits} vez/veces · último: ${g.last_target || '—'}` : 'Al llegar al objetivo se cierran todas las posiciones de todos los bots y el contador vuelve a cero.';
  q('rOpenN').textContent = pos.length;
  q('rTrades').innerHTML = pos.slice(0, 12).map(pp => tradeCard(pp, d, true)).join('') || '<div class="empty">Sin operaciones abiertas</div>';

  // ── Operaciones ──
  q('opSeg').innerHTML = seg([['all', `Todas · ${pos.length}`], ...ENGINES.map(n => [n, `${SHORT_LABEL[n]} · ${E[n].positions.length}`])], opFilter, 'opf');
  const shown = opFilter === 'all' ? pos : pos.filter(pp => pp._eng === opFilter);
  const unreal = shown.reduce((s, pp) => s + pp.pnl, 0);
  q('opSum').innerHTML = `${shown.length} abiertas · PnL abierto ${usd(unreal)}`;
  q('btnCloseEng').textContent = opFilter === 'all' ? 'Cerrar TODAS' : `Cerrar todas · ${SHORT_LABEL[opFilter]}`;
  q('opTrades').innerHTML = shown.map(pp => tradeCard(pp, d, false)).join('') || '<div class="empty">Sin operaciones abiertas</div>';
  const rn = {}; (r.rows || []).forEach(z => rn[z.symbol] = z);
  q('tPos').innerHTML = shown.map(row => {
    const isR = RATIO.includes(row._eng);
    const exits = isR ? [row.tp_price > 0 ? `<span class="pos">TP</span> ${num(row.tp_price, 8)}` : '', row.sl_price > 0 ? `<span class="neg">SL</span> ${num(row.sl_price, 8)}` : '',
                         !row.revenge && exitOf(d, row._eng).ratio_exit ? `<span class="muted">ratio ${row.direction === 'SHORT' ? '≥' : '≤'}</span> ${num(row.ratio_target || row.ratio_trigger, 4)}${rn[row.symbol] && rn[row.symbol].last != null ? ` <span class="muted">(ahora ${num(rn[row.symbol].last, 4)})</span>` : ''}` : ''].filter(Boolean).join(' · ')
                      : `<span class="muted">${row.inverted ? 'SL' : 'TP'}</span> ${num(row.tp_price, 8)}`;
    return `<tr><td><b>${esc(row.symbol)}</b> <span class="muted">#${row.trade_id}</span>${row.sent === false ? ' <span class="st off">SIMULADA</span>' : ''}</td>
      <td>${esc(SHORT_LABEL[row._eng])}</td><td>${tag(row.direction)}${row.revenge ? ' <span class="tag REV">VENGANZA</span>' : ''}</td>
      <td>${row.revenge ? `venganza de #${row.rev_parent}` : isR ? `${num(row.ratio_trigger, 4)}→${num(row.ratio_entry, 4)} ${pct(row.ratio_change_pct || 0)}` : pct(row.signal_pct)}</td>
      <td>${num(row.entry_price, 8)}</td><td>${num(row.price, 8)}</td><td>${pct(row.move_pct)}</td><td>${exits || '—'}</td>
      <td>${num(row.notional, 2)}</td><td>${usd(row.pnl)}</td>
      <td>${pct(row.mfe_pct)} <span class="muted">${usd(row.mfe_usd)}</span></td><td>${pct(row.mae_pct)} <span class="muted">${usd(row.mae_usd)}</span></td>
      <td>${esc(row.opened)} <span class="muted">(${dur(row.age_s)})</span></td>
      <td><button class="small ghost" data-close="${row.trade_id}" data-label="${esc(row.direction)} ${esc(row.symbol)} #${row.trade_id}">Cerrar</button></td></tr>`; }).join('')
    || '<tr><td colspan="14" class="muted">Sin posiciones abiertas</td></tr>';

  const e = E[cur], ec = e.config, isRatio = RATIO.includes(cur);
  document.querySelectorAll('.engName').forEach(el => el.textContent = e.label);
  q('sigSeg').innerHTML = seg(ENGINES.map(n => [n, SHORT_LABEL[n]]), cur, 'eng');
  q('nSig').textContent = e.signal_count;
  q('thSigC').textContent = isRatio ? 'Ratio' : 'Vela';
  q('thSigB').textContent = isRatio ? 'Cambio' : 'Cuerpo';
  q('thSigS').textContent = isRatio ? '' : 'Segundo';
  if (view === 'operaciones') q('tSig').innerHTML = e.signals.map(s => `<tr><td>${esc(s.time)}</td><td>${esc(s.symbol)}</td><td>${tag(s.direction)}</td>
    <td>${esc(s.candle)} <span class="muted">${num(s.open, 8)}→${num(s.close, 8)}</span></td><td>${pct(s.body_pct)}</td>
    <td class="muted">${isRatio ? '' : Math.round(s.candle_age_s || 0) + 's'}</td>
    <td class="${/^(venganza )?abierta/.test(s.action) ? 'pos' : 'muted'}">${esc(s.action)}</td></tr>`).join('')
    || `<tr><td colspan="7" class="muted">${isRatio ? 'Esperando una caída (SHORT) o subida (LONG) del ratio dentro del rango' : (k.running ? 'Esperando velas con cuerpo ≥ ±' + ec.threshold_pct + '%' : 'Las velas están apagadas (Bots → Encender velas)')}</td></tr>`;

  // ── Bots ──
  q('rtSum').textContent = `24 h ≥ ±${rc.watch_min_pct}% · ratio ↓ SHORT / ↑ LONG ${rc.drop_min_pct}–${rc.drop_max_pct}%`;
  const stateOf = n => `<span class="st ${E[n].config.paused ? 'off' : 'on'}">${E[n].config.paused ? 'PAUSADO' : 'EN MARCHA'}</span>`;
  q('r1State').innerHTML = stateOf('ratio'); q('r2State').innerHTML = stateOf('ratio_tp'); q('r3State').innerHTML = stateOf('ratio_rev');
  q('r1Sum').textContent = tpsl(rc);
  const dirBtns = n => { const bc = E[n].config;
    return ['SHORT', 'LONG'].map(dir => { const bl = dir === 'LONG' ? bc.block_long : bc.block_short;
      return `<button type="button" class="small ${bl ? 'danger' : 'ok'}" data-block="${dir}" data-eng="${n}" data-on="${bl ? '1' : ''}">${dir} ${bl ? 'desactivado · activar' : 'activo · desactivar'}</button>`; }).join('')
      + `<button type="button" class="small ${bc.paused ? 'ok' : 'warnb'}" data-pause="${n}">${bc.paused ? 'Reanudar bot' : 'Pausar bot'}</button>`; };
  for (const n of RATIO) q('rb_' + n).innerHTML = dirBtns(n);
  const rv = r.rev || {};
  q('r3Sum').textContent = `${tpsl(rv)} · venganza ${rv.revenge ? 'ON ×' + rv.rev_mult : 'OFF'}`;
  q('rvCurTp').textContent = `(ahora ${rv.tp_pct > 0 ? '+' + rv.tp_pct + '%' : 'apagado'})`;
  q('rvCurSl').textContent = `(ahora ${rv.sl_pct > 0 ? '−' + rv.sl_pct + '%' : 'apagado'})`;
  const bre = q('rvBtnExit');
  bre.dataset.on = rv.ratio_exit ? '1' : '';
  bre.textContent = rv.ratio_exit ? 'TP por ratio activo · desactivar' : 'TP por ratio desactivado · activar';
  bre.className = 'small ' + (rv.ratio_exit ? 'ok' : 'ghost');
  const sw = q('revSwitch');
  sw.dataset.on = rv.revenge ? '1' : ''; sw.classList.toggle('on', !!rv.revenge);
  q('revState').className = 'st ' + (rv.revenge ? 'on' : 'off'); q('revState').textContent = rv.revenge ? 'ON' : 'OFF';
  q('revMultSeg').innerHTML = seg([1, 2, 3, 4, 10].map(m => [m, '×' + m]), rv.rev_mult, 'rev-mult');
  q('revSlSeg').innerHTML = seg([50, 100, 120, 150].map(m => [m, m + '%']), rv.rev_sl_pct, 'rev-sl');
  q('revDirSeg').innerHTML = seg([['opposite', 'Contraria'], ['same', 'Misma']], rv.rev_dir, 'rev-dir');
  const revBox = q('revBox');
  if (!revBox.contains(document.activeElement)) {
    q('revMult').value = rv.rev_mult; q('revRecover').value = rv.rev_recover_pct; q('revSl').value = rv.rev_sl_pct;
  }
  if (rv.rev_mult) {
    const loss = c.notional * (rv.sl_pct > 0 ? rv.sl_pct : 5) / 100, tpU = loss * rv.rev_recover_pct / 100, slU = loss * rv.rev_mult * rv.rev_sl_pct / 100;
    q('revNote').innerHTML = `Si una operación de este bot cierra en pérdida (SL, TP por ratio, cierre manual…), abre un trade de venganza en la dirección <b>${rv.rev_dir === 'opposite' ? 'contraria' : 'misma'}</b> con el notional cerrado <b>×${rv.rev_mult}</b>. `
      + `TP = ${rv.rev_recover_pct}% de la pérdida; SL = ${rv.rev_sl_pct}% de (pérdida × ${rv.rev_mult}). Ejemplo: pérdida −${loss.toFixed(2)} → TP +${tpU.toFixed(2)} · SL −${slU.toFixed(2)} USD. `
      + `Si la venganza pierde, no se encadena otra. No se dispara con el objetivo global ni con «Cerrar todas».`;
  }
  q('rtCurW').textContent = `(ahora ${rc.watch_min_pct}%)`;
  q('rtCurMin').textContent = `(ahora ${rc.drop_min_pct}%)`;
  q('rtCurMax').textContent = `(ahora ${rc.drop_max_pct}%)`;
  q('rtCurTp').textContent = `(ahora ${rc.tp_pct > 0 ? '+' + rc.tp_pct + '%' : 'apagado'})`;
  q('rtCurSl').textContent = `(ahora ${rc.sl_pct > 0 ? '−' + rc.sl_pct + '%' : 'apagado'})`;
  const be = q('rtBtnExit');
  be.dataset.on = rc.ratio_exit ? '1' : '';
  be.textContent = rc.ratio_exit ? 'TP por ratio activo · desactivar' : 'TP por ratio desactivado · activar';
  be.className = 'small ' + (rc.ratio_exit ? 'ok' : 'ghost');

  q('klState').innerHTML = `<span class="st ${k.running ? 'on' : 'idle'}">${k.running ? 'VELAS ENCENDIDAS' : 'VELAS APAGADAS'}</span>`;
  const bk = q('btnKlines');
  bk.dataset.on = k.running ? '1' : '';
  bk.textContent = k.running ? 'Apagar velas' : 'Encender velas';
  bk.className = k.running ? 'warnb' : 'ok';
  q('klNote').textContent = k.running
    ? `Velas 1m de ${k.symbols} símbolos en ${k.active}/${k.connections} conexiones · ${d.eval_total.toLocaleString('es')} velas evaluadas. Es el stream con más tráfico (~0,5 MB/s).`
    : 'Con las velas apagadas los bots al cierre y en vivo no reciben señales (sus posiciones abiertas siguen con su TP/SL). Encenderlas abre el WebSocket de velas de todo el mercado: ~0,5 MB/s.';
  const klForm = n => { const b = E[n], bc = b.config;
    return `<div class="box"><h4>${esc(b.label)} <span class="st ${bc.paused ? 'off' : 'on'}" style="margin-left:6px">${bc.paused ? 'PAUSADO' : 'EN MARCHA'}</span></h4>
      <div class="note" style="margin:-4px 0 10px">${esc(b.hint)} · señal ±${bc.threshold_pct}% · ${xk} ${bc.tp_pct}%${b.decide_ms === null ? '' : ' · decisión ' + b.decide_ms + ' ms'}</div>
      <form data-cfg="${n}">
        <label class="f">% cuerpo de vela para entrar <input name="threshold_pct" type="number" step="0.01" min="0.01" max="100" placeholder="ahora ${bc.threshold_pct}"></label>
        <label class="f">% ${xk === 'TP' ? 'take profit' : 'stop loss (invertido)'} <input name="tp_pct" type="number" step="0.01" min="0.01" max="100" placeholder="ahora ${bc.tp_pct}"></label>
        <button type="submit">Guardar</button>
      </form>
      <div class="row c" style="margin-top:10px">
        ${['LONG', 'SHORT'].map(dir => { const bl = dir === 'LONG' ? bc.block_long : bc.block_short;
          return `<button type="button" class="small ${bl ? 'danger' : 'ok'}" data-block="${dir}" data-eng="${n}" data-on="${bl ? '1' : ''}">${dir} ${bl ? 'bloqueados · desbloquear' : 'activos · bloquear'}</button>`; }).join('')}
        <button type="button" class="small ${bc.paused ? 'ok' : 'warnb'}" data-pause="${n}">${bc.paused ? 'Reanudar' : 'Pausar'}</button>
      </div></div>`; };
  // El formulario no se repinta mientras se escribe en él.
  const kb = q('klBots');
  if (!kb.contains(document.activeElement)) kb.innerHTML = klForm('close') + klForm('live');

  // ── Mercado ──
  q('mkSum').textContent = `24 h por WebSocket (${r.ticker_mode === 'ws' ? 'continuo' : 'exploración de ' + r.scan_s + ' s cada 5 min'}) · ${r.known_symbols} símbolos conocidos`;
  if (view === 'mercado') {
    q('rtN').textContent = r.rows.length;
    q('rtInfo').innerHTML = `24 h ${r.ticker_connected ? '<span class="pos">ok</span>' : '<span class="neg">sin datos</span>'}`
      + ` (${r.ticker_symbols} símbolos, ${r.ticker_age_s === null ? '—' : 'hace ' + dur(r.ticker_age_s)})`
      + ` · lista diaria de símbolos ${esc(r.symbols_scanned || '—')}`
      + ` · vigilados actualizados ${esc(r.watch_updated || '—')} · próxima consulta del ratio en ${r.next_cycle_in > 0 ? dur(r.next_cycle_in) : '—'}`
      + ` · consultas ${r.polls} · errores ${r.errors}`
      + (r.filters === 0 ? ` · <span class="neg">sin filtros de cantidad (exchangeInfo): no se puede abrir</span>` : '')
      + (r.ticker_error && !r.ticker_connected ? ` · <span class="warn">24 h: ${esc(r.ticker_error)}</span>` : '')
      + (r.rest_blocked_s > 0 ? ` · <span class="neg">ratio en pausa ${dur(r.rest_blocked_s)} (límite de peticiones)</span>` : '')
      + (r.last_error ? ` · <span class="warn">${esc(r.last_error)}</span>` : '');
    q('tRt').innerHTML = r.rows.map(z => `<tr>
      <td><b>${esc(z.symbol)}</b></td><td>${z.pct24 === null ? '—' : pct(z.pct24)}</td><td>${num(z.price, 8)}</td>
      <td>${num(z.prev, 4)}</td><td>${num(z.last, 4)}</td><td>${z.chg === null ? '<span class="muted">—</span>' : pct(z.chg)}</td>
      <td class="${z.lag ? 'warn' : 'muted'}" title="${z.lag ? 'Binance aún no publicó el punto más nuevo de este símbolo' : ''}">${esc(z.data_time || '—')}${z.lag ? ' ⏳' : ''}</td><td>${z.side ? `${z.side === 'SHORT' ? 'caída ↓' : 'subida ↑'} ${tag(z.side)}` : '<span class="muted">—</span>'}</td>
      <td>${z.has_position ? '<span class="st on">POSICIÓN</span>' : z.in_range ? '<span class="st off">EN RANGO</span>' : '<span class="muted">—</span>'}</td></tr>`).join('')
      || '<tr><td colspan="9" class="muted">Ningún símbolo cumple el % de 24 h todavía</td></tr>';
    q('evSeg').innerHTML = seg([['close', 'Al cierre'], ['live', 'En vivo']], evEngine, 'ev');
    const ev = E[evEngine].evaluations;
    q('evNote').textContent = k.running ? `Umbral ±${E[evEngine].config.threshold_pct}%` : 'Las velas están apagadas: no hay evaluaciones.';
    q('evMin').textContent = ev.top_minute;
    q('tTop').innerHTML = ev.top.map(z => `<tr><td>${esc(z.symbol)}</td><td>${num(z.open, 8)}</td><td>${num(z.close, 8)}</td>
      <td>${pct(z.body_pct)}</td><td>${z.signal ? tag(z.signal) : '<span class="muted">—</span>'}</td></tr>`).join('')
      || '<tr><td colspan="5" class="muted">Esperando el primer cierre de vela</td></tr>';
    q('tMin').innerHTML = ev.minutes.map(m => `<tr><td>${esc(m.minute)}</td><td>${m.evaluated}</td><td>${m.long}</td><td>${m.short}</td>
      <td>${pct(m.max_up)}</td><td>${pct(m.max_down)}</td></tr>`).join('')
      || '<tr><td colspan="6" class="muted">Sin evaluaciones todavía</td></tr>';
  }

  // ── Historial ──
  q('hiSeg').innerHTML = seg(ENGINES.map(n => [n, SHORT_LABEL[n]]), cur, 'eng');
  q('csvLink').href = '/api/trades.csv?engine=' + encodeURIComponent(cur);
  q('hiSum').innerHTML = `${esc(e.label)} · ${e.closed_count} cerradas · ${e.wins} ganadas · realizado ${usd(e.realized_pnl)}`;
  q('nClosed').textContent = e.closed_count;
  if (view === 'historial') {
    q('tClosed').innerHTML = e.closed_trades.map(t => `<tr><td>${esc(t.closed)}</td><td>${esc(t.symbol)}</td><td>${tag(t.direction)}${t.revenge ? ' <span class="tag REV">VENGANZA</span>' : ''}</td>
      <td>${num(t.entry_price, 8)}</td><td>${num(t.close_price, 8)}</td><td>${pct(t.move_pct)}</td><td>${usd(t.pnl)}</td>
      <td class="muted">${num(t.fees || 0, 4)}</td><td>${pct(t.mfe_pct || 0)}</td><td>${pct(t.mae_pct || 0)}</td>
      <td>${dur(t.duration_s || 0)}</td><td>${esc(t.reason)}${t.sent === false ? ' <span class="st off">SIMULADA</span>' : ''}</td></tr>`).join('')
      || '<tr><td colspan="12" class="muted">Aún no hay cierres</td></tr>';
    const exc = e.excursions || {};
    q('tExc').innerHTML = ['all', 'LONG', 'SHORT'].map(kk => {
      const rr = exc[kk] || {n: 0}, label = kk === 'all' ? 'Todas' : tag(kk);
      if (!rr.n) return `<tr><td>${label}</td><td>0</td><td colspan="8" class="muted">—</td></tr>`;
      return `<tr><td>${label}</td><td>${rr.n}</td><td>${rr.wins}</td><td>${pct(rr.avg_mfe_pct)}</td><td>${pct(rr.avg_mae_pct)}</td>
        <td>${pct(rr.best_mfe_pct)}</td><td>${pct(rr.worst_mae_pct)}</td><td>${usd(rr.avg_mae_usd)}</td>
        <td>${dur(rr.avg_time_to_mae_s)}</td><td>${dur(rr.avg_duration_s)}</td></tr>`;
    }).join('');
  }

  // ── Sistema ──
  q('glbSum').textContent = `objetivo ${g.profit_target_usd > 0 ? '+' + g.profit_target_usd + ' USD' : 'apagado'} · límite ${g.max_open > 0 ? g.max_open : 'sin límite'} · fee ${g.fee_pct}%`;
  q('curTarget').textContent = `(ahora ${g.profit_target_usd > 0 ? '+' + g.profit_target_usd : 'apagado'})`;
  q('curMax').textContent = `(ahora ${g.max_open > 0 ? g.max_open : 'sin límite'})`;
  q('curFee').textContent = `(ahora ${g.fee_pct}%)`;
  q('tokenRow').hidden = !c.admin_token;
  q('exUrl').textContent = x.url || '(vacío: sin señales)';
  q('exSum').textContent = x.url ? `${x.url} · ${x.sent} enviadas · ${x.errors} errores` : 'sin link: no se mandan señales';
  q('exSent').textContent = x.sent;
  q('exErr').textContent = x.errors;
  q('exLat').textContent = x.last_ok
    ? `Última señal (${x.last_ok}): HTTP al executor ${x.http_ms} ms · total desde el mensaje de Binance ${x.total_ms} ms`
    : 'Latencia: aún no hay señales enviadas';
  const nt = d.net;
  if (nt) {
    q('netSum').textContent = `entrada ${bytes(nt.rx_per_hour)}/h · web ${bytes(nt.web_per_hour)}/h`;
    q('netGrid').innerHTML = [
      ['Entrada total', bytes(nt.rx_total)], ['Entrada por hora', bytes(nt.rx_per_hour)],
      [`Precios en vivo (${esc(nt.price_stream || '—')}, ${nt.price_symbols} pares)`, bytes(nt.rx.prices)],
      [`24 h por WebSocket (${nt.ticker_mode === 'ws' ? 'continuo' : 'exploración'})`, bytes(nt.rx.ticker)],
      ['REST (ratio y exchangeInfo)', bytes(nt.rx.rest)],
      ['Web enviada', bytes(nt.web_tx)], ['Web por hora', bytes(nt.web_per_hour)],
    ].map(([a, b]) => `<div>${a}<b>${b}</b></div>`).join('');
  }
  if (view === 'sistema') q('events').textContent = d.events.join('\n');
}

// ── Acciones ──
const glbText = r => `objetivo ${r.profit_target_usd > 0 ? '+' + r.profit_target_usd + ' USD' : 'apagado'} · límite ${r.max_open > 0 ? r.max_open + ' abiertas' : 'sin límite'} · fee ${r.fee_pct}% por lado`;
async function glb(body, url = '/api/global', box = 'glbMsg', okMsg = glbText) {
  q(box).textContent = 'Guardando…';
  try {
    const r = await post(url, body);
    q(box).innerHTML = r.ok ? `<span class="pos">Guardado.</span> ${okMsg(r)}` : `<span class="neg">${esc(r.error)}</span>`;
    if (!r.ok && box !== 'glbMsg') alert(r.error);
    return r.ok;
  } catch (e) { q(box).innerHTML = `<span class="neg">${esc(e)}</span>`; }
  finally { refresh(); }
}
async function ratioPost(body, okMsg) {
  q('rtMsg').textContent = 'Guardando…';
  try {
    const r = await post('/api/ratio', body);
    q('rtMsg').innerHTML = r.ok ? `<span class="pos">Guardado.</span> ${okMsg(r)}` : `<span class="neg">${esc(r.error)}</span>`;
    return r.ok;
  } catch (e) { q('rtMsg').innerHTML = `<span class="neg">${esc(e)}</span>`; }
  finally { refresh(); }
}
async function revPost(body, okMsg, box = 'revMsg') {
  q(box).textContent = 'Guardando…';
  try {
    const r = await post('/api/ratio', {engine: 'ratio_rev', ...body});
    q(box).innerHTML = r.ok ? `<span class="pos">Guardado.</span> ${okMsg(r)}` : `<span class="neg">${esc(r.error)}</span>`;
    return r.ok;
  } catch (e) { q(box).innerHTML = `<span class="neg">${esc(e)}</span>`; }
  finally { refresh(); }
}
const revMsg = r => `Venganza ${r.revenge ? 'ON' : 'OFF'} · ×${r.rev_mult} · TP ${r.rev_recover_pct}% de la pérdida · SL ${r.rev_sl_pct}% · ${r.rev_dir === 'opposite' ? 'contraria' : 'misma dirección'}`;
async function closeAll(engine) {
  const n = engine ? last.engines[engine].positions.length : last.global.open_count;
  const who = engine ? `del ${last.engines[engine].label}` : 'de todos los bots';
  if (!n) { alert(`No hay posiciones abiertas ${who}.`); return; }
  if (!confirm(`¿Cerrar a mercado las ${n} posiciones abiertas ${who}?`)) return;
  try { const r = await post('/api/close-all', engine ? {engine} : {}); alert(r.ok ? r.msg : r.error); } catch (e) { alert(e); }
  refresh();
}
const ACTIONS = {
  invert(b) {
    const inv = !b.dataset.on;
    if (confirm(inv ? '¿Invertir los bots de velas? Las entradas nuevas irán CONTRA la vela y el % será stop loss, sin TP. Las posiciones abiertas siguen con su TP.'
                    : '¿Volver al modo normal? Las entradas nuevas seguirán la vela con TP. Las abiertas invertidas siguen con su SL.'))
      glb({inverted: inv}, '/api/global', b.id === 'btnInvert2' ? 'cfgMsg' : 'actMsg', r => r.inverted ? 'Modo INVERTIDO en los bots de velas.' : 'Modo normal en los bots de velas.');
  },
  exPause(b) {
    const p = !b.dataset.on;
    if (confirm(p ? '¿Pausar las entradas al executor? Los bots siguen operando en simulación (posiciones SIMULADAS) pero no se manda ninguna apertura. Los cierres de lo ya enviado sí se mandan.'
                  : '¿Reanudar los envíos al executor? Las entradas nuevas volverán a abrirse de verdad.'))
      glb({executor_paused: p}, '/api/global', 'actMsg', r => r.executor_paused ? 'Envíos al executor pausados: las entradas nuevas solo se simulan.' : 'Envíos al executor reanudados.');
  },
  reset() { if (confirm('¿Poner a cero el PnL de la sesión? El objetivo se contará desde ahora.')) glb({}, '/api/session/reset', 'actMsg', () => 'PnL de la sesión a cero.'); },
  closeAll() { closeAll(null); },
  closeEng() { closeAll(opFilter === 'all' ? null : opFilter); },
  klines(b) {
    const on = !b.dataset.on;
    if (confirm(on ? '¿Encender las velas 1m? Los bots al cierre y en vivo vuelven a recibir señales. Es el stream con más tráfico (~0,5 MB/s).'
                   : '¿Apagar las velas? Los bots al cierre y en vivo dejan de abrir; sus posiciones abiertas siguen con su TP/SL.'))
      glb({klines_enabled: on}, '/api/global', 'cfgMsg', r => r.klines_enabled ? 'Velas encendidas.' : 'Velas apagadas.');
  },
  ratioExit(b) { ratioPost({ratio_exit: !b.dataset.on}, r => `TP por ratio ${r.ratio_exit ? 'activado' : 'desactivado'}`); },
  revRatioExit(b) { revPost({ratio_exit: !b.dataset.on}, r => `TP por ratio ${r.ratio_exit ? 'activado' : 'desactivado'} en el bot ratio + venganza`, 'rtMsg'); },
  revToggle(b) {
    const on = !b.dataset.on;
    if (on || confirm('¿Desactivar la venganza? Las venganzas ya abiertas siguen hasta su TP/SL.')) revPost({revenge: on}, r => `Venganza ${r.revenge ? 'activada' : 'desactivada'}`);
  },
};

document.addEventListener('click', async ev => {
  const t = ev.target.closest('button, a[data-view]');
  if (!t) return;
  const ds = t.dataset;
  if (ds.act && ACTIONS[ds.act]) return ACTIONS[ds.act](t);
  if (ds.resetpnl) {
    if (confirm(`¿Poner a cero el PnL realizado de ${SHORT_LABEL[ds.resetpnl]}? El historial se conserva y el objetivo global no cambia.`))
      glb({engine: ds.resetpnl}, '/api/realized/reset', 'actMsg', r => `PnL realizado de ${SHORT_LABEL[r.engine]} a cero (era ${usd(r.was)}).`);
    return;
  }
  if (ds.goto) {
    if (ds.eng) { cur = ds.eng; store.set('botkline_engine', cur); if (ds.goto === 'operaciones') { opFilter = ds.eng; store.set('botkline_opfilter', opFilter); } }
    if (ds.goto === 'bots') { const p = document.querySelector(`details[data-key="${PANEL_OF[ds.eng] || 'b_klines'}"]`); if (p) { p.open = true; setTimeout(() => p.scrollIntoView({block: 'start'}), 50); } }
    location.hash = ds.goto;
    if (location.hash.slice(1) === view) refresh();
    return;
  }
  if (ds.opf) { opFilter = ds.opf; store.set('botkline_opfilter', opFilter); if (ENGINES.includes(opFilter)) { cur = opFilter; store.set('botkline_engine', cur); } return refresh(); }
  if (ds.eng && !ds.block) { cur = ds.eng; store.set('botkline_engine', cur); return refresh(); }
  if (ds.ev) { evEngine = ds.ev; store.set('botkline_evengine', evEngine); return refresh(); }
  if (ds.revMult) return revPost({rev_mult: +ds.revMult}, revMsg);
  if (ds.revSl) return revPost({rev_sl_pct: +ds.revSl}, revMsg);
  if (ds.revDir) return revPost({rev_dir: ds.revDir}, revMsg);
  if (ds.pause) {
    const b = last && last.engines[ds.pause]; if (!b) return;
    const paused = !b.config.paused;
    if (!confirm(paused ? `¿Pausar el ${b.label}? No abrirá entradas nuevas; sus posiciones abiertas siguen hasta su TP/SL.` : `¿Reanudar el ${b.label}?`)) return;
    try { const r = await post('/api/pause', {engine: ds.pause, paused}); if (!r.ok) alert(r.error); } catch (e) { alert(e); }
    return refresh();
  }
  if (ds.block) {
    try { const r = await post('/api/block', {engine: ds.eng, direction: ds.block, blocked: !ds.on}); if (!r.ok) alert(r.error); } catch (e) { alert(e); }
    return refresh();
  }
  if (ds.close) {
    if (!confirm(`¿Cerrar ${ds.label || ds.close} a mercado?`)) return;
    try { const r = await post('/api/close/' + encodeURIComponent(ds.close), {}); alert(r.ok ? r.msg : (r.error || r.msg)); } catch (e) { alert(e); }
    return refresh();
  }
});

document.addEventListener('submit', async ev => {
  const f = ev.target;
  if (!f.dataset.cfg) return;
  ev.preventDefault();
  q('cfgMsg').textContent = 'Guardando…';
  try {
    const r = await post('/api/config', {engine: f.dataset.cfg, threshold_pct: f.threshold_pct.value, tp_pct: f.tp_pct.value});
    q('cfgMsg').innerHTML = r.ok ? `<span class="pos">Guardado en ${esc(last.engines[r.engine].label)}.</span> Señal ±${r.threshold_pct}% · ${last.global.inverted ? 'SL' : 'TP'} ${r.tp_pct}%`
                                 : `<span class="neg">${esc(r.error)}</span>`;
    if (r.ok) f.reset();
  } catch (e) { q('cfgMsg').innerHTML = `<span class="neg">${esc(e)}</span>`; }
  document.activeElement && document.activeElement.blur();
  refresh();
});

q('rtForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  if (await ratioPost({watch_min_pct: q('rtWatch').value, drop_min_pct: q('rtMin').value, drop_max_pct: q('rtMax').value},
      r => `24 h ≥ ±${r.watch_min_pct}% · caída/subida del ratio ${r.drop_min_pct}–${r.drop_max_pct}% (la lista se recalcula en la próxima consulta de 5 min)`)) {
    q('rtWatch').value = ''; q('rtMin').value = ''; q('rtMax').value = '';
  }
});
q('rtExitForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  if (await ratioPost({tp_pct: q('rtTp').value, sl_pct: q('rtSl').value},
      r => `TP ${r.tp_pct > 0 ? '+' + r.tp_pct + '%' : 'apagado'} · SL ${r.sl_pct > 0 ? '−' + r.sl_pct + '%' : 'apagado'} (también en las posiciones abiertas)`)) {
    q('rtTp').value = ''; q('rtSl').value = '';
  }
});
q('rvExitForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  if (await revPost({tp_pct: q('rvTp').value, sl_pct: q('rvSl').value},
      r => `TP ${r.tp_pct > 0 ? '+' + r.tp_pct + '%' : 'apagado'} · SL ${r.sl_pct > 0 ? '−' + r.sl_pct + '%' : 'apagado'} en el bot ratio + venganza`, 'rtMsg')) {
    q('rvTp').value = ''; q('rvSl').value = '';
  }
});
for (const [id, key] of [['revMult', 'rev_mult'], ['revRecover', 'rev_recover_pct'], ['revSl', 'rev_sl_pct']]) {
  q(id).addEventListener('change', ev => { if (ev.target.value) revPost({[key]: ev.target.value}, revMsg).then(() => ev.target.blur()); });
}
q('glbForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  if (await glb({profit_target_usd: q('glbTarget').value, max_open: q('glbMax').value, fee_pct: q('glbFee').value})) {
    q('glbTarget').value = ''; q('glbMax').value = ''; q('glbFee').value = '';
  }
});
q('exForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  const body = {url: q('exNew').value.trim()};
  const secret = q('exSecret').value;
  if (secret) body.secret = secret;
  if (!body.url && !confirm('Link vacío: los bots dejarán de mandar señales. ¿Seguir?')) return;
  q('exMsg').textContent = 'Guardando…';
  try {
    const r = await post('/api/executor', body);
    q('exMsg').innerHTML = r.ok
      ? `<span class="pos">Link cambiado.</span> ${r.url ? (r.reachable ? 'El executor responde (' + esc(r.detail) + ').' : '<span class="warn">No responde: ' + esc(r.detail) + '</span>') : esc(r.detail)}`
      : `<span class="neg">${esc(r.error)}</span>`;
    if (r.ok) { q('exNew').value = ''; q('exSecret').value = ''; }
  } catch (e) { q('exMsg').innerHTML = `<span class="neg">${esc(e)}</span>`; }
  refresh();
});

showView(location.hash.slice(1) || store.get('botkline_view', 'resumen'));
</script>
</body>
</html>"""

PAGE = HTML.replace("__POLL_MS__", str(STATUS_POLL_MS)).encode("utf-8")
PAGE_ETAG = hashlib.sha1(PAGE).hexdigest()[:16]


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), threaded=True)

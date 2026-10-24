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

Bot de ratio de top traders (tercer bot, "ratio")
  • Vigila SOLO los perpetuos con variación 24 h ≥ +RATIO_WATCH_MIN_PCT (20 %), leída del
    WebSocket !ticker@arr. La lista se recalcula cada 5 min y se suscribe a precios en vivo.
  • Cada 5 min consulta el Top Trader Long/Short Ratio (Accounts) de esa lista. (Binance
    NO ofrece este dato por WebSocket: es REST, 1 petición por símbolo vigilado cada 5 min,
    por el proxy, con freno automático ante HTTP 429/418.)
  • Si el ratio cae entre drop_min % y drop_max % de un punto de 5 m al siguiente → SHORT.
  • TP: cuando el ratio RECUPERA el mismo % que cayó, medido desde el ratio de entrada
    (cae 27,1 % de 1,18 a 0,86 → TP cuando sube 27,1 % desde 0,86 = 1,093). Sin stop loss.
  • LONG (apagado por defecto, se activa desde la web): espejo para los PERDEDORES
    (24 h ≤ -20 %): el ratio SUBE entre drop_min % y drop_max % → LONG; TP cuando el
    ratio baja ese mismo % desde el ratio de entrada.

Precios para el take profit: WS.py (!markPrice@arr de todos los símbolos +
bookTicker en tiempo real de los que tienen posición abierta).
Recuperación tras reinicio: state_store.py, solo con copia en disco local (Upstash
desactivado por ahora: en Render free el disco se borra en cada reinicio).
"""

from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import csv
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
from typing import Deque, Dict, List, Optional, Tuple

from flask import Flask, jsonify, make_response, request

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from WS import SymbolWebSocketPriceCache                       # noqa: E402
from kline_ws import Candle, KlineWebSocketStream              # noqa: E402
from state_store import StateStore, make_owner_id                    # noqa: E402
import strategy                                               # noqa: E402


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
SYMBOL_REFRESH_HOURS = float(os.getenv("SYMBOL_REFRESH_HOURS", "12"))

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
RATIO_WATCH_MIN_PCT = float(os.getenv("RATIO_WATCH_MIN_PCT", "20"))   # |24 h| mínimo para vigilar
RATIO_DROP_MIN_PCT  = float(os.getenv("RATIO_DROP_MIN_PCT", "25"))    # caída mínima del ratio (5 m → 5 m)
RATIO_DROP_MAX_PCT  = float(os.getenv("RATIO_DROP_MAX_PCT", "30"))    # caída máxima (1,18→0,86 = 27 %)
RATIO_PERIOD        = "5m"
RATIO_REFRESH_S     = 300          # lista de vigilados y consulta del ratio, cada 5 min
RATIO_POLL_DELAY_S  = float(os.getenv("RATIO_POLL_DELAY_S", "15"))    # espera tras el minuto :00/:05
RATIO_RETRIES       = int(os.getenv("RATIO_RETRIES", "2"))            # reintentos si aún no hay dato nuevo
RATIO_RETRY_GAP_S   = float(os.getenv("RATIO_RETRY_GAP_S", "20"))
RATIO_MAX_AGE_S     = float(os.getenv("RATIO_MAX_AGE_S", "720"))      # dato de ratio más viejo no dispara
RATIO_WATCH_MAX     = int(os.getenv("RATIO_WATCH_MAX", "100"))        # tope de símbolos vigilados
RATIO_CONCURRENCY   = max(1, int(os.getenv("RATIO_CONCURRENCY", "4")))

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
EXECUTOR_WARM_S  = float(os.getenv("EXECUTOR_WARM_S", "20"))            # GET para mantener viva la conexión

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

STATUS_POLL_MS = int(os.getenv("STATUS_POLL_MS", "1500"))


def _utc(ts: float, pattern: str = "%Y-%m-%d %H:%M:%S") -> str:
    try:
        return datetime.fromtimestamp(float(ts), timezone.utc).strftime(pattern)
    except (TypeError, ValueError, OverflowError, OSError):
        return ""


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
        self._queue: "queue.Queue[tuple]" = queue.Queue()
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
    # un GET cada EXECUTOR_WARM_S si está ociosa, así una señal sale en el RTT.

    def _ensure_workers(self) -> None:
        if self._workers:
            return
        with self._start_lock:
            if self._workers:
                return
            for i in range(EXECUTOR_WORKERS):
                t = threading.Thread(target=self._worker, daemon=True, name=f"executor-{i}")
                t.start()
                self._workers.append(t)

    def _warm_conn(self, conns: dict) -> None:
        target = self.target
        if not target.url:
            return
        try:
            self._request(conns, target, "GET", "/api/state", None, retry=False)
        except Exception:
            pass                        # la próxima señal abrirá una conexión nueva

    def _worker(self) -> None:
        conns: Dict[Tuple[str, str], http.client.HTTPConnection] = {}
        self._warm_conn(conns)          # conexión lista antes de la primera señal
        while True:
            try:
                item = self._queue.get(timeout=EXECUTOR_WARM_S)
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
        self._queue.put((payload, target, t0))

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

    def update(self, price: float) -> float:
        """Actualiza precio y excursiones. Llamar con el lock del bot tomado."""
        self.last_price = price
        move = strategy.favorable_move_pct(price, self.entry_price, self.direction)
        now = time.time()
        if move > self.mfe_pct:
            self.mfe_pct, self.mfe_ts = move, now
            self.mfe_usd = strategy.pnl_usd(price, self.entry_price, self.qty, self.direction)
        if move < self.mae_pct:
            self.mae_pct, self.mae_ts = move, now
            self.mae_usd = strategy.pnl_usd(price, self.entry_price, self.qty, self.direction)
        if price > self.high_price:
            self.high_price = price
        if self.low_price <= 0 or price < self.low_price:
            self.low_price = price
        return move


ENGINE_CLOSE = "close"
ENGINE_LIVE  = "live"
ENGINE_RATIO = "ratio"
ENGINE_LABELS = {ENGINE_CLOSE: "Bot al cierre", ENGINE_LIVE: "Bot en vivo",
                 ENGINE_RATIO: "Bot ratio top traders"}
ENGINE_HINTS = {
    ENGINE_RATIO: "SHORT cuando el ratio de cuentas de los top traders se gira (cae) en un símbolo con 24 h ≥ +20 %; "
                  "TP cuando el ratio recupera el mismo % que cayó",
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
        self.blocked = {strategy.LONG: bool(cfg.get("block_long")),
                        strategy.SHORT: bool(cfg.get("block_short"))}
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
                "block_long": self.blocked[strategy.LONG], "block_short": self.blocked[strategy.SHORT],
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
            ENGINE_CLOSE: Engine(ENGINE_CLOSE, saved.get(ENGINE_CLOSE) or legacy,
                                 SIGNAL_THRESHOLD_PCT, TAKE_PROFIT_PCT),
            ENGINE_LIVE:  Engine(ENGINE_LIVE, saved.get(ENGINE_LIVE) or {},
                                 LIVE_SIGNAL_THRESHOLD_PCT, LIVE_TAKE_PROFIT_PCT),
        }
        self.live = self.engines[ENGINE_LIVE]
        # Bot de ratio: LONG bloqueado por defecto (se activa desde la web).
        self.engines[ENGINE_RATIO] = Engine(ENGINE_RATIO, saved.get(ENGINE_RATIO) or {"block_long": True},
                                            1.0, 1.0)
        rs = settings.get("ratio") if isinstance(settings.get("ratio"), dict) else {}

        def _pos(v, d):
            try:
                v = float(v)
                return v if v > 0 else d
            except (TypeError, ValueError):
                return d
        self.ratio_cfg = {"watch_min_pct": _pos(rs.get("watch_min_pct"), RATIO_WATCH_MIN_PCT),
                          "drop_min_pct":  _pos(rs.get("drop_min_pct"), RATIO_DROP_MIN_PCT),
                          "drop_max_pct":  _pos(rs.get("drop_max_pct"), RATIO_DROP_MAX_PCT)}
        if self.ratio_cfg["drop_min_pct"] >= self.ratio_cfg["drop_max_pct"]:
            self.ratio_cfg["drop_min_pct"], self.ratio_cfg["drop_max_pct"] = RATIO_DROP_MIN_PCT, RATIO_DROP_MAX_PCT
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
        # Ajustes comunes a los dos bots (botón y campos de la web).
        self.inverted = bool(settings.get("inverted", INVERT_SIGNALS))
        self.fee_pct = float(settings["fee_pct"]) if settings.get("fee_pct") is not None else FEE_PCT
        self.profit_target_usd = (float(settings["profit_target_usd"])
                                  if settings.get("profit_target_usd") is not None else PROFIT_TARGET_USD)
        # Objetivo global: PnL realizado acumulado en el último reinicio del contador.
        # Modo prueba: los bots siguen operando en simulación pero NO mandan
        # entradas nuevas al executor (los cierres de lo ya enviado sí salen).
        self.executor_paused = bool(settings.get("executor_paused"))
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

    def _refresh_pos_syms(self) -> None:
        with self.lock:
            self._pos_syms = frozenset(p.symbol for e in self.engines.values()
                                       for p in e.positions.values())

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
                "executor_paused": self.executor_paused, "ratio": self.ratio_cfg}
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
                    p.tp_price = strategy.exit_price(p.entry_price, p.direction, tp_pct, p.inverted)
                syms = {p.symbol for p in eng.positions.values()}
            for sym in syms:
                self._on_price_event(sym, "config")      # revisa ya el TP nuevo
        if changes:
            self._save_settings()
            self.store.mark_dirty(True)
            self.log(f"[config] {eng.label}: " + " | ".join(changes))
        return {"engine": eng.name, "threshold_pct": eng.threshold_pct, "tp_pct": eng.tp_pct}

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
        return {"inverted": self.inverted, "fee_pct": self.fee_pct,
                "profit_target_usd": self.profit_target_usd, "max_open": self.max_open,
                "executor_paused": self.executor_paused}

    def open_count(self) -> int:
        with self.lock:
            return sum(len(e.positions) for e in self.engines.values())

    def realized_total(self) -> float:
        with self.lock:
            return sum(e.total_realized_pnl for e in self.engines.values())

    def _net_pnl(self, p: Position, price: float) -> float:
        return strategy.net_pnl_usd(price, p.entry_price, p.qty, p.direction, self.fee_pct)

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

    def close_all(self, eng: Optional[Engine] = None, reason: str = "MANUAL") -> Tuple[int, int]:
        """Cierra todas las posiciones (de un bot o de los dos). (cerradas, total)."""
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
        return {"engine": eng.name, "block_long": eng.blocked[strategy.LONG],
                "block_short": eng.blocked[strategy.SHORT]}

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
            if eng.name == ENGINE_RATIO:
                continue
            self.log(f"{eng.label} | {ENGINE_HINTS[eng.name]} | señal ≥ {eng.threshold_pct:g}% | "
                     f"{'SL' if self.inverted else 'TP'} {eng.tp_pct:g}% | {NOTIONAL_USDT:g} USDT")
        self.log(f"Modo {'INVERTIDO (contra la vela, sin TP)' if self.inverted else 'normal (sigue la vela, sin SL)'} | "
                 f"fee {self.fee_pct:g}% por lado | objetivo global "
                 f"{f'+{self.profit_target_usd:g} USD' if self.profit_target_usd > 0 else 'apagado'}")
        self.log(f"Executor → {self.executor.target.url or '(vacío)'}")
        rc = self.ratio_cfg
        self.log(f"Bot ratio top traders | vigila 24 h ≥ {rc['watch_min_pct']:g}% | caída del ratio "
                 f"{rc['drop_min_pct']:g}–{rc['drop_max_pct']:g}% | LONG "
                 f"{'bloqueado' if self.engines[ENGINE_RATIO].blocked[strategy.LONG] else 'activo'} | "
                 f"{NOTIONAL_USDT:g} USDT | TP = ratio recupera el mismo % que cayó")
        self.log(f"[estado] Persistencia: {self._store_desc}")
        self.store.start()
        self.executor.warm()
        await self._init_symbols()

        # El estado (posiciones abiertas) se restaura ANTES de abrir los WebSockets.
        if await asyncio.to_thread(self.store.acquired.wait, STATE_BOOT_WAIT_S):
            self._restore_from_store()
        else:
            self.log("[estado] ⚠️ Aún no recupero el estado local: sin entradas ni cierres hasta recuperarlo")

        self.price_cache = SymbolWebSocketPriceCache(sorted(self._pos_syms))
        self.price_cache.set_update_callback(self._on_price_event)
        self.price_cache.start()

        if KLINE_BOTS_ENABLED:
            self.kline_stream = KlineWebSocketStream(
                self.symbols, intervals=(KLINE_INTERVAL,), history=KLINE_HISTORY,
                dtype=KLINE_DTYPE, on_close=self._on_kline_close, on_update=self._on_kline_live,
            )
            self.kline_stream.start()
            self.log(f"Velas 1m por WebSocket: {len(self.symbols)} símbolos, "
                     f"{KLINE_HISTORY} velas cerradas por símbolo + vela en curso para el bot en vivo")
        else:
            self.log("Velas 1m DESACTIVADAS (KLINE_BOTS_ENABLED=false): solo opera el bot de ratio")

        await asyncio.gather(
            self._supervised(self._engine_loop, "engine"),
            self._supervised(self._maintenance_loop, "maintenance"),
            self._supervised(self._ticker_loop, "ticker"),
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
                handlers = [urllib.request.ProxyHandler({"http": proxy, "https": proxy})] if proxy else []
                opener = urllib.request.build_opener(*handlers)
                req = urllib.request.Request(f"{BASE_URL}/fapi/v1/exchangeInfo",
                                             headers={"User-Agent": "BOTKLINE/1.0"})
                with opener.open(req, timeout=15) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                self.log(f"REST exchangeInfo por {label}")
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
        cached = None
        try:
            with open(SYMBOLS_CACHE_FILE, "r", encoding="utf-8") as fh:
                cached = json.load(fh)
        except Exception:
            pass
        age_h = (time.time() - float(cached.get("saved_at", 0))) / 3600 if cached else None
        if cached and cached.get("filters") and age_h is not None and age_h < SYMBOL_REFRESH_HOURS:
            self.filters = dict(cached["filters"])
            self.log(f"Símbolos desde caché en disco: {len(self.filters)} (hace {age_h:.1f} h)")
        else:
            try:
                self.filters = await asyncio.to_thread(self._fetch_exchange_info)
                tmp = f"{SYMBOLS_CACHE_FILE}.tmp"
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump({"symbols": sorted(self.filters), "filters": self.filters,
                               "saved_at": time.time()}, fh)
                os.replace(tmp, SYMBOLS_CACHE_FILE)
            except Exception as exc:
                self.last_error = str(exc)
                if cached and cached.get("filters"):
                    self.filters = dict(cached["filters"])
                    self.log(f"Usando caché viejo de símbolos ({len(self.filters)})")
                elif INITIAL_SYMBOLS:
                    self.filters = {s: {"stepSize": 0.001, "minQty": 0.0, "minNotional": 5.0}
                                    for s in INITIAL_SYMBOLS}
                    self.log(f"Usando INITIAL_SYMBOLS ({len(self.filters)}) sin filtros reales")
                else:
                    self.log("⚠️ Sin lista de símbolos: el bot no puede suscribir velas")
        self.symbols = sorted(self.filters)
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
            direction = strategy.LONG if (body >= thr) != inv else strategy.SHORT
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
        for eng in self.engines.values():
            direction = strategy.entry_direction(candle.open, candle.close, eng.threshold_pct, inv)
            if direction is None:
                continue
            if eng is self.live:
                if self._live_fired.get(symbol) == candle.open_time:
                    continue
                self._live_fired[symbol] = candle.open_time
            loop.call_soon_threadsafe(self._handle_signal, eng, symbol, direction, candle, arrived, t0, inv)

    def _record_eval(self, symbol: str, candle: Candle) -> None:
        body = strategy.body_change_pct(candle.open, candle.close)
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
                            "signal": strategy.signal_direction(o, c, thr) or ""})
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
                "body_pct":  strategy.body_change_pct(candle.open, candle.close),
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
        if eng._last_candle.get(symbol, -1) >= candle.open_time:
            return                                          # duplicado tras reconexión
        eng._last_candle[symbol] = candle.open_time
        eng.signal_count += 1
        body = strategy.body_change_pct(candle.open, candle.close)
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
        qty, eff, why = strategy.size_order(
            NOTIONAL_USDT, price, f.get("stepSize", 0.001), f.get("minQty", 0.0),
            f.get("minNotional", 5.0), MAX_NOTIONAL_OVER)
        if qty <= 0:
            self.log(f"[{eng.label}] señal {direction} {symbol} ({body:+.2f}%) saltada: {why}")
            return skip(f"saltada: {why}")

        pos = Position(
            trade_id=self._next_trade_id(), symbol=symbol, direction=direction,
            entry_price=price, qty=qty, notional=eff,
            tp_price=strategy.exit_price(price, direction, eng.tp_pct, inverted),
            signal_pct=body, candle_open=candle.open, candle_close=candle.close,
            candle_time=candle.open_time, last_price=price,
            strategy=eng.name, candle_age_s=round(age, 2), inverted=inverted,
            sent=not self.executor_paused,
        )
        with self.lock:
            eng.positions[pos.trade_id] = pos
            eng.opened_count += 1
        self._refresh_pos_syms()
        if self.price_cache is not None:
            self.price_cache.ensure_symbols([symbol])       # bookTicker en tiempo real
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
        """Revisa la salida (TP, o SL si se abrió invertida) de TODAS las posiciones
        abiertas del símbolo, de los dos bots."""
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
                    if pos.strategy == ENGINE_RATIO:
                        continue                    # su salida es por ratio, no por precio
                    if strategy.exit_hit(price, pos.tp_price, pos.direction, pos.inverted):
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
                gross = strategy.pnl_usd(price, pos.entry_price, pos.qty, pos.direction)
                fees = strategy.fees_usd(pos.entry_price, price, pos.qty, self.fee_pct)
                pnl = gross - fees                          # neto: fees de entrada y salida
                now = time.time()
                rec = {
                    **asdict(pos),
                    "close_price": price,
                    "closed_at_ts": now,
                    "duration_s": now - pos.opened_ts,
                    "move_pct": strategy.favorable_move_pct(price, pos.entry_price, pos.direction),
                    "pnl": pnl,
                    "pnl_gross": gross,
                    "fees": fees,
                    "fee_pct": self.fee_pct,
                    "pnl_pct": pnl / pos.notional * 100.0 if pos.notional else 0.0,
                    "reason": reason,
                    "time_to_mfe_s": (pos.mfe_ts - pos.opened_ts) if pos.mfe_ts else 0.0,
                    "time_to_mae_s": (pos.mae_ts - pos.opened_ts) if pos.mae_ts else 0.0,
                }
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
            if not pos.sent:
                return True                     # simulada: el executor nunca la abrió
            self.executor.send({
                "action":      "close",
                "trade_id":    pos.trade_id,
                "symbol":      symbol,
                "direction":   pos.direction,
                "reason":      reason,
                "close_price": price,
                "pnl":         round(pnl, 6),
                "strategy":    eng.name,
            })
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
                         drop_max_pct: Optional[float] = None) -> dict:
        """Ajusta en caliente el % 24 h para vigilar y el rango de caída del ratio."""
        new = dict(self.ratio_cfg)
        for key, v in (("watch_min_pct", watch_min_pct), ("drop_min_pct", drop_min_pct),
                       ("drop_max_pct", drop_max_pct)):
            if v is not None:
                new[key] = float(v)
        if new["drop_min_pct"] >= new["drop_max_pct"]:
            raise ValueError("la caída mínima debe ser menor que la máxima")
        if new != self.ratio_cfg:
            self.log(f"[config] ratio: 24 h ≥ {self.ratio_cfg['watch_min_pct']:g}% → {new['watch_min_pct']:g}% | "
                     f"rango {self.ratio_cfg['drop_min_pct']:g}–{self.ratio_cfg['drop_max_pct']:g}% → "
                     f"{new['drop_min_pct']:g}–{new['drop_max_pct']:g}%")
            self.ratio_cfg = new
            self._save_settings()
            self.store.mark_dirty(True)
        return dict(self.ratio_cfg)

    async def _ticker_loop(self) -> None:
        """WebSocket de 24 h de todos los símbolos (!miniTicker@arr / !ticker@arr)."""
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
        """Si exchangeInfo falló al arrancar (proxy/región), reintenta cada 60 s."""
        while self.running:
            await asyncio.sleep(60.0)
            if not self.filters:
                self.log("[REST] reintentando exchangeInfo…")
                await self._init_symbols()

    def _on_tickers(self, raw) -> None:
        data = json.loads(raw)
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            return
        now = time.time()
        for t in data:
            try:
                sym = t["s"]
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
            except (KeyError, TypeError, ValueError):
                continue
            self._tick[sym] = (pct, last, now)
        self.ticker_last_ts = now

    def _refresh_watchlist(self) -> None:
        """Recalcula la lista de vigilados (24 h ≥ +X %; con LONG activo, también ≤ -X %)
        y suscribe los nuevos a precios en vivo."""
        wmin = self.ratio_cfg["watch_min_pct"]
        eng = self.engines[ENGINE_RATIO]
        want_short = not eng.blocked[strategy.SHORT]
        want_long = not eng.blocked[strategy.LONG]
        picked = []
        for sym, (pct, _px, _ts) in list(self._tick.items()):
            if pct >= wmin and want_short:
                picked.append((abs(pct), sym, strategy.SHORT))
            elif pct <= -wmin and want_long:
                picked.append((abs(pct), sym, strategy.LONG))
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
        if added and self.price_cache is not None:
            self.price_cache.ensure_symbols(added)          # bookTicker en vivo

    def _ratio_symbols(self) -> List[str]:
        eng = self.engines[ENGINE_RATIO]
        with self.lock:
            open_syms = {p.symbol for p in eng.positions.values()}
        return list(dict.fromkeys(list(self.watch) + sorted(open_syms)))

    def _rest_get(self, path: str, params: dict):
        """GET público a Binance por el proxy (y por IP directa si el proxy falla).
        Un 429/418 NO se reintenta por otra ruta: se pausa todo el REST."""
        routes = [REST_PROXY_URL, ""] if REST_PROXY_URL else [""]
        order = routes[self._rest_route:] + routes[:self._rest_route]
        url = f"{BASE_URL}{path}?{urlencode(params)}"
        last_exc: Optional[Exception] = None
        for proxy in order:
            try:
                handlers = [urllib.request.ProxyHandler({"http": proxy, "https": proxy})] if proxy else []
                opener = urllib.request.build_opener(*handlers)
                req = urllib.request.Request(url, headers={"User-Agent": "BOTKLINE/1.0"})
                with opener.open(req, timeout=15) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
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
        for _ in range(30):                                  # espera los primeros tickers
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

    async def _ratio_cycle(self) -> None:
        self._refresh_watchlist()
        self.ratio_last_cycle_ts = time.time()
        pending = self._ratio_symbols()
        for attempt in range(RATIO_RETRIES + 1):
            if not pending or time.time() < self._rest_block_until:
                break
            if attempt:
                await asyncio.sleep(RATIO_RETRY_GAP_S)       # aún no publicó el punto nuevo de 5 m
            fresh = await self._poll_ratios(pending)
            pending = [s for s in pending if s in fresh and not fresh[s]]
        if time.time() < self._rest_block_until:
            self.log(f"[ratio] ⚠️ REST en pausa {self._rest_block_until - time.time():.0f} s por límite de peticiones")

    async def _poll_ratios(self, symbols: List[str]) -> Dict[str, bool]:
        sem = asyncio.Semaphore(RATIO_CONCURRENCY)
        fresh: Dict[str, bool] = {}

        async def one(sym: str) -> None:
            if time.time() < self._rest_block_until:
                return
            async with sem:
                try:
                    pts = await asyncio.to_thread(self._fetch_ratio, sym)
                except Exception as exc:
                    self.ratio_errors += 1
                    self.ratio_last_error = f"{sym}: {exc}"
                    return
            self.ratio_polls += 1
            try:
                fresh[sym] = self._on_ratio_points(sym, pts)
            except Exception as exc:
                self.ratio_last_error = f"{sym}: {exc!r}"

        await asyncio.gather(*(one(s) for s in symbols))
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

    def _ratio_check_tp(self, sym: str, pts: List[Tuple[int, float]]) -> None:
        """TP: el ratio recupera el valor que tenía antes del gatillo."""
        eng = self.engines[ENGINE_RATIO]
        with self.lock:
            targets = [p for p in eng.positions.values()
                       if p.symbol == sym and (p.ratio_target or p.ratio_trigger) > 0]
        if not targets or not self.store.can_act():
            return
        for p in targets:
            after = [r for ts, r in pts if ts > p.ratio_ts]
            if not after:
                continue
            goal = p.ratio_target or p.ratio_trigger
            if p.direction == strategy.SHORT:
                hit, best = max(after) >= goal, max(after)
            else:
                hit, best = min(after) <= goal, min(after)
            if not hit:
                continue
            price = (self._live_price(sym) or (self.price_cache.get_mark_price(sym) if self.price_cache else None)
                     or p.last_price or p.entry_price)
            self.log(f"[ratio] TP {p.direction} {sym}: ratio {best:g} alcanzó el objetivo {goal:g} "
                     f"(entrada {p.ratio_entry:g}, recupera {abs(p.ratio_change_pct):.1f}%)")
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
        cfg = self.ratio_cfg
        dmin, dmax, wmin = cfg["drop_min_pct"], cfg["drop_max_pct"], cfg["watch_min_pct"]
        if -dmax <= chg <= -dmin:
            side = strategy.SHORT
        elif dmin <= chg <= dmax:
            side = strategy.LONG
        else:
            return
        t = self._tick.get(sym)
        pct24 = t[0] if t else None
        if pct24 is None or (side == strategy.SHORT and pct24 < wmin) or (side == strategy.LONG and pct24 > -wmin):
            return                                  # solo se opera lo que está en la lista de vigilados
        eng = self.engines[ENGINE_RATIO]
        eng.signal_count += 1

        def skip(action: str) -> None:
            self._record_ratio_signal(eng, sym, side, prev, last, chg, action)

        age_s = time.time() - last[0] / 1000
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
            dup = any(p.symbol == sym and p.direction == side for p in eng.positions.values())
            until = eng.cooldown.get(sym, 0.0)
        if dup:
            return skip("ya hay una abierta en este par y dirección")
        if until > time.time():
            return skip("ignorada: en pausa tras cierre")
        price = self._live_price(sym)
        if not price:
            return skip("sin precio en vivo")
        if sym not in self.filters:
            return skip("saltada: sin filtros del símbolo (exchangeInfo no cargó: revisa el proxy)")
        f = self.filters[sym]
        qty, eff, why = strategy.size_order(
            NOTIONAL_USDT, price, f.get("stepSize", 0.001), f.get("minQty", 0.0),
            f.get("minNotional", 5.0), MAX_NOTIONAL_OVER)
        if qty <= 0:
            self.log(f"[{eng.label}] señal {side} {sym} saltada: {why}")
            return skip(f"saltada: {why}")
        # TP: el ratio recupera el mismo % que cayó (SHORT) / subió (LONG), desde el ratio de entrada.
        target = last[1] * (1 + abs(chg) / 100.0) if side == strategy.SHORT else last[1] * (1 - abs(chg) / 100.0)
        pos = Position(
            trade_id=self._next_trade_id(), symbol=sym, direction=side,
            entry_price=price, qty=qty, notional=eff, tp_price=0.0,
            signal_pct=chg, candle_open=prev[1], candle_close=last[1], candle_time=last[0],
            last_price=price, strategy=eng.name, inverted=False, sent=not self.executor_paused,
            ratio_trigger=prev[1], ratio_entry=last[1], ratio_change_pct=chg, ratio_ts=last[0],
            ratio_target=target,
        )
        with self.lock:
            eng.positions[pos.trade_id] = pos
            eng.opened_count += 1
        self._refresh_pos_syms()
        if self.price_cache is not None:
            self.price_cache.ensure_symbols([sym])
        self.store.mark_dirty(True)
        skip(f"abierta #{pos.trade_id}")
        self.log(f"[{eng.label}] ABRE {side} {sym}{'' if pos.sent else ' [SIMULADA: envíos al executor pausados]'} | "
                 f"ratio {prev[1]:g}→{last[1]:g} ({chg:+.1f}%) | 24 h {pct24:+.1f}% | px={price:g} qty={qty:g} "
                 f"({eff:.2f} USDT) | TP cuando el ratio {'≥' if side == strategy.SHORT else '≤'} {target:g} | "
                 f"trade #{pos.trade_id}")
        if pos.sent:
            self.executor.send({
                "action": "open", "trade_id": pos.trade_id, "symbol": sym, "direction": side,
                "price": price, "quantity": qty, "notional": round(eff, 4),
                "level": round(chg, 4), "strategy": eng.name,
            }, time.perf_counter())

    def _ratio_snapshot(self) -> dict:
        eng = self.engines[ENGINE_RATIO]
        cfg = dict(self.ratio_cfg)
        dmin, dmax = cfg["drop_min_pct"], cfg["drop_max_pct"]
        with self.lock:
            open_syms = {p.symbol for p in eng.positions.values()}
        now = time.time()
        rows = []
        for sym in list(dict.fromkeys(list(self.watch) + sorted(open_syms))):
            t = self._tick.get(sym)
            st = self._ratio.get(sym) or {}
            side = self.watch_side.get(sym, "")
            chg = st.get("chg")
            in_range = bool(chg is not None and ((side == strategy.SHORT and -dmax <= chg <= -dmin)
                                                 or (side == strategy.LONG and dmin <= chg <= dmax)))
            rows.append({
                "symbol": sym, "pct24": t[0] if t else None, "price": t[1] if t else None,
                "prev": st.get("prev"), "last": st.get("last"), "chg": chg,
                "data_time": _utc(st["last_ts"] / 1000, "%H:%M") if st.get("last_ts") else "",
                "side": side, "in_range": in_range, "has_position": sym in open_syms,
            })
        rows.sort(key=lambda r: abs(r["pct24"] or 0), reverse=True)
        return {
            "cfg": cfg, "rows": rows, "watch_count": len(self.watch),
            "long_enabled": not eng.blocked[strategy.LONG], "short_enabled": not eng.blocked[strategy.SHORT],
            "ticker_connected": self.ticker_connected, "ticker_symbols": len(self._tick),
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
            "LONG": agg([r for r in rows if r.get("direction") == strategy.LONG]),
            "SHORT": agg([r for r in rows if r.get("direction") == strategy.SHORT]),
        }

    def _engine_snapshot(self, eng: Engine, now: float) -> dict:
        with self.lock:
            positions = []
            for p in sorted(eng.positions.values(), key=lambda p: p.opened_ts, reverse=True):
                price = self._price(p.symbol) or p.last_price
                positions.append({
                    **asdict(p),
                    "price":    price,
                    "move_pct": strategy.favorable_move_pct(price, p.entry_price, p.direction),
                    "pnl":      self._net_pnl(p, price),
                    "age_s":    now - p.opened_ts,
                    "opened":   _utc(p.opened_ts, "%m-%d %H:%M:%S"),
                })
            closed = [{**t, "closed": _utc(t.get("closed_at_ts", 0), "%m-%d %H:%M:%S")}
                      for t in eng.closed_trades[:100]]
            signals = list(eng.signals)[:60]
            wins = sum(1 for t in eng.closed_trades if t.get("pnl", 0) > 0)
            closed_count = len(eng.closed_trades)
            decide = list(eng._decide_ms)
        return {
            "name":           eng.name,
            "label":          eng.label,
            "hint":           ENGINE_HINTS[eng.name],
            "config":         eng.settings(),
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
            "excursions":     self.excursion_stats(eng),
            "evaluations":    (self.evaluations(eng.threshold_pct) if eng.name != ENGINE_RATIO else
                               {"total": 0, "minutes": [], "top": [], "top_minute": ""}),
        }

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

    def snapshot(self) -> dict:
        now = time.time()
        with self.lock:
            events = list(self.events)[:80]
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
                "enabled": KLINE_BOTS_ENABLED,
                "symbols": k1.get("symbols", 0), "with_data": k1.get("with_data", 0),
                "closed_candles": k1.get("closed_candles", 0),
                "live_updates": self.live_updates,
                "connections": ks.get("connections", 0), "active": ks.get("active_connections", 0),
                "reconnects": ks.get("reconnects", 0), "last_error": ks.get("last_error", ""),
            },
            "prices_connected": bool(pc and pc.is_connected()),
            "eval_total":      self.eval_count,
            "ratio":           self._ratio_snapshot(),
            "global":          self._global_snapshot(),
            "engines":         {n: self._engine_snapshot(e, now) for n, e in self.engines.items()},
            "events":          events,
            "last_error":      self.last_error,
            "persistence":     self.store.status(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# FLASK
# ─────────────────────────────────────────────────────────────────────────────

bot = KlineBot()
bot.start()
atexit.register(bot.shutdown)

app = Flask(__name__)


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
    return _no_store(make_response(HTML.replace("__POLL_MS__", str(STATUS_POLL_MS))))


@app.get("/api/status")
def api_status():
    return _no_store(jsonify(bot.snapshot()))


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
    if eng.name == ENGINE_RATIO:
        return jsonify({"ok": False, "error": "El bot de ratio se ajusta en /api/ratio"}), 400
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
    vals = {}
    if "inverted" in data:
        vals["inverted"] = bool(data["inverted"])
    if "executor_paused" in data:
        vals["executor_paused"] = bool(data["executor_paused"])
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
    """Ajusta el bot de ratio. Body JSON (todo opcional): {"watch_min_pct": 20,
    "drop_min_pct": 25, "drop_max_pct": 30}. LONG/SHORT se activan con /api/block
    ({"engine": "ratio", "direction": "LONG", "blocked": false})."""
    busy = _guard()
    if busy:
        return busy
    data = request.get_json(silent=True) or {}
    vals = {}
    for key, hi in (("watch_min_pct", 1000), ("drop_min_pct", 100), ("drop_max_pct", 100)):
        if data.get(key) in (None, ""):
            continue
        try:
            v = float(data[key])
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": f"{key} inválido"}), 400
        if not 0 < v <= hi:
            return jsonify({"ok": False, "error": f"{key} debe estar entre 0 y {hi:g}"}), 400
        vals[key] = v
    if not vals:
        return jsonify({"ok": False, "error": "Nada que cambiar"}), 400
    try:
        cfg = bot.set_ratio_config(**vals)
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


@app.post("/api/pause")
def api_pause():
    """Pausa o reanuda un bot. Body JSON: {"engine": "close"|"live", "paused": true|false}."""
    busy = _guard()
    if busy:
        return busy
    data = request.get_json(silent=True) or {}
    eng, err = _engine_param(data, default=None)
    if err:
        return err
    if eng is None:
        return jsonify({"ok": False, "error": "Falta engine (close o live)"}), 400
    return jsonify({"ok": True, **bot.set_paused(eng, bool(data.get("paused")))})


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
    {"engine": "close"|"live", "direction": "LONG"|"SHORT", "blocked": true|false}."""
    busy = _guard()
    if busy:
        return busy
    data = request.get_json(silent=True) or {}
    eng, err = _engine_param(data)
    if err:
        return err
    direction = str(data.get("direction", "")).upper()
    if direction not in (strategy.LONG, strategy.SHORT):
        return jsonify({"ok": False, "error": "direction debe ser LONG o SHORT"}), 400
    return jsonify({"ok": True, **bot.set_block(eng, direction, bool(data.get("blocked")))})


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
    "time_to_mae_s", "high_price", "low_price", "ratio_target", "ratio_trigger", "ratio_entry", "ratio_change_pct",
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
    snap = bot.snapshot()
    return jsonify({
        "ok": True, "running": bot.running,
        "positions": {n: len(e["positions"]) for n, e in snap["engines"].items()},
        "klines": snap["klines"], "prices_connected": snap["prices_connected"],
        "executor": snap["executor"]["url"], "persistence": snap["persistence"],
        "last_error": snap["last_error"],
    })


@app.get("/api/recovery")
def api_recovery():
    doc = bot.store.last_doc or bot.store.read_local()
    if isinstance(doc, dict) and isinstance(doc.get("executor"), dict):
        doc = {**doc, "executor": {**doc["executor"], "secret": "***"}}
    return _no_store(jsonify({"status": bot.store.status(), "document": doc}))


# ─────────────────────────────────────────────────────────────────────────────
# WEB
# ─────────────────────────────────────────────────────────────────────────────

HTML = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BOTKLINE · velas 1m</title>
<style>
  :root { --bg:#0b1020; --panel:#121933; --panel2:#0f1630; --line:#26304f; --txt:#e8edf8; --muted:#8e9bbb;
          --green:#34d399; --red:#f87171; --amber:#fbbf24; --blue:#60a5fa; --radius:14px; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--txt); font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
         font-variant-numeric:tabular-nums; }
  main { max-width:1240px; margin:0 auto; padding:16px; }
  .muted { color:var(--muted); } .pos { color:var(--green); } .neg { color:var(--red); } .warn { color:var(--amber); }
  small { font-size:12px; }

  /* Cabecera */
  .top { display:flex; flex-wrap:wrap; gap:10px; align-items:center; justify-content:space-between; margin-bottom:12px; }
  .brand { font-size:20px; font-weight:800; letter-spacing:.3px; }
  .brand small { font-weight:400; color:var(--muted); margin-left:6px; }
  .pills { display:flex; flex-wrap:wrap; gap:6px; }
  .pill { border:1px solid var(--line); background:var(--panel2); border-radius:999px; padding:3px 10px; font-size:12px; }
  .pill.ok { border-color:rgba(52,211,153,.4); color:var(--green); }
  .pill.bad { border-color:rgba(248,113,113,.4); color:var(--red); }
  .pill.alert { border-color:rgba(251,191,36,.5); color:var(--amber); }
  #rules { margin:-4px 0 12px; }

  /* Tarjetas de cifras */
  .kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:10px; margin-bottom:14px; }
  .kpi { background:var(--panel); border:1px solid var(--line); border-radius:var(--radius); padding:12px 14px; }
  .kpi .label { color:var(--muted); font-size:12px; }
  .kpi .v { font-size:20px; font-weight:700; margin-top:2px; }
  .bar { height:6px; background:var(--panel2); border-radius:99px; overflow:hidden; margin-top:8px; }
  .bar > i { display:block; height:100%; width:0; background:var(--green); transition:width .4s; }

  /* Secciones plegables */
  details.panel { background:var(--panel); border:1px solid var(--line); border-radius:var(--radius); margin-bottom:12px; }
  details.panel > summary { list-style:none; cursor:pointer; padding:12px 16px; font-weight:700; font-size:15px;
                            display:flex; gap:8px; align-items:center; user-select:none; }
  details.panel > summary::-webkit-details-marker { display:none; }
  details.panel > summary::before { content:'▸'; color:var(--muted); transition:transform .15s; display:inline-block; width:12px; }
  details.panel[open] > summary::before { transform:rotate(90deg); }
  details.panel > summary .hint { font-weight:400; font-size:12px; color:var(--muted); }
  details.panel > summary .badge { margin-left:auto; }
  .body { padding:0 16px 14px; }
  .badge { background:var(--panel2); border:1px solid var(--line); border-radius:999px; padding:1px 9px; font-size:12px; font-weight:600; }

  /* Tarjetas de bots */
  .bots { display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:10px; }
  .bot { background:var(--panel2); border:1px solid var(--line); border-radius:12px; padding:12px; }
  .bot.sel { border-color:var(--blue); }
  .bot.paused { opacity:.8; border-style:dashed; }
  .bot h3 { margin:0; font-size:15px; display:flex; align-items:center; gap:8px; }
  .bot .rows { display:grid; grid-template-columns:repeat(4,1fr); gap:6px; margin:10px 0; }
  .bot .rows div { font-size:12px; color:var(--muted); } .bot .rows b { display:block; color:var(--txt); font-size:14px; }
  .bot .acts { display:flex; gap:6px; flex-wrap:wrap; }

  /* Tablas */
  .scroll { overflow:auto; max-height:430px; border:1px solid var(--line); border-radius:10px; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { padding:7px 9px; border-bottom:1px solid var(--line); text-align:right; white-space:nowrap; }
  th:first-child, td:first-child { text-align:left; }
  thead th { position:sticky; top:0; background:#162043; color:var(--muted); font-weight:600; z-index:1; }
  tbody tr:hover td { background:rgba(96,165,250,.05); }
  .tag { padding:2px 8px; border-radius:999px; font-weight:700; font-size:11px; }
  .LONG { background:rgba(52,211,153,.15); color:var(--green); } .SHORT { background:rgba(248,113,113,.15); color:var(--red); }
  .st { padding:2px 8px; border-radius:999px; font-size:11px; font-weight:700; }
  .st.on { background:rgba(52,211,153,.15); color:var(--green); } .st.off { background:rgba(251,191,36,.15); color:var(--amber); }

  /* Formularios y botones */
  form, .row { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
  input { flex:1 1 200px; background:var(--panel2); color:var(--txt); border:1px solid var(--line); border-radius:8px; padding:8px 10px; font:inherit; }
  label.f { flex:1 1 200px; display:flex; flex-direction:column; gap:4px; font-size:12px; color:var(--muted); }
  label.f input { flex:none; }
  button { background:var(--blue); color:#06101f; border:0; border-radius:8px; padding:8px 14px; font-weight:700; cursor:pointer; font:inherit; font-weight:700; }
  button:hover { filter:brightness(1.1); }
  button.ghost { background:#24305a; color:var(--txt); }
  button.danger { background:var(--red); color:#1a0606; }
  button.ok { background:var(--green); }
  button.warnb { background:var(--amber); }
  button.small { padding:4px 10px; font-size:12px; }
  .msg { margin-top:8px; min-height:18px; }
  .sub { font-size:13px; font-weight:700; margin:14px 0 8px; color:var(--muted); }

  /* Pestañas */
  .tabs { display:flex; gap:8px; flex-wrap:wrap; position:sticky; top:0; z-index:5; background:var(--bg); padding:10px 0; margin:6px 0 4px; }
  .tabs button { background:#24305a; color:var(--txt); padding:9px 16px; }
  .tabs button.on { background:var(--blue); color:#06101f; }
  .tabs button.on .pos, .tabs button.on .neg { color:#06101f; }
  .tabs button * { pointer-events:none; }
  #engHint { margin:0 0 10px; }
  pre { margin:0; max-height:320px; overflow:auto; font-size:12px; color:var(--muted); white-space:pre-wrap; }
  @media (max-width:640px) { .bot .rows { grid-template-columns:repeat(2,1fr); } }
</style>
</head>
<body>
<main>
  <div class="top">
    <div class="brand">BOTKLINE <small>ratio top traders · short a los que más suben</small></div>
    <div class="pills" id="pills"></div>
  </div>
  <div class="muted" id="rules"></div>

  <section class="kpis">
    <div class="kpi"><div class="label">PnL de la sesión <span id="sessTarget"></span></div><div class="v" id="sessPnl">—</div>
      <div class="bar"><i id="sessBar"></i></div><small class="muted" id="sessDetail"></small></div>
    <div class="kpi"><div class="label">PnL realizado (total)</div><div class="v" id="sRpnlAll">—</div><small class="muted" id="sWins"></small></div>
    <div class="kpi"><div class="label">PnL abierto (total)</div><div class="v" id="sUpnlAll">—</div></div>
    <div class="kpi"><div class="label">Posiciones abiertas</div><div class="v" id="sOpen">—</div><small class="muted" id="sOpenLim"></small></div>
    <div class="kpi"><div class="label" id="kKlLabel">Vigilados (24 h)</div><div class="v" id="sKl">—</div><small class="muted" id="sEval"></small></div>
  </section>

  <details class="panel" data-key="control" open>
    <summary>Control <span class="hint">pausar bots, invertir, cerrar todo, objetivo y límite</span></summary>
    <div class="body">
      <div class="bots" id="bots"></div>

      <div class="sub">Acciones para los dos bots</div>
      <div class="row">
        <button type="button" id="btnInvert">—</button>
        <button type="button" class="danger" id="btnCloseAll">Cerrar TODAS las posiciones</button>
        <button type="button" id="btnExPause">—</button>
        <button type="button" class="ghost" id="btnReset">Poner a cero el PnL de la sesión</button>
      </div>
      <div class="muted" id="modeHint" style="margin-top:8px"></div>
      <div class="muted" id="sessHits" style="margin-top:4px"></div>

      <div class="sub">Ajustes</div>
      <form id="glbForm">
        <label class="f">Objetivo en USD para cerrar todo (0 = apagado) <span id="curTarget"></span>
          <input id="glbTarget" type="number" step="0.01" min="0" placeholder="ej. 12"></label>
        <label class="f">Límite de posiciones abiertas, los dos bots (0 = sin límite) <span id="curMax"></span>
          <input id="glbMax" type="number" step="1" min="0" placeholder="ej. 10"></label>
        <label class="f">Fee % por lado <span id="curFee"></span>
          <input id="glbFee" type="number" step="0.001" min="0" max="1" placeholder="ej. 0.05"></label>
        <button type="submit" style="align-self:flex-end">Guardar</button>
      </form>
      <div class="row" style="margin-top:8px" id="tokenRow">
        <input id="exToken" type="password" placeholder="Token admin (si está configurado)" autocomplete="off" style="display:none;max-width:320px">
      </div>
      <div id="glbMsg" class="muted msg"></div>
    </div>
  </details>

  <details class="panel" data-key="ratio" open>
    <summary>Ratio top traders <span class="hint" id="rtSum"></span> <span class="badge" id="rtN">0</span></summary>
    <div class="body">
      <div class="muted" id="rtInfo"></div>
      <form id="rtForm" style="margin-top:10px">
        <label class="f">Vigilar símbolos con 24 h ≥ ±% <span id="rtCurW"></span>
          <input id="rtWatch" type="number" step="0.1" min="0.1" max="1000" placeholder="ej. 20"></label>
        <label class="f">Caída MÍNIMA del ratio, % <span id="rtCurMin"></span>
          <input id="rtMin" type="number" step="0.1" min="0.1" max="100" placeholder="ej. 25"></label>
        <label class="f">Caída MÁXIMA del ratio, % <span id="rtCurMax"></span>
          <input id="rtMax" type="number" step="0.1" min="0.1" max="100" placeholder="ej. 30"></label>
        <button type="submit" style="align-self:flex-end">Guardar</button>
      </form>
      <div class="row" style="margin-top:8px">
        <button type="button" id="rtBtnShort">SHORT</button>
        <button type="button" id="rtBtnLong">LONG</button>
        <span class="muted">SHORT: ganadores (24 h ≥ +X %), el ratio cae. LONG (espejo): perdedores (24 h ≤ −X %), el ratio sube.</span>
      </div>
      <div id="rtMsg" class="muted msg"></div>
      <div class="scroll"><table>
        <thead><tr><th>Símbolo</th><th>24 h</th><th>Precio</th><th>Ratio antes</th><th>Ratio ahora</th><th>Cambio</th><th>Dato</th><th>Lado</th><th>Estado</th></tr></thead>
        <tbody id="tRt"></tbody></table></div>
    </div>
  </details>

  <div class="tabs" id="tabs"></div>
  <div class="muted" id="engHint"></div>

  <details class="panel" data-key="pos" open>
    <summary>Posiciones abiertas · <span class="engName"></span> <span class="badge" id="nPos">0</span></summary>
    <div class="body">
      <div class="row" style="margin-bottom:8px">
        <span class="muted" id="engPnl"></span>
        <button type="button" class="danger small" id="btnCloseEng" style="margin-left:auto">Cerrar todas las de este bot</button>
      </div>
      <div class="scroll"><table>
        <thead><tr><th>Símbolo</th><th>Dir.</th><th>Vela</th><th>Entrada</th><th>Precio</th><th>Avance</th><th>Salida</th><th>Notional</th><th>PnL neto</th><th>MFE</th><th>MAE</th><th>Abierta</th><th></th></tr></thead>
        <tbody id="tPos"></tbody></table></div>
    </div>
  </details>

  <details class="panel" data-key="cfg">
    <summary>Parámetros · <span class="engName"></span> <span class="hint" id="cfgSum"></span></summary>
    <div class="body">
      <div class="muted">Señal actual: <b id="cfgThr">—</b> · <span id="cfgTpLbl">TP</span> actual: <b id="cfgTp">—</b> · <span id="engLat"></span></div>
      <form id="cfgForm" style="margin-top:10px">
        <input id="cfgNewThr" type="number" step="0.01" min="0.01" max="100" placeholder="% cuerpo de vela para entrar (ej. 3)">
        <input id="cfgNewTp" type="number" step="0.01" min="0.01" max="100" placeholder="% take profit (normal) / stop loss (invertido)">
        <button type="submit">Guardar</button>
      </form>
      <div id="cfgMsg" class="muted msg"></div>
      <div class="row">
        <button type="button" id="btnLong" data-block="LONG">LONG</button>
        <button type="button" id="btnShort" data-block="SHORT">SHORT</button>
        <span class="muted">Solo afecta a este bot. Bloquear frena las entradas nuevas de esa dirección.</span>
      </div>
    </div>
  </details>

  <details class="panel" data-key="sig">
    <summary>Señales recientes · <span class="engName"></span> <span class="badge" id="nSig">0</span></summary>
    <div class="body"><div class="scroll"><table>
      <thead><tr><th>Hora</th><th>Símbolo</th><th>Dir.</th><th id="thSigC">Vela</th><th id="thSigB">Cuerpo</th><th id="thSigS">Segundo</th><th>Acción</th></tr></thead>
      <tbody id="tSig"></tbody></table></div></div>
  </details>

  <details class="panel" data-key="closed">
    <summary>Operaciones cerradas · <span class="engName"></span> <span class="badge" id="nClosed">0</span></summary>
    <div class="body"><div class="scroll"><table>
      <thead><tr><th>Cierre</th><th>Símbolo</th><th>Dir.</th><th>Entrada</th><th>Salida</th><th>Avance</th><th>PnL neto</th><th>Fees</th><th>MFE</th><th>MAE</th><th>Duración</th><th>Motivo</th></tr></thead>
      <tbody id="tClosed"></tbody></table></div></div>
  </details>

  <details class="panel" data-key="exc">
    <summary>MFE / MAE de las cerradas · <span class="engName"></span> <a id="csvLink" class="hint" href="/api/trades.csv" onclick="event.stopPropagation()">descargar CSV</a></summary>
    <div class="body"><div class="scroll"><table>
      <thead><tr><th>Dirección</th><th>Ops</th><th>Ganadas</th><th>MFE medio</th><th>MAE medio</th><th>Mejor MFE</th><th>Peor MAE</th><th>MAE medio USD</th><th>Hasta MAE</th><th>Duración media</th></tr></thead>
      <tbody id="tExc"></tbody></table></div></div>
  </details>

  <details class="panel" data-key="eval">
    <summary>Evaluaciones <span class="hint">cuerpo al cierre, con el umbral de <span class="engName"></span></span></summary>
    <div class="body">
      <div class="sub" style="margin-top:0">Mayores cuerpos de la vela <span id="evMin"></span></div>
      <div class="scroll"><table>
        <thead><tr><th>Símbolo</th><th>Apertura</th><th>Cierre</th><th>Cuerpo</th><th>Señal</th></tr></thead>
        <tbody id="tTop"></tbody></table></div>
      <div class="sub">Por minuto</div>
      <div class="scroll"><table>
        <thead><tr><th>Vela</th><th>Evaluadas</th><th>≥ umbral (LONG)</th><th>≤ −umbral (SHORT)</th><th>Mayor subida</th><th>Mayor caída</th></tr></thead>
        <tbody id="tMin"></tbody></table></div>
    </div>
  </details>

  <details class="panel" data-key="exec">
    <summary>Executor <span class="hint" id="exSum"></span></summary>
    <div class="body">
      <div class="muted">Actual: <b id="exUrl">—</b> · enviadas <span id="exSent">0</span> · errores <span id="exErr">0</span></div>
      <div class="muted" id="exLat" style="margin-top:4px"></div>
      <form id="exForm" style="margin-top:10px">
        <input id="exNew" type="url" placeholder="https://mi-executor.onrender.com" autocomplete="off">
        <input id="exSecret" type="password" placeholder="Secreto (opcional, vacío = mantener)" autocomplete="off">
        <button type="submit">Cambiar link</button>
      </form>
      <div id="exMsg" class="muted msg"></div>
    </div>
  </details>

  <details class="panel" data-key="events">
    <summary>Eventos <span class="hint">los dos bots</span></summary>
    <div class="body"><pre id="events"></pre></div>
  </details>
</main>
<script>
const q = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num = (v, d=4) => (v === null || v === undefined || isNaN(v)) ? '—' : (+v).toLocaleString('es', {maximumFractionDigits:d});
const pct = v => `<span class="${v >= 0 ? 'pos' : 'neg'}">${v >= 0 ? '+' : ''}${(+v).toFixed(2)}%</span>`;
const usd = v => `<span class="${v >= 0 ? 'pos' : 'neg'}">${v >= 0 ? '+' : ''}${(+v).toFixed(4)}</span>`;
const dur = s => { s = Math.floor(s); const h = Math.floor(s/3600), m = Math.floor(s%3600/60);
                   return h ? `${h}h ${m}m` : m ? `${m}m ${s%60}s` : `${s}s`; };
const tag = d => `<span class="tag ${esc(d)}">${esc(d)}</span>`;
const store = {get(k, d) { try { const v = localStorage.getItem(k); return v === null ? d : v; } catch (e) { return d; } },
               set(k, v) { try { localStorage.setItem(k, v); } catch (e) {} }};
let cur = store.get('botkline_engine', 'ratio'), last = null;
q('exToken').value = store.get('botkline_token', '');

// Secciones plegables: se recuerda cuáles están abiertas.
const folds = JSON.parse(store.get('botkline_folds', '{}') || '{}');
document.querySelectorAll('details.panel').forEach(d => {
  const k = d.dataset.key;
  if (k in folds) d.open = folds[k];
  d.addEventListener('toggle', () => { folds[k] = d.open; store.set('botkline_folds', JSON.stringify(folds)); });
});

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

async function refresh() {
  try { last = await (await fetch('/api/status', {cache: 'no-store'})).json(); } catch (e) { return; }
  render(last);
}

function render(d) {
  const c = d.config, g = d.global, names = Object.keys(d.engines);
  if (!d.engines[cur]) cur = names[0];
  const e = d.engines[cur], ec = e.config, xk = g.inverted ? 'SL' : 'TP';
  const sum = f => names.reduce((s, n) => s + f(d.engines[n]), 0);

  // Cabecera
  const k = d.klines, p = d.persistence || {}, x = d.executor;
  q('pills').innerHTML = [
    `<span class="pill ${g.inverted ? 'alert' : 'ok'}">${g.inverted ? 'Modo INVERTIDO' : 'Modo normal'}</span>`,
    ...names.map(n => `<span class="pill ${d.engines[n].config.paused ? 'alert' : 'ok'}">${esc(d.engines[n].label)}: ${d.engines[n].config.paused ? 'pausado' : 'en marcha'}</span>`),
    ...(k.enabled ? [`<span class="pill ${k.active ? 'ok' : 'bad'}">Velas ${k.active ? 'conectadas' : 'desconectadas'}</span>`] : []),
    ...(d.ratio ? [`<span class="pill ${d.ratio.ticker_connected ? 'ok' : 'bad'}">Ticker 24 h ${d.ratio.ticker_connected ? 'conectado' : 'desconectado'}</span>`,
                   `<span class="pill ${d.ratio.rest_blocked_s > 0 ? 'bad' : 'ok'}">Ratio REST ${d.ratio.rest_blocked_s > 0 ? 'en pausa' : 'ok'}</span>`] : []),
    `<span class="pill ${!x.url ? 'bad' : g.executor_paused ? 'alert' : (x.errors && x.last_error ? 'alert' : 'ok')}">Executor ${!x.url ? 'sin link' : g.executor_paused ? 'PAUSADO (prueba)' : 'activo'}</span>`,
    `<span class="pill ${p.mode === 'ok' || p.mode === 'local' ? 'ok' : 'alert'}">Estado ${esc(p.mode)}</span>`,
  ].join('');
  q('rules').textContent = g.inverted
    ? `Cuerpo de la vela 1m ≥ umbral → entra CONTRA la vela · el % es stop loss, sin TP · ${c.notional} USDT por entrada · fee ${g.fee_pct}% por lado`
    : `Cuerpo de la vela 1m ≥ umbral → sigue la tendencia · el % es take profit, sin stop loss · ${c.notional} USDT por entrada · fee ${g.fee_pct}% por lado`;
  if (d.ratio) {
    const rc = d.ratio.cfg;
    q('rules').textContent = `Ratio top traders: vigila 24 h ≥ ±${rc.watch_min_pct}% · SHORT si el ratio cae ${rc.drop_min_pct}–${rc.drop_max_pct}% en 5 min · TP cuando el ratio recupera el mismo % que cayó · ${c.notional} USDT por entrada · fee ${g.fee_pct}% por lado`;
  }

  // Cifras
  q('sessPnl').innerHTML = usd(g.session_pnl);
  q('sessTarget').textContent = g.profit_target_usd > 0 ? `· objetivo +${g.profit_target_usd} USD` : '· objetivo apagado';
  q('sessBar').style.width = g.profit_target_usd > 0 ? `${Math.max(0, Math.min(100, g.session_pnl / g.profit_target_usd * 100))}%` : '0';
  q('sessDetail').innerHTML = `realizado ${usd(g.session_realized)} + abierto ${usd(g.session_unrealized)} · desde ${esc(g.session_since)}`;
  q('sRpnlAll').innerHTML = usd(sum(n => n.realized_pnl));
  q('sWins').textContent = `${sum(n => n.wins)}/${sum(n => n.closed_count)} ganadas · netas de fees`;
  q('sUpnlAll').innerHTML = usd(sum(n => n.unrealized_pnl));
  q('sOpen').textContent = g.open_count;
  q('sOpenLim').textContent = g.max_open > 0 ? `límite ${g.max_open}` : 'sin límite';
  if (d.ratio) {
    q('sKl').innerHTML = `${d.ratio.watch_count}`;
    q('sEval').textContent = `de ${d.ratio.ticker_symbols} símbolos · lista de ${d.ratio.watch_updated || '—'}`;
  } else {
    q('sKl').innerHTML = `${k.with_data}/${k.symbols}`;
    q('sEval').textContent = `${k.active}/${k.connections} conexiones · ${d.eval_total.toLocaleString('es')} velas evaluadas`;
  }

  // Control
  const rcfg = (d.ratio || {}).cfg || {};
  q('bots').innerHTML = names.map(n => { const b = d.engines[n], bc = b.config;
    const bl = [bc.block_long && 'LONG', bc.block_short && 'SHORT'].filter(Boolean).join(', ');
    return `<div class="bot ${n === cur ? 'sel' : ''} ${bc.paused ? 'paused' : ''}">
      <h3>${esc(b.label)} <span class="st ${bc.paused ? 'off' : 'on'}">${bc.paused ? 'PAUSADO' : 'EN MARCHA'}</span></h3>
      <div class="muted" style="font-size:12px;margin-top:2px">${esc(b.hint)}</div>
      <div class="rows">
        ${n === 'ratio' ? `<div>Caída ratio<b>${rcfg.drop_min_pct}–${rcfg.drop_max_pct}%</b></div><div>TP<b>ratio recupera %</b></div>`
                        : `<div>Señal<b>±${bc.threshold_pct}%</b></div><div>${xk}<b>${bc.tp_pct}%</b></div>`}
        <div>Abiertas<b>${b.positions.length}</b></div><div>Bloqueados<b class="${bl ? 'neg' : ''}">${bl || '—'}</b></div>
        <div>Señales / abiertas<b>${b.signal_count} / ${b.opened_count}</b></div><div>Cerradas<b>${b.closed_count} (${b.wins} gan.)</b></div>
        <div>PnL abierto<b>${usd(b.unrealized_pnl)}</b></div><div>PnL realizado<b>${usd(b.realized_pnl)}</b></div>
      </div>
      <div class="acts">
        <button type="button" class="small ${bc.paused ? 'ok' : 'warnb'}" data-pause="${esc(n)}" data-paused="${bc.paused ? '1' : ''}">${bc.paused ? 'Reanudar' : 'Pausar'}</button>
        <button type="button" class="small ghost" data-engine="${esc(n)}">Ver detalle</button>
      </div></div>`; }).join('');
  const bi = q('btnInvert');
  bi.dataset.inverted = g.inverted ? '1' : '';
  bi.textContent = g.inverted ? 'Modo INVERTIDO · volver a normal' : 'Modo normal · invertir';
  bi.className = g.inverted ? 'warnb' : 'ok';
  const bx = q('btnExPause');
  bx.dataset.paused = g.executor_paused ? '1' : '';
  bx.textContent = g.executor_paused ? 'Envíos al executor PAUSADOS · reanudar' : 'Pausar entradas al executor (prueba)';
  bx.className = g.executor_paused ? 'warnb' : 'ghost';
  q('modeHint').textContent = (g.executor_paused ? 'MODO PRUEBA: las entradas nuevas solo se simulan aquí, no se mandan al executor. ' : '') + (g.inverted
    ? 'Invertido: vela verde → SHORT, roja → LONG. El % de cada bot es stop loss; no hay TP.'
    : 'Normal: vela verde → LONG, roja → SHORT. El % de cada bot es take profit; no hay stop loss.');
  q('sessHits').textContent = g.target_hits ? `Objetivo alcanzado ${g.target_hits} vez/veces · último: ${g.last_target || '—'}` : 'Al llegar al objetivo se cierran todas las posiciones de los dos bots y el contador vuelve a cero.';
  q('curTarget').textContent = `(ahora ${g.profit_target_usd > 0 ? '+' + g.profit_target_usd : 'apagado'})`;
  q('curMax').textContent = `(ahora ${g.max_open > 0 ? g.max_open : 'sin límite'})`;
  q('curFee').textContent = `(ahora ${g.fee_pct}%)`;
  q('exToken').style.display = c.admin_token ? '' : 'none';
  q('tokenRow').style.display = c.admin_token ? '' : 'none';

  // Pestañas
  q('tabs').innerHTML = names.map(n => { const b = d.engines[n];
    return `<button type="button" data-engine="${esc(n)}" class="${n === cur ? 'on' : ''}">${esc(b.label)}${b.config.paused ? ' (pausado)' : ''} <span>· ${b.positions.length} abiertas · </span>${usd(b.realized_pnl)}</button>`; }).join('');
  q('engHint').textContent = `${e.label}: ${e.hint}.`;
  document.querySelectorAll('.engName').forEach(el => el.textContent = e.label);
  q('csvLink').href = '/api/trades.csv?engine=' + encodeURIComponent(cur);

  // Posiciones
  q('nPos').textContent = e.positions.length;
  q('engPnl').innerHTML = `PnL abierto ${usd(e.unrealized_pnl)} · realizado ${usd(e.realized_pnl)} (${e.wins}/${e.closed_count} ganadas)`;
  const rn = {}; ((d.ratio || {}).rows || []).forEach(x => rn[x.symbol] = x);
  q('tPos').innerHTML = e.positions.map(r => `<tr>
    <td><b>${esc(r.symbol)}</b> <span class="muted">#${r.trade_id}</span>${r.sent === false ? ' <span class="st off">SIMULADA</span>' : ''}</td><td>${tag(r.direction)}</td>
    <td>${cur === 'ratio' ? `${num(r.ratio_trigger, 4)}→${num(r.ratio_entry, 4)} ${pct(r.ratio_change_pct || 0)}` : pct(r.signal_pct) + (cur === 'live' ? ` <span class="muted">${Math.round(r.candle_age_s || 0)}s</span>` : '')}</td><td>${num(r.entry_price, 8)}</td><td>${num(r.price, 8)}</td>
    <td>${pct(r.move_pct)}</td><td>${cur === 'ratio' ? `<span class="muted">TP ratio ${r.direction === 'SHORT' ? '≥' : '≤'}</span> ${num(r.ratio_target || r.ratio_trigger, 4)} <span class="muted">(ahora ${rn[r.symbol] && rn[r.symbol].last != null ? num(rn[r.symbol].last, 4) : '—'})</span>` : `<span class="muted">${r.inverted ? 'SL' : 'TP'}</span> ${num(r.tp_price, 8)}`}</td><td>${num(r.notional, 2)}</td><td>${usd(r.pnl)}</td>
    <td>${pct(r.mfe_pct)} <span class="muted">${usd(r.mfe_usd)}</span></td><td>${pct(r.mae_pct)} <span class="muted">${usd(r.mae_usd)}</span></td>
    <td>${esc(r.opened)} <span class="muted">(${dur(r.age_s)})</span></td>
    <td><button class="small ghost" data-close="${r.trade_id}" data-label="${esc(r.direction)} ${esc(r.symbol)} #${r.trade_id}">Cerrar</button></td></tr>`).join('')
    || '<tr><td colspan="13" class="muted">Sin posiciones abiertas</td></tr>';

  // Parámetros (el bot de ratio se ajusta en su propio panel)
  const isRatio = cur === 'ratio';
  document.querySelector('details[data-key="cfg"]').style.display = isRatio ? 'none' : '';
  document.querySelector('details[data-key="eval"]').style.display = isRatio ? 'none' : '';
  q('thSigC').textContent = isRatio ? 'Ratio' : 'Vela';
  q('thSigB').textContent = isRatio ? 'Cambio' : 'Cuerpo';
  q('thSigS').textContent = isRatio ? '' : 'Segundo';
  q('cfgThr').textContent = `±${ec.threshold_pct}%`;
  q('cfgTp').textContent = `${ec.tp_pct}%`;
  q('cfgTpLbl').textContent = xk;
  q('cfgSum').textContent = `señal ±${ec.threshold_pct}% · ${xk} ${ec.tp_pct}%`;
  q('engLat').textContent = e.decide_ms === null ? 'decisión: sin entradas aún' : `decisión media ${e.decide_ms} ms`;
  for (const [id, dir, blocked] of [['btnLong', 'LONG', ec.block_long], ['btnShort', 'SHORT', ec.block_short]]) {
    const b = q(id);
    b.dataset.blocked = blocked ? '1' : '';
    b.textContent = blocked ? `${dir} bloqueados · desbloquear` : `${dir} activos · bloquear`;
    b.className = blocked ? 'danger' : 'ok';
  }

  // Señales y cerradas
  q('nSig').textContent = e.signal_count;
  q('tSig').innerHTML = e.signals.map(s => `<tr><td>${esc(s.time)}</td><td>${esc(s.symbol)}</td><td>${tag(s.direction)}</td>
    <td>${esc(s.candle)} <span class="muted">${num(s.open, 8)}→${num(s.close, 8)}</span></td><td>${pct(s.body_pct)}</td>
    <td class="muted">${isRatio ? '' : Math.round(s.candle_age_s || 0) + 's'}</td>
    <td class="${s.action.startsWith('abierta') ? 'pos' : 'muted'}">${esc(s.action)}</td></tr>`).join('')
    || `<tr><td colspan="7" class="muted">${isRatio ? 'Esperando una caída del ratio dentro del rango' : 'Esperando velas con cuerpo ≥ ±' + ec.threshold_pct + '%'}</td></tr>`;
  q('nClosed').textContent = e.closed_count;
  q('tClosed').innerHTML = e.closed_trades.map(t => `<tr><td>${esc(t.closed)}</td><td>${esc(t.symbol)}</td><td>${tag(t.direction)}</td>
    <td>${num(t.entry_price, 8)}</td><td>${num(t.close_price, 8)}</td><td>${pct(t.move_pct)}</td><td>${usd(t.pnl)}</td>
    <td class="muted">${num(t.fees || 0, 4)}</td><td>${pct(t.mfe_pct || 0)}</td><td>${pct(t.mae_pct || 0)}</td>
    <td>${dur(t.duration_s || 0)}</td><td>${esc(t.reason)}${t.sent === false ? ' <span class="st off">SIMULADA</span>' : ''}</td></tr>`).join('')
    || '<tr><td colspan="12" class="muted">Aún no hay cierres</td></tr>';

  const exc = e.excursions;
  q('tExc').innerHTML = ['all', 'LONG', 'SHORT'].map(kk => {
    const r = exc[kk], label = kk === 'all' ? 'Todas' : tag(kk);
    if (!r.n) return `<tr><td>${label}</td><td>0</td><td colspan="8" class="muted">—</td></tr>`;
    return `<tr><td>${label}</td><td>${r.n}</td><td>${r.wins}</td><td>${pct(r.avg_mfe_pct)}</td><td>${pct(r.avg_mae_pct)}</td>
      <td>${pct(r.best_mfe_pct)}</td><td>${pct(r.worst_mae_pct)}</td><td>${usd(r.avg_mae_usd)}</td>
      <td>${dur(r.avg_time_to_mae_s)}</td><td>${dur(r.avg_duration_s)}</td></tr>`;
  }).join('');

  // Evaluaciones
  const ev = e.evaluations;
  q('evMin').textContent = ev.top_minute;
  q('tTop').innerHTML = ev.top.map(r => `<tr><td>${esc(r.symbol)}</td><td>${num(r.open, 8)}</td><td>${num(r.close, 8)}</td>
    <td>${pct(r.body_pct)}</td><td>${r.signal ? tag(r.signal) : '<span class="muted">—</span>'}</td></tr>`).join('')
    || '<tr><td colspan="5" class="muted">Esperando el primer cierre de vela</td></tr>';
  q('tMin').innerHTML = ev.minutes.map(m => `<tr><td>${esc(m.minute)}</td><td>${m.evaluated}</td><td>${m.long}</td><td>${m.short}</td>
    <td>${pct(m.max_up)}</td><td>${pct(m.max_down)}</td></tr>`).join('')
    || '<tr><td colspan="6" class="muted">Sin evaluaciones todavía</td></tr>';

  // Executor y eventos
  q('exUrl').textContent = x.url || '(vacío: sin señales)';
  q('exSum').textContent = x.url ? `${x.url} · ${x.sent} enviadas · ${x.errors} errores` : 'sin link: no se mandan señales';
  q('exSent').textContent = x.sent;
  q('exErr').textContent = x.errors;
  q('exLat').textContent = x.last_ok
    ? `Última señal (${x.last_ok}): HTTP al executor ${x.http_ms} ms · total desde el mensaje de Binance ${x.total_ms} ms`
    : 'Latencia: aún no hay señales enviadas';
  renderRatio(d);
  q('events').textContent = d.events.join('\n');
}

q('exForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  const body = {url: q('exNew').value.trim()};
  const secret = q('exSecret').value;
  if (secret) body.secret = secret;
  if (!body.url && !confirm('Link vacío: los dos bots dejarán de mandar señales. ¿Seguir?')) return;
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

q('cfgForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  q('cfgMsg').textContent = 'Guardando…';
  try {
    const r = await post('/api/config', {engine: cur, threshold_pct: q('cfgNewThr').value, tp_pct: q('cfgNewTp').value});
    q('cfgMsg').innerHTML = r.ok
      ? `<span class="pos">Guardado en ${esc(last && last.engines[r.engine] ? last.engines[r.engine].label : r.engine)}.</span> Señal ±${r.threshold_pct}% · ${last && last.global.inverted ? 'SL' : 'TP'} ${r.tp_pct}%`
      : `<span class="neg">${esc(r.error)}</span>`;
    if (r.ok) { q('cfgNewThr').value = ''; q('cfgNewTp').value = ''; }
  } catch (e) { q('cfgMsg').innerHTML = `<span class="neg">${esc(e)}</span>`; }
  refresh();
});

async function glb(body, url = '/api/global') {
  q('glbMsg').textContent = 'Guardando…';
  try {
    const r = await post(url, body);
    q('glbMsg').innerHTML = r.ok
      ? `<span class="pos">Guardado.</span> ${r.executor_paused ? 'Envíos al executor pausados · ' : ''}${r.inverted ? 'Modo invertido' : 'Modo normal'} · objetivo ${r.profit_target_usd > 0 ? '+' + r.profit_target_usd + ' USD' : 'apagado'} · límite ${r.max_open > 0 ? r.max_open + ' abiertas' : 'sin límite'} · fee ${r.fee_pct}% por lado`
      : `<span class="neg">${esc(r.error)}</span>`;
    return r.ok;
  } catch (e) { q('glbMsg').innerHTML = `<span class="neg">${esc(e)}</span>`; }
  finally { refresh(); }
}

q('glbForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  if (await glb({profit_target_usd: q('glbTarget').value, max_open: q('glbMax').value, fee_pct: q('glbFee').value})) {
    q('glbTarget').value = ''; q('glbMax').value = ''; q('glbFee').value = '';
  }
});

q('btnInvert').addEventListener('click', () => {
  const inv = !q('btnInvert').dataset.inverted;
  const msg = inv
    ? '¿Invertir los dos bots? Las entradas nuevas irán CONTRA la vela y el % será stop loss, sin TP. Las posiciones abiertas siguen con su TP.'
    : '¿Volver al modo normal? Las entradas nuevas seguirán la vela con TP. Las posiciones abiertas invertidas siguen con su SL.';
  if (confirm(msg)) glb({inverted: inv});
});

q('btnExPause').addEventListener('click', () => {
  const p = !q('btnExPause').dataset.paused;
  const msg = p
    ? '¿Pausar las entradas al executor? Los bots seguirán operando en simulación (verás posiciones SIMULADAS) pero no se mandará ninguna apertura. Los cierres de lo ya enviado sí se mandan.'
    : '¿Reanudar los envíos al executor? Las entradas nuevas volverán a abrirse de verdad. Las simuladas que sigan abiertas no se mandan.';
  if (confirm(msg)) glb({executor_paused: p});
});

q('btnReset').addEventListener('click', () => {
  if (confirm('¿Poner a cero el PnL de la sesión? El objetivo se contará desde ahora.')) glb({}, '/api/session/reset');
});

async function closeAll(engine) {
  const n = engine ? last.engines[engine].positions.length : last.global.open_count;
  const who = engine ? `del ${last.engines[engine].label}` : 'de los dos bots';
  if (!n) { alert(`No hay posiciones abiertas ${who}.`); return; }
  if (!confirm(`¿Cerrar a mercado las ${n} posiciones abiertas ${who}?`)) return;
  try {
    const r = await post('/api/close-all', engine ? {engine} : {});
    alert(r.ok ? r.msg : r.error);
  } catch (e) { alert(e); }
  refresh();
}
q('btnCloseAll').addEventListener('click', () => closeAll(null));
q('btnCloseEng').addEventListener('click', () => closeAll(cur));

document.addEventListener('click', async ev => {
  const t = ev.target, ds = t.dataset || {};
  if (ds.pause) {
    const paused = !ds.paused, label = last ? last.engines[ds.pause].label : ds.pause;
    if (!confirm(paused ? `¿Pausar el ${label}? No abrirá entradas nuevas; sus posiciones abiertas siguen hasta su TP/SL.` : `¿Reanudar el ${label}?`)) return;
    try { const r = await post('/api/pause', {engine: ds.pause, paused}); if (!r.ok) alert(r.error); } catch (e) { alert(e); }
    refresh();
    return;
  }
  const tab = t.closest && t.closest('[data-engine]');
  if (tab) {
    cur = tab.dataset.engine;
    store.set('botkline_engine', cur);
    q('cfgMsg').textContent = '';
    if (last) render(last);
    if (tab.closest('#bots')) q('tabs').scrollIntoView({behavior: 'smooth'});
    return;
  }
  if (ds.block) {
    try { const r = await post('/api/block', {engine: cur, direction: ds.block, blocked: !ds.blocked}); if (!r.ok) alert(r.error); } catch (e) { alert(e); }
    refresh();
    return;
  }
  if (!ds.close || !confirm(`¿Cerrar ${ds.label || ds.close} a mercado?`)) return;
  try {
    const r = await post('/api/close/' + encodeURIComponent(ds.close), {});
    alert(r.ok ? r.msg : (r.error || r.msg));
  } catch (e) { alert(e); }
  refresh();
});

function renderRatio(d) {
  const r = d.ratio; if (!r) return;
  const c = r.cfg;
  q('rtSum').textContent = `24 h ≥ ±${c.watch_min_pct}% · caída del ratio ${c.drop_min_pct}–${c.drop_max_pct}%`;
  q('rtN').textContent = r.rows.length;
  q('rtCurW').textContent = `(ahora ${c.watch_min_pct}%)`;
  q('rtCurMin').textContent = `(ahora ${c.drop_min_pct}%)`;
  q('rtCurMax').textContent = `(ahora ${c.drop_max_pct}%)`;
  q('rtInfo').innerHTML = `Ticker 24 h ${r.ticker_connected ? '<span class="pos">conectado</span>' : '<span class="neg">desconectado</span>'}`
    + ` (${r.ticker_symbols} símbolos, último mensaje ${r.ticker_age_s === null ? '—' : 'hace ' + dur(r.ticker_age_s)})`
    + ` · lista actualizada ${esc(r.watch_updated || '—')} · próxima consulta en ${r.next_cycle_in > 0 ? dur(r.next_cycle_in) : '—'}`
    + ` · consultas ${r.polls} · errores ${r.errors}`
    + (r.filters === 0 ? ` · <span class="neg">sin exchangeInfo (proxy/región): no se puede operar ni consultar el ratio</span>` : '')
    + (r.ticker_error && !r.ticker_connected ? ` · <span class="warn">ticker: ${esc(r.ticker_error)}</span>` : '')
    + (r.rest_blocked_s > 0 ? ` · <span class="neg">REST en pausa ${dur(r.rest_blocked_s)} (límite de peticiones)</span>` : '')
    + (r.last_error ? ` · <span class="warn">${esc(r.last_error)}</span>` : '');
  for (const [id, dir, on] of [['rtBtnShort', 'SHORT', r.short_enabled], ['rtBtnLong', 'LONG', r.long_enabled]]) {
    const b = q(id);
    b.dataset.on = on ? '1' : '';
    b.textContent = on ? `${dir} activo · desactivar` : `${dir} desactivado · activar`;
    b.className = on ? 'ok' : 'danger';
  }
  q('tRt').innerHTML = r.rows.map(x => `<tr>
    <td><b>${esc(x.symbol)}</b></td><td>${x.pct24 === null ? '—' : pct(x.pct24)}</td><td>${num(x.price, 8)}</td>
    <td>${num(x.prev, 4)}</td><td>${num(x.last, 4)}</td><td>${x.chg === null ? '<span class="muted">—</span>' : pct(x.chg)}</td>
    <td class="muted">${esc(x.data_time || '—')}</td><td>${x.side ? tag(x.side) : '<span class="muted">—</span>'}</td>
    <td>${x.has_position ? '<span class="st on">POSICIÓN</span>' : x.in_range ? '<span class="st off">EN RANGO</span>' : '<span class="muted">—</span>'}</td></tr>`).join('')
    || '<tr><td colspan="9" class="muted">Ningún símbolo cumple el % de 24 h todavía</td></tr>';
}

q('rtForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  q('rtMsg').textContent = 'Guardando…';
  try {
    const r = await post('/api/ratio', {watch_min_pct: q('rtWatch').value, drop_min_pct: q('rtMin').value, drop_max_pct: q('rtMax').value});
    q('rtMsg').innerHTML = r.ok
      ? `<span class="pos">Guardado.</span> 24 h ≥ ±${r.watch_min_pct}% · caída del ratio ${r.drop_min_pct}–${r.drop_max_pct}% (la lista de vigilados se recalcula en la próxima consulta de 5 min)`
      : `<span class="neg">${esc(r.error)}</span>`;
    if (r.ok) { q('rtWatch').value = ''; q('rtMin').value = ''; q('rtMax').value = ''; }
  } catch (e) { q('rtMsg').innerHTML = `<span class="neg">${esc(e)}</span>`; }
  refresh();
});

for (const [id, dir] of [['rtBtnShort', 'SHORT'], ['rtBtnLong', 'LONG']]) {
  q(id).addEventListener('click', async () => {
    const on = !!q(id).dataset.on;                       // activo ahora → se va a bloquear
    const msg = on ? `¿Desactivar ${dir} en el bot de ratio? No abrirá entradas nuevas de ese lado (las abiertas siguen hasta su TP).`
                   : `¿Activar ${dir} en el bot de ratio?` + (dir === 'LONG' ? ' Pasará a vigilar también los símbolos con 24 h ≤ −X % (perdedores).' : '');
    if (!confirm(msg)) return;
    try { const r = await post('/api/block', {engine: 'ratio', direction: dir, blocked: on}); if (!r.ok) alert(r.error); } catch (e) { alert(e); }
    refresh();
  });
}

refresh();
setInterval(refresh, __POLL_MS__);
</script>
</body>
</html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), threaded=True)

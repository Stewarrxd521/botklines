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
import re
import socket
import ssl
import statistics
import struct
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
# Proxy HTTP opcional para el REST de Binance (el ratio y, una vez cada varios días, exchangeInfo).
REST_PROXY_URL = os.getenv("REST_PROXY_URL", "").strip()   # http://usuario:clave@host:puerto (variable de entorno)
# Proxies SOLO para el REST de Binance (ratio de top traders): se usan, rotando, mientras la IP
# directa esté vetada (418/429). REST_PROXY_URLS (separadas por comas) reemplaza esta lista.
_DEFAULT_PROXIES = (
    "http://fixie:JoXk8xTakh7l345@ventoux.usefixie.com:80,"
    "http://fixie:cRuvdj10B4X67d1@ventoux.usefixie.com:80,"
    "http://fixie:i3vqqjb7isp0bWq@ventoux.usefixie.com:80,"
    "http://fixie:GHbIB5zTUESRi7O@ventoux.usefixie.com:80,"
    "http://fixie:MNNq7XkuOq55G52@ventoux.usefixie.com:80"
)
REST_PROXIES = list(dict.fromkeys(
    [u.strip() for u in os.getenv("REST_PROXY_URLS", _DEFAULT_PROXIES).split(",") if u.strip()]
    + ([REST_PROXY_URL] if REST_PROXY_URL else [])))


def proxy_label(url: str) -> str:
    """Nombre corto de una ruta para logs y la web, sin la clave."""
    if not url:
        return "IP directa"
    p = urlsplit(url)
    return f"proxy {p.username or ''}@{p.hostname or ''}#{REST_PROXIES.index(url) + 1 if url in REST_PROXIES else '?'}"
INITIAL_SYMBOLS = [s.strip().upper() for s in os.getenv("INITIAL_SYMBOLS", "").split(",") if s.strip()]
SYMBOLS_CACHE_FILE = os.getenv(
    "SYMBOLS_CACHE_FILE", os.path.join(tempfile.gettempdir(), "futures_symbols_cache.json"))
# exchangeInfo (filtros de cantidad) se pide cada 10 días como mucho; los nombres de los
# símbolos se descubren por WebSocket (ver SYMBOL_SCAN_HOURS).
SYMBOL_REFRESH_HOURS = float(os.getenv("SYMBOL_REFRESH_HOURS", "240"))
# exchangeInfo por PROXY (REST GET /fapi/v1/exchangeInfo, solo esa petición): la IP pública normal del servidor no
# la hace. Si los proxies fallan, se vuelve a la IP normal (API WebSocket, un intento) y, si tampoco, al archivo.
EXCHANGE_INFO_VIA_PROXY = os.getenv("EXCHANGE_INFO_VIA_PROXY", "true").lower() in ("1", "true", "yes")
EXCHANGE_INFO_DIRECT_FALLBACK = os.getenv("EXCHANGE_INFO_DIRECT_FALLBACK", "true").lower() in ("1", "true", "yes")
EXCHANGE_INFO_PROXY_TRIES = max(int(os.getenv("EXCHANGE_INFO_PROXY_TRIES", "2")), 1)   # proxies distintos por intento
# Tras un fallo no se vuelve a pedir en seguida: 60 s, 2 min, 4 min… hasta este tope (s). Evita el "cada rato".
EXCHANGE_INFO_MAX_BACKOFF_S = float(os.getenv("EXCHANGE_INFO_MAX_BACKOFF_S", "1800"))

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
RATIO_POLL_DELAY_S  = float(os.getenv("RATIO_POLL_DELAY_S", "180"))   # Binance publica el punto de 5 m de todos los pares ~3 min después: se consulta a :03/:08
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

# Bot de funding alto: abre en el lado que COBRA el funding justo antes del cobro y cierra justo después.
FUNDING_ENABLED       = os.getenv("FUNDING_ENABLED", "true").lower() in ("1", "true", "yes")
FUNDING_MIN_PCT       = abs(float(os.getenv("FUNDING_MIN_PCT", "0.3")))    # |funding| mínimo para entrar (%)
FUNDING_NOTIONAL      = float(os.getenv("FUNDING_NOTIONAL", "20"))         # USDT por entrada
FUNDING_OPEN_BEFORE_S = float(os.getenv("FUNDING_OPEN_BEFORE_S", "2"))     # abre N s antes del cobro
FUNDING_CLOSE_AFTER_S = float(os.getenv("FUNDING_CLOSE_AFTER_S", "1"))     # cierra N s después del cobro
# Consulta final del funding: SIEMPRE con anticipación (mínimo FUNDING_SCAN_MIN_S, 3 min), para tener los pares, el
# precio en vivo y el reloj listos mucho antes de abrir. Un valor guardado por debajo del mínimo se ignora.
FUNDING_SCAN_MIN_S = float(os.getenv("FUNDING_SCAN_MIN_S", "180"))
FUNDING_SCAN_BEFORE_S = max(float(os.getenv("FUNDING_SCAN_BEFORE_S", "180")), FUNDING_SCAN_MIN_S)
# Una consulta lee TODOS los pares: junta mensajes del stream hasta que dos seguidos no traen pares nuevos
# (mínimo FUNDING_COLLECT_MIN_S y como mucho FUNDING_COLLECT_MAX_S). Antes se tomaba solo el primer mensaje y,
# si llegaba incompleto, un par que cumplía no aparecía o desaparecía de la tabla.
FUNDING_COLLECT_MIN_S = float(os.getenv("FUNDING_COLLECT_MIN_S", "4"))
# REST: SOLO en la consulta final (3 min antes del cobro) y como mucho una vez cada FUNDING_REST_MIN_INTERVAL_S
# (≈ 1 por hora): /fapi/v1/premiumIndex, UNA petición, peso 10, ~40 KB con gzip, trae TODOS los pares de una vez.
# Se une a lo que traiga el WebSocket para que no falte ningún par; con esa lista se suscriben en vivo por
# WebSocket los que cumplen o están cerca. El arranque, la exploración horaria y la manual van SOLO por WebSocket.
FUNDING_REST_SNAPSHOT = os.getenv("FUNDING_REST_SNAPSHOT", "true").lower() in ("1", "true", "yes")
FUNDING_REST_MIN_INTERVAL_S = float(os.getenv("FUNDING_REST_MIN_INTERVAL_S", "3000"))
# La tabla de la web y la exploración horaria usan el último valor visto de cada par en los últimos
# FUNDING_CACHE_TTL_S: un par que un escaneo por WebSocket no trajo no desaparece de la lista.
FUNDING_CACHE_TTL_S = float(os.getenv("FUNDING_CACHE_TTL_S", "900"))
FUNDING_COLLECT_MAX_S = float(os.getenv("FUNDING_COLLECT_MAX_S", "12"))
# Seguimiento en vivo tras la consulta final: el funding de Binance es una ESTIMACIÓN que cambia hasta el cobro.
# Los pares que cobran en esa hora y están a ≥ FUNDING_TRACK_RATIO del umbral se siguen por su stream propio
# (<par>@markPrice@1s, pocos pares: poco ancho de banda) hasta FUNDING_TRACK_END_S antes de abrir. Si uno cruza
# el umbral entra; si uno que cumplía baja del umbral, no se abre.
FUNDING_TRACK = os.getenv("FUNDING_TRACK", "true").lower() in ("1", "true", "yes")
FUNDING_TRACK_RATIO = float(os.getenv("FUNDING_TRACK_RATIO", "0.5"))
FUNDING_TRACK_MAX = int(os.getenv("FUNDING_TRACK_MAX", "40"))
FUNDING_TRACK_END_S = float(os.getenv("FUNDING_TRACK_END_S", "0.5"))
FUNDING_TRACK_WS_BASES = [u.strip() for u in os.getenv(
    "FUNDING_TRACK_WS_BASES", "wss://fstream.binance.com/market/stream?streams=,"
                              "wss://fstream.binance.com/stream?streams=").split(",") if u.strip()]
# Pares nuevos con funding alto sin filtros de cantidad (la caché de exchangeInfo dura días): se pide
# exchangeInfo otra vez, como mucho una vez cada FUNDING_FILTERS_RETRY_S.
FUNDING_FILTERS_RETRY_S = float(os.getenv("FUNDING_FILTERS_RETRY_S", "1800"))
FUNDING_PERIOD_S      = 3600            # Binance cobra en horas en punto (cada 1, 4 u 8 h según el par)
# El funding se lee por WebSocket (!markPrice@arr: tasa "r", próximo cobro "T", mark "p" y hora "E" de
# todos los pares). Se abre antes del cobro, se juntan mensajes hasta tener todos los pares y se cierra:
# nada de REST. Se prueban en orden; la que funcione se recuerda.
FUNDING_WS_URLS = [u.strip() for u in os.getenv(
    "FUNDING_WS_URLS", "wss://fstream.binance.com/market/ws/!markPrice@arr@1s,"
                       "wss://fstream.binance.com/market/ws/!markPrice@arr,"
                       "wss://fstream.binance.com/ws/!markPrice@arr@1s").split(",") if u.strip()]
FUNDING_WS_TIMEOUT_S = float(os.getenv("FUNDING_WS_TIMEOUT_S", "5"))   # espera al primer mensaje por URL
# exchangeInfo (filtros de cantidad) por la API WebSocket de futuros (método "exchangeInfo"), sin REST.
API_WS_URLS = [u.strip() for u in os.getenv(
    "API_WS_URLS", "wss://ws-fapi.binance.com/ws-fapi/v1,wss://ws-fapi.binance.com/ws-fapi/v1?returnRateLimits=false"
).split(",") if u.strip()]
API_WS_TIMEOUT_S = float(os.getenv("API_WS_TIMEOUT_S", "20"))
# Respaldo: si el WebSocket no da exchangeInfo se usa este archivo del repositorio (JSON tal cual lo
# devuelve Binance, con o sin el envoltorio {"result": ...}). Se puede refrescar a mano cuando quieras.
# Reloj de Binance: se mide con el método "time" de la API WebSocket (ida y vuelta, varias muestras, se queda
# con las de menor latencia) FUNDING_CLOCK_SYNC_BEFORE_S segundos antes de cada cobro, en un hilo aparte.
FUNDING_CLOCK_SYNC_BEFORE_S = float(os.getenv("FUNDING_CLOCK_SYNC_BEFORE_S", "300"))   # sincronización completa: 5 min antes
# Además, el reloj se re-mide solo (rápido, solo Binance, ~1 KB) cada FUNDING_CLOCK_RESYNC_S aunque no haya
# cobro cerca, para que el ancla nunca envejezca (el reloj del servidor deriva unos ms por hora). 0 = apagado.
FUNDING_CLOCK_RESYNC_S = float(os.getenv("FUNDING_CLOCK_RESYNC_S", "600"))
# Segunda sincronización completa, 2 min antes del cobro, por si algo cambió desde la de 5 min. 0 = apagada.
FUNDING_CLOCK_RECHECK_BEFORE_S = float(os.getenv("FUNDING_CLOCK_RECHECK_BEFORE_S", "120"))
FUNDING_CLOCK_QUIET_S = 20.0       # la re-medición periódica no se hace tan cerca de la apertura
FUNDING_MIN_LEAD_S = float(os.getenv("FUNDING_MIN_LEAD_S", "0.4"))       # si al abrir faltan menos s, NO se entra (no alcanzaría el cobro)
FUNDING_OPEN_MIN_S = round(max(0.2, FUNDING_MIN_LEAD_S + 0.1), 2)      # mínimo de "abrir N s antes del cobro"
FUNDING_CLOCK_VERIFY_BEFORE_S = float(os.getenv("FUNDING_CLOCK_VERIFY_BEFORE_S", "8"))  # re-medición rápida (solo Binance)
# Proveedores externos de hora (referencia para comparar con Binance). NTP por UDP/123 y, si el hosting
# bloquea UDP, el /cdn-cgi/trace de Cloudflare (trae `ts=` con milisegundos) por HTTPS.
FUNDING_NTP_HOSTS = [h.strip() for h in os.getenv(
    "FUNDING_NTP_HOSTS", "time.cloudflare.com,time.google.com,pool.ntp.org").split(",") if h.strip()]
FUNDING_HTTP_TIME_URLS = [u.strip() for u in os.getenv(
    "FUNDING_HTTP_TIME_URLS", "https://www.cloudflare.com/cdn-cgi/trace").split(",") if u.strip()]
FUNDING_CLOCK_PROVIDER_SAMPLES = int(os.getenv("FUNDING_CLOCK_PROVIDER_SAMPLES", "5"))
FUNDING_CLOCK_TOLERANCE_S = float(os.getenv("FUNDING_CLOCK_TOLERANCE_S", "0.15"))     # Binance vs proveedores: aviso si difieren más
# Exploración horaria propia del bot de funding (independiente de la de los demás bots): lee TODOS los
# pares por WebSocket, como máximo FUNDING_HOURLY_MAX_SYMBOLS pares y FUNDING_HOURLY_MAX_S segundos, y deja
# la lista de los que ya cumplen el umbral (se les calienta el precio en vivo antes de la consulta final).
FUNDING_HOURLY_SCAN = os.getenv("FUNDING_HOURLY_SCAN", "true").lower() in ("1", "true", "yes")
FUNDING_HOURLY_BEFORE_S = float(os.getenv("FUNDING_HOURLY_BEFORE_S", "300"))      # cuánto antes de cada hora en punto
FUNDING_HOURLY_MAX_SYMBOLS = int(os.getenv("FUNDING_HOURLY_MAX_SYMBOLS", "3000"))   # tope de seguridad, no de diseño
FUNDING_HOURLY_MAX_S = float(os.getenv("FUNDING_HOURLY_MAX_S", "30"))
FUNDING_WATCH_PRICE_MAX = int(os.getenv("FUNDING_WATCH_PRICE_MAX", "10"))         # precios en vivo que se calientan
FUNDING_CLOCK_SAMPLES = int(os.getenv("FUNDING_CLOCK_SAMPLES", "12"))
FUNDING_CLOCK_MAX_RTT_S = float(os.getenv("FUNDING_CLOCK_MAX_RTT_S", "0.4"))   # muestras más lentas se descartan
EXCHANGE_INFO_FILE = os.getenv("EXCHANGE_INFO_FILE", os.path.join(_HERE, "exchangeInfo.txt"))
# Fuente del 24 h, siempre por WebSocket: "scan" = exploración de TICKER_SCAN_S por ciclo de
# 5 min (~10 % del tiempo conectado); "ws" = !miniTicker@arr abierto siempre (~80 KB/s).
TICKER_MODE         = os.getenv("TICKER_MODE", "scan").strip().lower()
TICKER_SCAN_S       = float(os.getenv("TICKER_SCAN_S", "30"))         # duración máxima de cada exploración
# Las exploraciones de cada ciclo se cortan cuando un mensaje ya casi no trae símbolos nuevos
# (2-3 mensajes, ~2-3 s, en vez de 30 s; los que no se movieron conservan su 24 h); la diaria, que rehace la lista de símbolos, dura TICKER_SCAN_S entero.
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
# El TP de cada apertura viaja en la señal ("tp") y el executor lo pone en Binance con la
# cantidad de esa posición: un TP por trade_id aunque varios bots compartan par (_open_tp).
SEND_TP = os.getenv("SEND_TP", "true").lower() in ("1", "true", "yes")
WATCH_LIVE_PRICE = os.getenv("WATCH_LIVE_PRICE", "true").lower() in ("1", "true", "yes")
# Demora mínima (s) entre la apertura y un cierre por precio, ratio o indicador (editable en la web):
# evita cerrar en el mismo tick de la apertura (las de 0 s salían con el precio del disparo, no el real).
MIN_HOLD_S = max(0.0, float(os.getenv("MIN_HOLD_S", "2")))
# Precio de apertura/cierre en papel como el executor: libro de 20 niveles (asks para comprar, bids
# para vender), medio ponderado de la cantidad; sin libro, mejor ask/bid; sin eso, el precio en vivo.
BOOK_FILL = os.getenv("BOOK_FILL", "true").lower() in ("1", "true", "yes")
# TP por precio: antes de cerrar se confirma con el libro que la salida llega al TP; si no, no cierra.
TP_BOOK_CHECK = os.getenv("TP_BOOK_CHECK", "true").lower() in ("1", "true", "yes")
BOOK_DEPTH = 20
BOOK_MAX_AGE_S = float(os.getenv("BOOK_MAX_AGE_S", "1.5"))     # libro reutilizable (los 3 bots de ratio)
BOOK_TIMEOUT_S = float(os.getenv("BOOK_TIMEOUT_S", "2"))
# El libro se toma por WebSocket (sin REST). Desde 2026-04 el libro va por /public; se prueban en orden.
BOOK_WS_URLS = [u.strip().rstrip("/") for u in os.getenv(
    "BOOK_WS_URLS", "wss://fstream.binance.com/public/ws,wss://fstream.binance.com/market/ws,"
                    "wss://fstream.binance.com/ws").split(",") if u.strip()]
# TP por RSI + VWAP ±kσ (UTC) de los bots de ratio, con velas 1m: REST una vez y WebSocket después.
IND_RSI_PERIOD = int(os.getenv("IND_RSI_PERIOD", "14"))
IND_RSI_LEVEL = float(os.getenv("IND_RSI_LEVEL", "13"))       # SHORT: RSI < 13 · LONG: RSI > 87
IND_VWAP_K = float(os.getenv("IND_VWAP_K", "2.5"))
# Velas del RSI/VWAP: se piden por REST a otros exchanges, en este orden (el primero que tenga el
# par); Binance solo como último recurso, para no gastar su REST (solo el ratio lo usa).
IND_EXCHANGES = [x.strip().lower() for x in os.getenv("IND_EXCHANGES", "bybit,kucoin,coinbase,mexc,binance").split(",")
                 if x.strip().lower() in ("bybit", "kucoin", "coinbase", "mexc", "binance")] or ["binance"]
ALT_URLS = {"bybit": os.getenv("BYBIT_URL", "https://api.bybit.com").rstrip("/"),
            "kucoin": os.getenv("KUCOIN_URL", "https://api-futures.kucoin.com").rstrip("/"),
            "coinbase": os.getenv("COINBASE_URL", "https://api.exchange.coinbase.com").rstrip("/"),
            "mexc": os.getenv("MEXC_URL", "https://contract.mexc.com").rstrip("/")}
ALT_MAX_ROWS = {"bybit": 1000, "kucoin": 500, "coinbase": 290, "mexc": 1000}     # velas por petición
EX_LABEL = {"bybit": "Bybit", "kucoin": "KuCoin", "coinbase": "Coinbase", "mexc": "MEXC", "binance": "Binance"}
# Velas de Binance (último recurso): en tramos de ≤499 (peso 2 cada uno; 1500 de golpe pesan 10).
BINANCE_KLINES_CHUNK = max(1, min(499, int(os.getenv("BINANCE_KLINES_CHUNK", "499"))))
# Peso usado por minuto que Binance devuelve en X-MBX-USED-WEIGHT-1M: al pasar este tope la ruta
# descansa hasta el minuto siguiente, antes de que llegue el 429 (y luego el 418 por insistir).
REST_WEIGHT_BUDGET = int(os.getenv("REST_WEIGHT_BUDGET", str(int(2400 * 0.8))))
# Veto de una ruta tras un 418/429: Binance manda en Retry-After el tiempo del baneo, que puede ser de horas o
# días. No se espera tanto: la ruta descansa REST_BAN_RETRY_CYCLES ciclos del ratio (5 min c/u) y luego se
# vuelve a probar con UNA sola petición. Si responde, se sigue por ella (la IP pública manda); si vuelve el
# 418, otro descanso igual. Mientras tanto el ratio sale por los proxies.
REST_BAN_RETRY_CYCLES = max(float(os.getenv("REST_BAN_RETRY_CYCLES", "2")), 0.2)
REST_BAN_MIN_S = float(os.getenv("REST_BAN_MIN_S", "30"))
REST_PROBE_HOLD_S = 20.0           # mientras la petición de prueba está en vuelo nadie más usa esa ruta


def price_stream_for(klines_on: bool, fast: bool = True) -> str:
    """fast=False (ahorro de datos): markPrice@1s, 1 msg/s por símbolo con posición."""
    return PRICE_STREAM_ENV or ("bookTicker" if klines_on else "aggTrade" if fast else "markPrice@1s")


def http_get_json(url: str, proxy: str = "", timeout: float = 15.0,
                  headers_out: Optional[dict] = None) -> Tuple[object, int]:
    """GET JSON pidiendo gzip (Binance lo admite: ~5-10× menos bytes). (datos, bytes recibidos).
    Con headers_out (dict) devuelve ahí las cabeceras de la respuesta, en minúsculas."""
    handlers = [urllib.request.ProxyHandler({"http": proxy, "https": proxy})] if proxy else []
    opener = urllib.request.build_opener(*handlers)
    req = urllib.request.Request(url, headers={"User-Agent": "BOTKLINE/1.0", "Accept-Encoding": "gzip"})
    with opener.open(req, timeout=timeout) as resp:
        raw = resp.read()
        if headers_out is not None:
            headers_out.update({k.lower(): v for k, v in resp.headers.items()})
        if (resp.headers.get("Content-Encoding") or "").lower() == "gzip":
            body = gzip.decompress(raw)
        else:
            body = raw
    return json.loads(body), len(raw)


def route_ban_s(retry_after, cap_s: float, min_s: float = 30.0) -> float:
    """Cuánto descansa una ruta tras un 418/429: lo que pide Binance (Retry-After, s), pero nunca más de cap_s
    (1-2 ciclos) ni menos de min_s. Retry-After ausente o raro → cap_s."""
    try:
        wait = float(retry_after)
    except (TypeError, ValueError):
        wait = cap_s
    if wait != wait or wait <= 0:          # NaN o ≤ 0
        wait = cap_s
    return max(min(wait, cap_s), min(min_s, cap_s))


def _fmt_wait(sec: float) -> str:
    """Duración legible: 45 s, 10 min, 3 h 20 min, 2 d 4 h."""
    sec = max(0.0, float(sec))
    if sec < 90:
        return f"{sec:.0f} s"
    m = int(round(sec / 60))
    if m < 60:
        return f"{m} min"
    h, m = divmod(m, 60)
    if h < 48:
        return f"{h} h {m} min" if m else f"{h} h"
    d, h = divmod(h, 24)
    return f"{d} d {h} h" if h else f"{d} d"


def ban_until_from_body(body: str) -> Optional[float]:
    """Binance explica el 418 en el cuerpo: "...IP banned until 1731370000000..." (ms). Devuelve s o None."""
    m = re.search(r"banned until (\d{12,14})", body or "")
    return int(m.group(1)) / 1000.0 if m else None


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


def funding_side(rate: float) -> Optional[str]:
    """Lado que COBRA el funding: con tasa positiva los LONG pagan a los SHORT, y al revés."""
    if rate > 0:
        return SHORT
    if rate < 0:
        return LONG
    return None


def funding_income_usd(notional: float, rate_pct: float, direction: str) -> float:
    """Funding cobrado (+) o pagado (−) por una posición de `notional` con la tasa `rate_pct` (%)."""
    return notional * rate_pct / 100.0 * (1.0 if direction == SHORT else -1.0)


def next_funding_ts(now: float, lead_s: float = 0.0, period_s: int = FUNDING_PERIOD_S) -> int:
    """Próximo cobro (hora en punto, segundos Unix) al que aún le quedan más de `lead_s` segundos."""
    t = (int(now) // period_s + 1) * period_s
    return t + period_s if t - now <= lead_s else t


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


def walk_book(levels, qty: float) -> Optional[float]:
    """Precio medio ponderado de una orden de mercado de `qty` contra `levels` [(precio, cantidad)]
    ordenados del mejor al peor (asks para comprar, bids para vender), como el executor: lo que
    pase de los niveles recibidos se valora al peor precio visto. None si no hay niveles válidos."""
    left, cost, last = qty, 0.0, 0.0
    for row in levels or ():
        try:
            px, q = float(row[0]), float(row[1])
        except (TypeError, ValueError, IndexError):
            continue
        if px <= 0 or q <= 0:
            continue
        if qty <= 0:
            return px
        take = min(left, q)
        cost, left, last = cost + take * px, left - take, px
        if left <= 1e-12:
            break
    if last <= 0 or qty <= 0:
        return None
    return round((cost + max(left, 0.0) * last) / qty, 10)


# ── Indicadores progresivos (velas 1m): RSI de Wilder y VWAP diario UTC con bandas σ ──

DAY_MS = 86_400_000


def klines_needed(now_ms: int, rsi_period: int) -> int:
    """Velas 1m a pedir por REST UNA sola vez para arrancar RSI y VWAP: las de hoy (desde las
    00:00 UTC, para el VWAP) o, si son menos, 10 × periodo (calentamiento del RSI de Wilder, que
    deja el valor a < 0,01 del de un histórico infinito), + la vela en curso (se descarta).
    Ej. 00:45 UTC con RSI 14 → 141; 12:00 → 721. Máximo de Binance: 1500."""
    today = int((now_ms % DAY_MS) // 60_000)
    return max(2, min(1500, max(today, 10 * max(1, rsi_period)) + 1))


class CandleIndicators:
    """RSI (Wilder) + VWAP anclado a las 00:00 UTC con su desviación típica, alimentados vela a vela
    cerrada (REST al arrancar, WebSocket después). Precio típico (h+l+c)/3 ponderado por volumen."""

    def __init__(self, rsi_period: int = 14) -> None:
        self.period = max(1, int(rsi_period))
        self.prev_close: Optional[float] = None
        self._seed: List[Tuple[float, float]] = []
        self.avg_gain: Optional[float] = None
        self.avg_loss: Optional[float] = None
        self.rsi: Optional[float] = None
        self.day = -1
        self.sv = self.svp = self.svp2 = 0.0
        self.day_candles = 0
        self.last_open = 0          # open_time (ms) de la última vela aplicada
        self.close = 0.0
        self.count = 0

    def add(self, open_time: int, high: float, low: float, close: float, volume: float) -> bool:
        if open_time <= self.last_open or close <= 0:
            return False                                    # repetida o desordenada
        p = self.period
        if self.prev_close is not None:
            ch = close - self.prev_close
            gain, loss = max(ch, 0.0), max(-ch, 0.0)
            if self.avg_gain is None:
                self._seed.append((gain, loss))
                if len(self._seed) == p:                     # primera media: simple
                    self.avg_gain = sum(g for g, _ in self._seed) / p
                    self.avg_loss = sum(lo for _, lo in self._seed) / p
                    self._seed = []
            else:
                self.avg_gain = (self.avg_gain * (p - 1) + gain) / p
                self.avg_loss = (self.avg_loss * (p - 1) + loss) / p
            if self.avg_gain is not None:
                if self.avg_loss == 0:
                    self.rsi = 100.0 if self.avg_gain > 0 else 50.0
                else:
                    self.rsi = 100.0 - 100.0 / (1.0 + self.avg_gain / self.avg_loss)
        self.prev_close = close
        day = open_time // DAY_MS
        if day != self.day:                                 # nuevo día UTC: el VWAP se reinicia
            self.day, self.sv, self.svp, self.svp2, self.day_candles = day, 0.0, 0.0, 0.0, 0
        tp = (high + low + close) / 3.0
        self.sv += volume
        self.svp += volume * tp
        self.svp2 += volume * tp * tp
        self.day_candles += 1
        self.last_open, self.close = open_time, close
        self.count += 1
        return True

    def vwap(self) -> Optional[Tuple[float, float]]:
        """(VWAP, σ) del día UTC en curso."""
        if self.sv <= 0:
            return None
        v = self.svp / self.sv
        return v, max(self.svp2 / self.sv - v * v, 0.0) ** 0.5

    def bands(self, k: float) -> Optional[Tuple[float, float, float]]:
        vw = self.vwap()
        if vw is None:
            return None
        v, sd = vw
        return v - k * sd, v, v + k * sd

    def ready(self) -> bool:
        return self.rsi is not None and self.sv > 0


def indicator_exit(ind: CandleIndicators, direction: str, cfg: dict) -> Optional[str]:
    """¿Toca el TP por RSI/VWAP? SHORT: RSI < nivel y cierre ≤ VWAP − kσ (sobreventa);
    LONG: RSI > 100 − nivel y cierre ≥ VWAP + kσ. Con uno solo de los dos activo, basta ese."""
    use_rsi, use_vwap = cfg.get("use_rsi", True), cfg.get("use_vwap", True)
    if not (use_rsi or use_vwap) or not ind.ready():
        return None
    lvl = float(cfg.get("rsi_level", 13))
    lo, _mid, hi = ind.bands(float(cfg.get("vwap_k", 2.5)))
    if direction == SHORT:
        ok_rsi, ok_vwap = ind.rsi < lvl, ind.close <= lo
    else:
        ok_rsi, ok_vwap = ind.rsi > 100.0 - lvl, ind.close >= hi
    if (ok_rsi or not use_rsi) and (ok_vwap or not use_vwap):
        parts = ([f"RSI {ind.rsi:.1f}"] if use_rsi else []) + \
                ([f"cierre {ind.close:g} {'≤' if direction == SHORT else '≥'} {lo if direction == SHORT else hi:g}"]
                 if use_vwap else [])
        return " · ".join(parts)
    return None


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
    ex_tp:         float = 0.0      # TP puesto en Binance por el executor (0 = ninguno)
    signal_price:  float = 0.0      # precio de la señal (entry_price es el del libro)
    entry_src:     str = ""         # de dónde salió entry_price: libro | mejor ask/bid | precio
    tp_rejects:    int = 0          # veces que el precio tocó el TP pero el libro no llegaba (no cerró)
    # ── Solo bot de funding ──
    funding_rate:  float = 0.0      # tasa de funding (%) vista al entrar
    funding_ts:    float = 0.0      # momento del cobro (s Unix, hora de Binance); 0 = no es de funding

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
ENGINE_FUNDING = "funding"         # cobra funding alto: entra justo antes del cobro y sale justo después
KLINE_ENGINES = (ENGINE_CLOSE, ENGINE_LIVE)
ENGINE_LABELS = {ENGINE_CLOSE: "Bot al cierre", ENGINE_LIVE: "Bot en vivo",
                 ENGINE_RATIO: "Bot ratio TP/SL", ENGINE_RATIO_TP: "Bot ratio solo TP ratio",
                 ENGINE_RATIO_REV: "Bot ratio + venganza", ENGINE_FUNDING: "Bot funding alto"}
_RATIO_ENTRY_HINT = ("SHORT cuando el ratio de los top traders CAE en un ganador (24 h ≥ +X %); LONG cuando SUBE en "
                     "un perdedor (24 h ≤ −X %)")
ENGINE_HINTS = {
    ENGINE_RATIO: _RATIO_ENTRY_HINT + ". Sale por TP/SL de precio o cuando el ratio recupera el % que se movió",
    ENGINE_RATIO_TP: _RATIO_ENTRY_HINT + ". Sale SOLO cuando el ratio recupera el % que se movió (sin TP/SL de precio)",
    ENGINE_RATIO_REV: _RATIO_ENTRY_HINT + ". TP/SL propios; si cierra en pérdida abre un trade de venganza para recuperarla",
    ENGINE_CLOSE: "entra cuando la vela 1m CIERRA con cuerpo ≥ umbral",
    ENGINE_LIVE:  "entra en cuanto la vela 1m EN CURSO supera el umbral, sin esperar al cierre",
    ENGINE_FUNDING: ("con funding ≥ ±X % abre en el lado que cobra (SHORT si es positivo, LONG si es negativo) "
                     "unos segundos antes del cobro y cierra justo después"),
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

def exchange_filters(data: dict) -> Dict[str, Dict[str, float]]:
    """Filtros de cantidad (stepSize, minQty, minNotional) de los perpetuos USDT en TRADING."""
    data = data.get("result", data) if isinstance(data, dict) else {}
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


def pick_clock_offset(samples: List[Tuple[float, float]], max_rtt: float = 0.4,
                      max_spread: float = 0.25) -> Optional[Tuple[float, float, float]]:
    """samples = [(rtt_s, offset_s)]. Devuelve (offset, rtt_mínimo, dispersión) o None si no es fiable.
    Una muestra lenta (red o event loop saturado) trae un error de hasta rtt/2: solo valen las rápidas."""
    good = sorted(x for x in samples if 0 < x[0] <= max_rtt)
    if len(good) < 3:
        return None
    best = good[0][0]
    keep = [o for r, o in good if r <= best * 1.5 + 0.02][:5]
    if len(keep) < 2:
        return None
    spread = max(keep) - min(keep)
    if spread > max_spread:
        return None
    return statistics.median(keep), best, spread


_NTP_EPOCH = 2208988800


def ntp_samples(host: str, n: int = 5, timeout: float = 1.0) -> List[Tuple[float, float]]:
    """Consulta NTP (UDP/123, sin dependencias). Devuelve [(rtt_s, hora_servidor − perf_counter)].
    rtt = tiempo de ida y vuelta menos el que el servidor tardó en responder (fórmula estándar de NTP)."""
    out: List[Tuple[float, float]] = []
    addr = socket.getaddrinfo(host, 123, socket.AF_INET, socket.SOCK_DGRAM)[0][4]
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(timeout)
        for _ in range(n):
            pkt = b"\x1b" + 47 * b"\0"                       # LI=0, VN=3, modo cliente
            p0 = time.perf_counter()
            try:
                sock.sendto(pkt, addr)
                data, _a = sock.recvfrom(512)
            except OSError:
                continue
            p3 = time.perf_counter()
            if len(data) < 48 or data[1] == 0 or (data[0] & 7) != 4:   # sin stratum / no es respuesta de servidor
                continue
            w = struct.unpack("!12I", data[:48])
            t2 = w[8] + w[9] / 2 ** 32 - _NTP_EPOCH                  # recibió
            t3 = w[10] + w[11] / 2 ** 32 - _NTP_EPOCH                # respondió
            rtt = (p3 - p0) - (t3 - t2)
            if rtt <= 0:
                continue
            out.append((rtt, (t2 + t3) / 2.0 - (p0 + p3) / 2.0))
            time.sleep(0.05)
    return out


def http_time_samples(url: str, n: int = 6, timeout: float = 2.0) -> List[Tuple[float, float]]:
    """Hora con milisegundos por HTTPS (campo `ts=` de Cloudflare /cdn-cgi/trace), sobre UNA conexión
    reutilizada (la primera petición, con TLS, se descarta). [(rtt_s, hora_servidor − perf_counter)]."""
    u = urlsplit(url)
    conn = http.client.HTTPSConnection(u.hostname, u.port or 443, timeout=timeout,
                                       context=ssl.create_default_context())
    out: List[Tuple[float, float]] = []
    try:
        for i in range(n + 1):
            p0 = time.perf_counter()
            conn.request("GET", (u.path or "/") + (f"?{u.query}" if u.query else ""),
                         headers={"Connection": "keep-alive", "Cache-Control": "no-cache"})
            resp = conn.getresponse()
            body = resp.read().decode("utf-8", "replace")
            p1 = time.perf_counter()
            if i == 0:
                continue
            srv = None
            for line in body.splitlines():
                if line.startswith("ts="):
                    srv = float(line[3:])
            if srv is not None and resp.status == 200:
                out.append((p1 - p0, srv - (p0 + p1) / 2.0))
            time.sleep(0.05)
    finally:
        conn.close()
    return out


def clock_compare(binance: Optional[Tuple[float, float, float]],
                  providers: Dict[str, Tuple[float, float, float]],
                  tolerance_s: float = 0.15) -> Tuple[Optional[float], str, dict]:
    """Elige el desfase (hora de Binance − perf_counter) con el que se opera y compara con los proveedores.
    binance / providers[nombre] = (desfase, rtt_min, dispersión) o None. Se opera con Binance, que es quien
    fija el instante del cobro; si Binance no midió bien, con la mediana de los proveedores (nunca con un
    valor viejo). Devuelve (desfase|None, fuente, info)."""
    ref = statistics.median([v[0] for v in providers.values()]) if providers else None
    info = {"providers": {k: {"off_ms": round(v[0] * 1000), "rtt_ms": round(v[1] * 1000)}
                          for k, v in providers.items()}}
    if binance is not None and ref is not None:
        diff = binance[0] - ref
        info["diff_ms"] = round(diff * 1000)
        info["agree"] = abs(diff) <= max(tolerance_s, binance[1] / 2 + 0.02)
    if binance is not None:
        return binance[0], "binance", info
    if ref is not None and len(providers) >= 2:
        return ref, "proveedores", info
    return None, "", info


class _WsRpc:
    """Conexión WebSocket mínima de petición/respuesta (websockets o, si no está, aiohttp)."""

    def __init__(self, url: str) -> None:
        self.url, self._ws, self._session, self._lib = url, None, None, ""

    async def __aenter__(self) -> "_WsRpc":
        try:
            import websockets                               # type: ignore
        except ImportError:
            websockets = None
        if websockets is not None:
            self._lib = "w"
            self._ws = await websockets.connect(self.url, ping_interval=20, ping_timeout=30, max_size=None)
        else:
            import aiohttp                                  # type: ignore
            self._lib = "a"
            self._session = aiohttp.ClientSession()
            try:
                self._ws = await self._session.ws_connect(self.url, heartbeat=20, max_msg_size=0)
            except BaseException:
                await self._session.close()             # sin __aexit__ si la conexión falla: no dejar la sesión abierta
                raise
        return self

    async def __aexit__(self, *exc) -> None:
        try:
            await self._ws.close()
        finally:
            if self._session is not None:
                await self._session.close()

    async def call(self, payload: str, timeout: float):
        if self._lib == "w":
            await self._ws.send(payload)
            return await asyncio.wait_for(self._ws.recv(), timeout)
        await self._ws.send_str(payload)
        return await asyncio.wait_for(self._ws.receive_str(), timeout)


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
        self._book_ws_ok = ""                       # última URL que dio el libro por WebSocket
        # Símbolos con precio pedido antes de una entrada → cuántos lo piden (ratio, funding, exploración
        # horaria). Con un contador, soltar el de uno no da de baja el precio que otro sigue esperando.
        self._price_hold: Dict[str, int] = {}
        self._bg_tasks: set = set()                 # tareas lanzadas en segundo plano (referencia fuerte)
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
        # Bot de funding alto: su motor y sus parámetros (editables en caliente desde la web).
        self.engines[ENGINE_FUNDING] = Engine(ENGINE_FUNDING, saved.get(ENGINE_FUNDING)
                                              or {"paused": not FUNDING_ENABLED}, 1.0, 1.0)
        self.funding_cfg = self._load_funding_cfg(settings.get("funding"))
        self._funding_rows: List[dict] = []           # última consulta: los de mayor |funding|
        self._funding_over = 0                        # cuántos pasaban el umbral en esa consulta
        self._funding_done_ts = 0                     # último cobro ya consultado y programado
        self._funding_target_ts = 0                   # cobro al que apunta ahora el bot
        self.funding_clock_off = 0.0                  # hora de Binance − hora local (s), solo informativo/respaldo
        self._perf_to_srv: Optional[float] = None     # hora de Binance − perf_counter(): ancla MONOTÓNICA (inmune a saltos del reloj del sistema)
        self.funding_clock_info: dict = {}            # última comparación Binance vs proveedores externos
        self._clock_verified_for = 0                  # cobro para el que ya se hizo la verificación rápida
        self._funding_hourly_for = 0                  # cobro para el que ya se hizo la exploración horaria
        self._scan_busy = False                       # hay una exploración completa en curso (horaria o manual)
        self._rows_retry_at = 0.0                     # próximo reintento si la tabla sigue vacía
        self._rescan_last = 0.0                       # última exploración pedida a mano (anti-spam)
        self._funding_watch: dict = {}                # resultado de la última exploración horaria
        self._watch_hold: set = set()                 # símbolos con precio calentado por esa exploración
        self.funding_clock_ts = 0.0                   # cuándo se midió por última vez (0 = nunca)
        self.funding_clock_rtt_ms = 0.0               # latencia mínima de esa medición
        self._clock_synced_for = 0                    # cobro para el que ya se sincronizó el reloj
        self._clock_next_try = 0.0
        self._clock_busy = False                      # hay una medición del reloj en curso
        self._clock_rechecked_for = 0                 # cobro para el que ya se hizo la sync de 2 min antes
        self.loop_lag_ms = 0.0                        # mayor retraso reciente del event loop
        self.funding_scans = 0
        self._funding_ws_ok = ""                      # última URL del stream de funding que respondió
        self._funding_track_ok = ""                   # última base del stream combinado que respondió
        self._funding_snap_msgs = 0                   # mensajes que hizo falta juntar en la última consulta
        self._funding_src: dict = {}                  # cuántos pares trajo cada fuente en la última consulta
        self._funding_rest_ts = 0.0                   # última vez que se pidió premiumIndex por REST (≤ 1 por hora)
        self._funding_seen: Dict[str, tuple] = {}     # símbolo → (cuándo, última fila vista)
        self._funding_live: Dict[str, dict] = {}      # último funding visto de los pares del cobro en curso
        self._funding_live_for = 0.0                  # cobro al que corresponde _funding_live
        self._funding_launched: set = set()           # pares ya programados para el cobro en curso
        self._filters_retry_ts = 0.0                  # último reintento de exchangeInfo por un par sin filtros
        self._filters_busy = False
        self._filters_from_file = False               # los filtros vienen del archivo de respaldo, no de Binance
        self.funding_errors = 0
        self.funding_last_error = ""
        self.funding_last_scan_ts = 0.0
        self.funding_last_scan_ms = 0.0
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
        self._route_block: Dict[str, float] = {}        # ruta REST ("" = IP directa, o el proxy) → bloqueada hasta
        self._route_ok: Dict[str, int] = {}
        self._route_errors: Dict[str, int] = {}
        self._route_weight: Dict[str, int] = {}    # último X-MBX-USED-WEIGHT-1M visto por ruta
        self._proxy_rr = 0
        self._route_lock = threading.Lock()
        self._route_probe: set = set()                 # rutas con el veto vencido que esperan su petición de prueba
        self._route_bans: Dict[str, int] = {}          # 418/429 recibidos por ruta desde el arranque
        self._route_ban_until: Dict[str, float] = {}   # hasta cuándo dice Binance que dura el baneo (informativo)
        self._rest_last_route: Optional[str] = None
        self._rest_times: Deque[float] = deque()      # peticiones de los últimos 5 min
        self._scan_seen: Optional[set] = None         # símbolos vistos en la exploración en curso
        self._filters_ts = 0.0                        # cuándo se obtuvieron los filtros de cantidad
        self._xi_next_try = 0.0                       # no pedir exchangeInfo a la red antes de esta hora
        self._xi_fail = 0                             # fallos seguidos pidiendo exchangeInfo (para el backoff)
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
        # Ahorro de datos (web): precio en vivo de los vigilados y precio rápido de las abiertas.
        self.watch_live = bool(settings.get("watch_live_price", WATCH_LIVE_PRICE))
        self.fast_prices = bool(settings.get("fast_prices", True))
        # Mandar el TP con cada apertura (el executor lo pone en Binance). Todos los bots.
        self.send_tp = bool(settings.get("send_tp", SEND_TP))
        try:
            self.min_hold_s = max(0.0, float(settings.get("min_hold_s", MIN_HOLD_S)))
        except (TypeError, ValueError):
            self.min_hold_s = MIN_HOLD_S
        self._hold_wakes: Dict[str, object] = {}       # símbolo → revisión programada al cumplir la demora
        # Libro de órdenes (REST, 20 niveles) para los precios en papel: símbolo → (ts, bids, asks, origen).
        self._books: Dict[str, Tuple[float, list, list, str]] = {}
        self._book_inflight: Dict[str, object] = {}
        self._tp_checking: set = set()                  # TPs por precio esperando el libro
        self._tp_reject_log: Dict[int, float] = {}
        self.book_fetches = 0
        self.book_errors = 0
        # TP por RSI + VWAP ±kσ (UTC) de los bots de ratio: parámetros comunes, encendido por bot.
        self.ind_cfg = self._load_ind_cfg(settings.get("indicators"))
        self._ind: Dict[str, CandleIndicators] = {}   # símbolo → indicadores al día (vela a vela)
        self._ind_buf: Dict[str, list] = {}           # velas del WS que llegan mientras se cargan las de REST
        self._ind_err: Dict[str, str] = {}
        self._ind_seed_ts: Dict[str, float] = {}
        self._ind_reset = False
        self._ind_seeding: set = set()
        self._ind_src: Dict[str, str] = {}            # símbolo → exchange de sus velas (binance | bybit)
        self.rx_alt_bytes = 0                          # REST a otros exchanges (velas)
        self._alt_sym: Dict[Tuple[str, str], str] = {}        # (exchange, par) → nombre allí
        self._alt_missing: Dict[Tuple[str, str], float] = {}  # (exchange, par) sin velas → cuándo se vio                # símbolos con carga REST programada o en curso
        self.ind_seeds = 0
        self.ind_hits = 0
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
            wanted = syms | self._price_hold.keys()
        pc = self.price_cache
        if (changed or force) and pc is not None:
            pc.update_symbols(sorted(wanted))
        if changed or force:
            self._sync_indicators()

    def _hold_price(self, syms) -> None:
        """Pide el precio en vivo de `syms` (se suma a las demás peticiones del mismo par)."""
        with self.lock:
            for s in syms:
                self._price_hold[s] = self._price_hold.get(s, 0) + 1

    def _release_price(self, syms) -> None:
        """Suelta UNA petición de precio de cada par de `syms`; se da de baja cuando nadie más lo pide."""
        with self.lock:
            for s in syms:
                n = self._price_hold.get(s, 0) - 1
                if n > 0:
                    self._price_hold[s] = n
                else:
                    self._price_hold.pop(s, None)

    def _spawn(self, coro):
        """Lanza una tarea en el loop guardando la referencia: asyncio solo guarda una referencia débil
        y una tarea sin referencia puede desaparecer a mitad (p. ej. la apertura de un funding)."""
        task = asyncio.ensure_future(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

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
                "klines_enabled": self.klines_enabled, "watch_live_price": self.watch_live,
                "fast_prices": self.fast_prices, "min_hold_s": self.min_hold_s, "indicators": self.ind_cfg,
                "funding": self.funding_cfg, "send_tp": self.send_tp}
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
                moved = list(eng.positions.values())
            for p in moved:
                self._sync_executor_tp(p)                 # el TP puesto en Binance, también
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
        want = price_stream_for(self.kline_stream is not None, self.fast_prices)
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
                   executor_paused: Optional[bool] = None, watch_live_price: Optional[bool] = None,
                   fast_prices: Optional[bool] = None, min_hold_s: Optional[float] = None,
                   send_tp: Optional[bool] = None) -> dict:
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
        if watch_live_price is not None and watch_live_price != self.watch_live:
            self.watch_live = watch_live_price
            self._sync_watch_prices()
            changes.append("precio en vivo de los vigilados " + ("ACTIVADO (cada 3 s)" if watch_live_price
                                                                 else "APAGADO (24 h cada 5 min)"))
        if fast_prices is not None and fast_prices != self.fast_prices:
            self.fast_prices = fast_prices
            self._set_price_stream()
            changes.append("precio de las abiertas " + ("rápido (aggTrade, ~100 ms)" if fast_prices
                                                        else "cada 1 s (markPrice@1s)"))
        if min_hold_s is not None and max(0.0, min_hold_s) != self.min_hold_s:
            changes.append(f"demora mínima antes de cerrar {self.min_hold_s:g} s → {max(0.0, min_hold_s):g} s")
            self.min_hold_s = max(0.0, min_hold_s)
        if send_tp is not None and send_tp != self.send_tp:
            self.send_tp = send_tp
            n = self._sync_all_executor_tp()
            changes.append(("TP con las aperturas ENCENDIDO" if send_tp else "TP con las aperturas APAGADO")
                           + (f" ({n} abiertas actualizadas en el executor)" if n else ""))
        if changes:
            self._save_settings()
            self.store.mark_dirty(True)
            self.log("[config] ambos bots: " + " | ".join(changes))
        return self.global_settings()

    def global_settings(self) -> dict:
        return {"inverted": self.inverted, "fee_pct": self.fee_pct, "klines_enabled": self.klines_enabled,
                "profit_target_usd": self.profit_target_usd, "max_open": self.max_open,
                "executor_paused": self.executor_paused, "watch_live_price": self.watch_live,
                "fast_prices": self.fast_prices, "min_hold_s": self.min_hold_s, "book_fill": BOOK_FILL,
                "send_tp": self.send_tp}

    def open_count(self) -> int:
        with self.lock:
            return sum(len(e.positions) for e in self.engines.values())

    def realized_total(self) -> float:
        with self.lock:
            return sum(e.total_realized_pnl for e in self.engines.values())

    def _net_pnl(self, p: Position, price: float) -> float:
        return net_pnl_usd(price, p.entry_price, p.qty, p.direction, self.fee_pct) + self._funding_paid(p)

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
        with self.lock:
            targets = [p for e in self.engines.values() for p in e.positions.values()]
        self.log(f"[objetivo] 🎯 realizado {realized:+.4f} + abierto {unreal:+.4f} = "
                 f"{realized + unreal:+.4f} USD ≥ {target:g}: cierro {len(targets)} posición(es)")
        left = [len(targets) + 1]

        def one_done() -> None:
            # La sesión se reinicia cuando TODOS los cierres están contabilizados (al precio del libro).
            left[0] -= 1
            if left[0] > 0:
                return
            try:
                self.target_hits += 1
                final = self.realized_total() - self.session_base
                self.last_target = f"{_utc(time.time(), '%m-%d %H:%M:%S')} · {final:+.4f} USD"
                self.reset_session(f"objetivo alcanzado con {final:+.4f} USD")
            finally:
                self._target_closing = False

        try:
            for pos in targets:
                price = self._price(pos.symbol) or pos.last_price or pos.entry_price
                if not self._close(pos.trade_id, price, "OBJETIVO", on_done=one_done):
                    one_done()
        finally:
            one_done()

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
            if eng.name not in KLINE_ENGINES or not self.klines_enabled:
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
        self.log(f"{ENGINE_LABELS[ENGINE_FUNDING]} | {self._funding_desc()}"
                 f"{' | PAUSADO' if self.engines[ENGINE_FUNDING].paused else ''}")
        self.log(f"Red: 24 h por {f'WebSocket, exploración de {TICKER_SCAN_S:g} s cada 5 min' if TICKER_MODE != 'ws' else 'WebSocket continuo'} · "
                 f"precio en vivo: con posición {price_stream_for(self.klines_enabled, self.fast_prices)}"
                 f"{', vigilados markPrice cada 3 s' if self.watch_live else ''}")
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
                                                     symbol_stream=price_stream_for(self.klines_enabled, self.fast_prices))
        self.price_cache.set_update_callback(self._on_price_event)
        self.price_cache.start()
        self._sync_indicators()                         # velas 1m de las recuperadas con TP RSI/VWAP
        ind_on = [ENGINE_LABELS[n] for n in RATIO_ENGINES if self.ind_cfg["enabled"].get(n)]
        ic = self.ind_cfg
        self.log(f"TP RSI/VWAP: {', '.join(ind_on) or 'apagado en los tres bots'} | RSI({ic['rsi_period']}) "
                 f"< {ic['rsi_level']:g} / > {100 - ic['rsi_level']:g} · VWAP ±{ic['vwap_k']:g}σ UTC | "
                 f"demora mínima {self.min_hold_s:g} s | precios en papel: {'libro 20 niveles' if BOOK_FILL else 'señal'}")

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
            self._supervised(self._funding_loop, "funding"),
            self._supervised(self._clock_keeper_loop, "reloj"),
            self._supervised(self._loop_lag_monitor, "lag del loop"),
            self._supervised(self._ind_alt_loop, "velas de otros exchanges"),
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

    async def _ws_exchange_info(self) -> dict:
        """exchangeInfo por la API WebSocket de futuros: se conecta, pide el método y se cierra."""
        try:
            import websockets                               # type: ignore
        except ImportError:
            websockets = None
        req = json.dumps({"id": f"xi-{int(time.time() * 1000)}", "method": "exchangeInfo", "params": {}})
        last_exc: Optional[Exception] = None
        for url in API_WS_URLS:
            try:
                if websockets is not None:
                    async with websockets.connect(url, ping_interval=20, ping_timeout=30, max_size=None) as ws:
                        await ws.send(req)
                        raw = await asyncio.wait_for(ws.recv(), API_WS_TIMEOUT_S)
                else:
                    import aiohttp                          # type: ignore
                    async with aiohttp.ClientSession() as session:
                        async with session.ws_connect(url, heartbeat=20, max_msg_size=0) as ws:
                            await ws.send_str(req)
                            raw = await asyncio.wait_for(ws.receive_str(), API_WS_TIMEOUT_S)
                msg = json.loads(raw)
                if not isinstance(msg, dict) or msg.get("status") != 200:
                    err = msg.get("error") if isinstance(msg, dict) else msg
                    raise RuntimeError(f"respuesta {msg.get('status') if isinstance(msg, dict) else '?'}: {err}")
                result = msg.get("result")
                if not isinstance(result, dict) or not result.get("symbols"):
                    raise RuntimeError("exchangeInfo sin símbolos")
                self.log(f"WS exchangeInfo ({len(raw) / 1024:.0f} KB) por {url.split('?')[0]}")
                return result
            except Exception as exc:
                last_exc = exc
        raise RuntimeError(f"exchangeInfo por WebSocket falló: {last_exc!r}")

    def _exchange_info_from_file(self, why: Exception) -> Dict[str, Dict[str, float]]:
        """Respaldo: exchangeInfo guardado en el repositorio (EXCHANGE_INFO_FILE)."""
        try:
            with open(EXCHANGE_INFO_FILE, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            out = exchange_filters(raw)
        except Exception as exc:
            raise RuntimeError(f"exchangeInfo: WebSocket falló ({why}) y el archivo "
                               f"{os.path.basename(EXCHANGE_INFO_FILE)} no sirve ({exc!r})")
        if not out:
            raise RuntimeError(f"exchangeInfo: WebSocket falló ({why}) y el archivo no trae perpetuos {QUOTE_ASSET}")
        data = raw.get("result", raw)
        when = _utc(float(data.get("serverTime") or 0) / 1000.0, "%Y-%m-%d") if data.get("serverTime") else "?"
        self.log(f"[símbolos] exchangeInfo por WebSocket falló ({why}); uso el archivo "
                 f"{os.path.basename(EXCHANGE_INFO_FILE)} (del {when}): {len(out)} perpetuos")
        return out

    def _exchange_info_via_proxy(self) -> dict:
        """exchangeInfo por REST a través de un proxy (rotando, saltando los vetados). Solo esta petición usa el
        proxy: el resto del bot sigue por la IP pública normal."""
        now = time.time()
        free = [r for r in REST_PROXIES if self._route_block.get(r, 0.0) <= now]
        if not free:
            raise RuntimeError("todos los proxies están en pausa")
        k = self._proxy_rr % len(free)
        self._proxy_rr += 1
        order = (free[k:] + free[:k])[:EXCHANGE_INFO_PROXY_TRIES]
        url = f"{BASE_URL}/fapi/v1/exchangeInfo"
        last_exc: Optional[Exception] = None
        for route in order:
            hdrs: dict = {}
            try:
                data, nbytes = http_get_json(url, route, 25.0, headers_out=hdrs)
            except urllib.error.HTTPError as exc:
                if exc.code in (418, 429):
                    self._ban_route(route, exc, "/fapi/v1/exchangeInfo")
                else:
                    self._route_fail_soft(route)
                last_exc = RuntimeError(f"HTTP {exc.code} por {proxy_label(route)}")
                continue
            except Exception as exc:
                last_exc = RuntimeError(f"{proxy_label(route)}: {exc}")
                self._route_fail_soft(route)
                continue
            if not isinstance(data, dict) or not data.get("symbols"):
                last_exc = RuntimeError(f"{proxy_label(route)}: respuesta sin símbolos")
                continue
            self.rx_rest_bytes += nbytes
            self._route_ok_hit(route)
            self._note_weight(route, hdrs.get("x-mbx-used-weight-1m"))
            self.log(f"exchangeInfo por {proxy_label(route)} ({nbytes / 1024:.0f} KB)")
            return data
        raise RuntimeError(f"exchangeInfo por proxy falló: {last_exc}")

    async def _fetch_exchange_info(self) -> Dict[str, Dict[str, float]]:
        """Orden: proxy (REST) → IP normal (WebSocket, 1 intento) → archivo del repositorio. Con backoff: tras un
        fallo no se toca la red otra vez hasta pasado 60 s, 2 min, 4 min… (tope EXCHANGE_INFO_MAX_BACKOFF_S)."""
        self._filters_from_file = False
        now = time.time()
        errors: List[str] = []
        if now < self._xi_next_try:
            errors.append(f"en espera {self._xi_next_try - now:.0f} s tras un fallo (sin tocar la red)")
        else:
            if EXCHANGE_INFO_VIA_PROXY and REST_PROXIES:
                try:
                    return self._exchange_filters_checked(await asyncio.to_thread(self._exchange_info_via_proxy))
                except Exception as exc:
                    errors.append(str(exc))
            if EXCHANGE_INFO_DIRECT_FALLBACK or not (EXCHANGE_INFO_VIA_PROXY and REST_PROXIES):
                try:
                    if errors:
                        self.log(f"[símbolos] {errors[-1]}: vuelvo a la IP normal (WebSocket)")
                    return self._exchange_filters_checked(await self._ws_exchange_info())
                except Exception as exc:
                    errors.append(str(exc))
            self._xi_fail += 1
            self._xi_next_try = time.time() + min(60.0 * 2 ** (self._xi_fail - 1), EXCHANGE_INFO_MAX_BACKOFF_S)
        out = self._exchange_info_from_file(RuntimeError(" | ".join(errors)))
        self._filters_from_file = True
        return out

    def _exchange_filters_checked(self, raw: dict) -> Dict[str, Dict[str, float]]:
        out = exchange_filters(raw)
        if not out:
            raise RuntimeError(f"exchangeInfo sin perpetuos {QUOTE_ASSET}")
        self._xi_fail, self._xi_next_try = 0, 0.0
        return out

    async def _init_symbols(self) -> None:
        """Filtros de cantidad (stepSize, mínimos) de exchangeInfo, cacheados en disco
        SYMBOL_REFRESH_HOURS (10 días). exchangeInfo sale por proxy; si falla, por la IP normal (WebSocket)."""
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
                self.filters = await self._fetch_exchange_info()
                if self._filters_from_file:
                    # Del archivo: no se guarda como caché fresca y el WebSocket se reintenta en 1 h (no cada minuto).
                    self._filters_ts = time.time() - max(SYMBOL_REFRESH_HOURS - 1, 0) * 3600
                else:
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

        # Sale ya al executor con el precio en vivo; la entrada en papel se corrige con el libro.
        px, src = self._fill_from_book(symbol, direction == LONG, qty, price)
        self._open_kline(eng, symbol, direction, candle, body, age, qty, price, px, src, inverted, t0, skip)

    def _open_kline(self, eng: Engine, symbol: str, direction: str, candle: Candle, body: float, age: float,
                    qty: float, signal_px: float, price: float, src: str, inverted: bool, t0: float, skip) -> None:
        eff = qty * price
        pos = Position(
            trade_id=self._next_trade_id(), symbol=symbol, direction=direction,
            entry_price=price, qty=qty, notional=eff,
            tp_price=exit_price(price, direction, eng.tp_pct, inverted),
            signal_pct=body, candle_open=candle.open, candle_close=candle.close,
            candle_time=candle.open_time, last_price=price,
            strategy=eng.name, candle_age_s=round(age, 2), inverted=inverted,
            sent=not self.executor_paused, signal_price=signal_px, entry_src=src,
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
                 f"({body:+.2f}% a los {age:.0f}s) | px={price:g}{f' ({src})' if src else ''} qty={qty:g} ({eff:.2f} USDT) | "
                 f"{kind} {pos.tp_price:g} | trade #{pos.trade_id}")
        if pos.sent:
            self.executor.send({
                "action":    "open",
                "trade_id":  pos.trade_id,
                "symbol":    symbol,
                "direction": direction,
                "price":     signal_px,
                "quantity":  qty,
                "notional":  round(eff, 4),
                "level":     round(body, 4),
                "strategy":  eng.name,
                **self._open_tp(pos),
            }, t0)
        eng._decide_ms.append(decide_ms)
        self._refine_entry(eng, pos)

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
        hits, wait = [], 0.0
        with self.lock:
            for eng in self.engines.values():
                for tid, pos in eng.positions.items():
                    if pos.symbol != symbol:
                        continue
                    pos.update(price)
                    if pos.strategy in RATIO_ENGINES:
                        # TP/SL fijos por precio (la salida por ratio se revisa en el ciclo de 5 min)
                        reason = price_exit(price, pos.direction, pos.tp_price, pos.sl_price)
                    elif exit_hit(price, pos.tp_price, pos.direction, pos.inverted):
                        reason = "SL" if pos.inverted else "TP"
                    else:
                        reason = None
                    if not reason:
                        continue
                    left = self._hold_left(pos)
                    if left > 0:                    # recién abierta: se revisa otra vez al cumplir la demora
                        wait = min(wait, left) if wait else left
                        continue
                    hits.append((tid, reason))
        if wait > 0:
            self._wake_after_hold(symbol, wait)
        if hits and self.store.can_act():
            for tid, reason in hits:
                if reason == "TP" and BOOK_FILL and TP_BOOK_CHECK:
                    self._close_tp_checked(tid, price)
                else:
                    self._close(tid, price, reason)

    def _close_tp_checked(self, trade_id: int, trigger: float) -> None:
        """TP por precio confirmado con el libro: en un mechazo el último precio puede tocar el TP
        sin que haya bids/asks a ese nivel. Antes de enviar el cierre se valora la salida con el
        libro (bids si cierra un LONG, asks si cierra un SHORT); si no llega al TP (igual o mejor),
        el cierre se rechaza y la posición sigue abierta hasta que el TP se cumpla de verdad."""
        with self.lock:
            _eng, pos = self._find(trade_id)
            if pos is None or trade_id in self._closing or trade_id in self._tp_checking:
                return
            self._tp_checking.add(trade_id)
            sym, direction, qty, tp = pos.symbol, pos.direction, pos.qty, pos.tp_price

        def decide(px: float, src: str) -> None:
            self._tp_checking.discard(trade_id)
            _e, p = self._find(trade_id)
            if p is None:
                return
            if exit_hit(px, tp, direction):
                self._close(trade_id, px, "TP")
                return
            p.tp_rejects += 1
            now = time.time()
            if now - self._tp_reject_log.get(trade_id, 0.0) >= 30:
                self._tp_reject_log[trade_id] = now
                got = favorable_move_pct(px, p.entry_price, direction)
                want = favorable_move_pct(tp, p.entry_price, direction)
                self.log(f"[TP] #{trade_id} {sym} {direction}: el precio tocó el TP ({trigger:g}) pero el libro "
                         f"da {px:g}{f' ({src})' if src else ''} = {got:+.2f}% < TP {want:+.2f}%: cierre rechazado, "
                         f"sigue abierta (rechazos: {p.tp_rejects})")

        try:
            self._with_fill(sym, direction == SHORT, qty, trigger, decide)
        except Exception:
            self._tp_checking.discard(trade_id)
            raise

    def _close(self, trade_id: int, price: float, reason: str, on_done=None) -> bool:
        """Cierra `trade_id`: la orden sale ya al executor (con `price`, el del disparo) y el cierre
        en papel se valora con el libro en ese instante, como lo hace el executor (_with_fill)."""
        with self.lock:
            eng, pos = self._find(trade_id)
            if pos is None or trade_id in self._closing:
                return False
            self._closing.add(trade_id)
            pos = eng.positions.pop(trade_id, None)
            if pos is None:
                self._closing.discard(trade_id)
                return False
            pos.update(price)
            symbol = pos.symbol
            # El executor suma en UNA posición las de todos los bots en el mismo par y
            # dirección: si queda otra enviada, se cierra solo la cantidad de esta.
            shared = pos.sent and any(p.sent and p.symbol == symbol and p.direction == pos.direction
                                      for e in self.engines.values() for p in e.positions.values())
        try:
            self._refresh_pos_syms()
            if pos.sent:                        # simulada: el executor nunca la abrió
                payload = {
                    "action":      "close",
                    "trade_id":    pos.trade_id,
                    "symbol":      symbol,
                    "direction":   pos.direction,
                    "reason":      reason,
                    "close_price": price,
                    "pnl":         round(self._net_pnl(pos, price), 6),
                    "strategy":    eng.name,
                }
                if shared:
                    payload["quantity"] = pos.qty   # cierre parcial; la última cierra todo lo real
                self.executor.send(payload)
        except Exception:
            with self.lock:
                self._closing.discard(trade_id)
            raise
        # Cerrar un LONG es vender (bids); cerrar un SHORT, comprar (asks).
        revenged = False
        if self._may_revenge(eng, pos, reason):
            # La venganza no espera la foto del libro (abrir un WebSocket nuevo tarda segundos): la
            # pérdida se estima ya con el libro reciente, el precio en vivo o el disparo, y sale al
            # executor ahora. El cierre en papel se corrige después con el libro.
            est, _src = self._fill_from_book(symbol, pos.direction == SHORT, pos.qty, price)
            est_pnl = net_pnl_usd(est, pos.entry_price, pos.qty, pos.direction, self.fee_pct)
            if est_pnl < 0:
                revenged = True
                self._open_revenge(eng, pos, est, -est_pnl, reason)
        self._with_fill(symbol, pos.direction == SHORT, pos.qty, price,
                        lambda px, src: self._finish_close(eng, pos, px, src, price, reason, on_done,
                                                           revenged))
        return True

    def _may_revenge(self, eng: Engine, pos: Position, reason: str) -> bool:
        """Este cierre puede abrir una venganza (si termina en pérdida)."""
        return (eng.name == ENGINE_RATIO_REV and not pos.revenge and bool(self.rev_cfg.get("revenge"))
                and reason not in REVENGE_SKIP_REASONS)

    def _finish_close(self, eng: Engine, pos: Position, price: float, src: str,
                      trigger: float, reason: str, on_done=None, revenged: bool = False) -> None:
        """Contabiliza el cierre en papel al precio del libro y, si toca (y no salió ya al
        cerrar), abre la venganza."""
        try:
            with self.lock:
                pos.update(price)
                gross = pnl_usd(price, pos.entry_price, pos.qty, pos.direction)
                fees = fees_usd(pos.entry_price, price, pos.qty, self.fee_pct)
                funding = self._funding_paid(pos)
                pnl = gross - fees + funding                # neto: fees de entrada y salida (+ funding cobrado)
                now = time.time()
                rec = {
                    **asdict(pos),
                    "close_price": price,
                    "close_src": src,
                    "trigger_price": trigger,
                    "closed_at_ts": now,
                    "duration_s": now - pos.opened_ts,
                    "move_pct": favorable_move_pct(price, pos.entry_price, pos.direction),
                    "pnl": pnl,
                    "pnl_gross": gross,
                    "fees": fees,
                    "fee_pct": self.fee_pct,
                    "funding_usd": funding,
                    "pnl_pct": pnl / pos.notional * 100.0 if pos.notional else 0.0,
                    "reason": reason,
                    "time_to_mfe_s": (pos.mfe_ts - pos.opened_ts) if pos.mfe_ts else 0.0,
                    "time_to_mae_s": (pos.mae_ts - pos.opened_ts) if pos.mae_ts else 0.0,
                }
                eng.total_realized_pnl += pnl
                eng.closed_trades.insert(0, rec)
                del eng.closed_trades[500:]
                if COOLDOWN_SECONDS > 0:
                    eng.cooldown[pos.symbol] = now + COOLDOWN_SECONDS
                # Misma sección crítica que contabiliza el cierre: el guardado lleva ambas cosas.
                self.store.append_trade(rec)
            self.store.mark_dirty(True)
            px_note = f" ({src}; disparo {trigger:g})" if src and price != trigger else ""
            self.log(f"[{eng.label}] CIERRA {pos.direction} {pos.symbol} ({reason}) | entrada {pos.entry_price:g} → "
                     f"{price:g}{px_note} ({rec['move_pct']:+.2f}%) | PnL neto {pnl:+.4f} USDT (fees {fees:.4f}"
                     f"{f' · funding {funding:+.4f}' if pos.funding_ts else ''}) | "
                     f"{rec['duration_s']:.1f} s | trade #{pos.trade_id}")
            if pnl < 0 and not revenged and self._may_revenge(eng, pos, reason):
                self._open_revenge(eng, pos, price, -pnl, reason)
        finally:
            with self.lock:
                self._closing.discard(pos.trade_id)
            self._tp_reject_log.pop(pos.trade_id, None)
            if on_done is not None:
                on_done()

    # ── Precio de ejecución en papel: libro de órdenes, como el executor ──────

    def _on_loop(self) -> bool:
        try:
            return self.loop is not None and asyncio.get_running_loop() is self.loop
        except RuntimeError:
            return False

    def _book_fresh(self, sym: str) -> bool:
        b = self._books.get(sym)
        return b is not None and time.time() - b[0] <= BOOK_MAX_AGE_S

    async def _ws_depth(self, sym: str) -> Optional[Tuple[list, list]]:
        """Foto del libro de 20 niveles por WebSocket (<par>@depth20@100ms: se conecta, toma el
        primer mensaje y se cierra). No usa REST ni consume peso de la IP en Binance."""
        stream = f"{sym.lower()}@depth20@100ms"
        last_exc: Optional[Exception] = None
        # Primero la URL que funcionó la última vez: las que no dan libro cuestan BOOK_TIMEOUT_S cada una.
        urls = sorted(BOOK_WS_URLS, key=lambda u: u != self._book_ws_ok)
        for base in urls:
            try:
                agen = _ws_messages(f"{base}/{stream}")
                try:
                    msg = await asyncio.wait_for(agen.__anext__(), BOOK_TIMEOUT_S)
                finally:
                    await agen.aclose()
                d = json.loads(msg)
                d = d.get("data", d) if isinstance(d, dict) else {}
                bids, asks = d.get("b") or d.get("bids") or [], d.get("a") or d.get("asks") or []
                if bids or asks:
                    self._book_ws_ok = base
                    return bids, asks
            except Exception as exc:
                last_exc = exc
        raise RuntimeError(f"sin libro por WebSocket: {last_exc!r}")

    async def _load_book(self, sym: str) -> None:
        self.book_fetches += 1
        try:
            bids, asks = await self._ws_depth(sym)
            self._books[sym] = (time.time(), bids, asks, "libro")
        except Exception as exc:
            self.book_errors += 1
            self.last_error = f"libro {sym}: {exc}"

    def _fetch_book(self, sym: str) -> None:
        """Versión bloqueante (fuera del loop del bot: pruebas, hilos)."""
        asyncio.run(self._load_book(sym))

    def _fill_from_book(self, sym: str, buy: bool, qty: float, fallback: float) -> Tuple[float, str]:
        """Precio de una orden de mercado de `qty`: el libro reciente (asks si compra, bids si
        vende); si no hay, el precio en vivo; si tampoco, `fallback` (precio de la señal)."""
        b = self._books.get(sym)
        if b is not None and time.time() - b[0] <= BOOK_MAX_AGE_S:
            px = walk_book(b[2] if buy else b[1], qty)
            if px:
                return px, b[3]
        live = self._price(sym)
        if live:
            return live, "precio en vivo"
        return fallback, "señal"

    def _with_fill(self, sym: str, buy: bool, qty: float, fallback: float, cont) -> None:
        """cont(precio, origen) con el precio de ejecución en papel. Si hay que pedir el libro y
        estamos en el loop del bot, se pide en segundo plano y cont corre al volver (no bloquea)."""
        if not BOOK_FILL:
            return cont(fallback, "")
        if self._book_fresh(sym):
            return cont(*self._fill_from_book(sym, buy, qty, fallback))
        if not self._on_loop():
            self._fetch_book(sym)
            return cont(*self._fill_from_book(sym, buy, qty, fallback))

        def done(_fut) -> None:
            try:
                cont(*self._fill_from_book(sym, buy, qty, fallback))
            except Exception as exc:
                self.last_error = f"{sym}: {exc!r}"
                self.log(f"[libro] error tras pedir el libro de {sym}: {exc!r}")

        fut = self._book_inflight.get(sym)              # una sola foto por par aunque cierren varias
        if fut is None:
            fut = asyncio.ensure_future(self._load_book(sym))
            self._book_inflight[sym] = fut
            fut.add_done_callback(lambda _f: self._book_inflight.pop(sym, None))
        fut.add_done_callback(done)

    def _refine_entry(self, eng: Engine, pos: Position) -> None:
        """La orden ya salió al executor con el precio en vivo; en papel se pide el libro en
        segundo plano y se corrige la entrada cuando llega (el TP/SL puesto no cambia)."""
        if not BOOK_FILL or pos.entry_src == "libro":
            return

        def fix(px: float, src: str) -> None:
            if src != "libro" or not px:
                return
            with self.lock:
                if eng.positions.get(pos.trade_id) is not pos:
                    return                              # ya cerró
                old = pos.entry_price
                pos.entry_price, pos.notional, pos.entry_src = px, pos.qty * px, src
            self.store.mark_dirty(True)
            if px != old:
                self.log(f"[libro] #{pos.trade_id} {pos.symbol} {pos.direction}: entrada en papel "
                         f"{old:g} → {px:g} (libro)")

        self._with_fill(pos.symbol, pos.direction == LONG, pos.qty, pos.entry_price, fix)

    # ── Demora mínima antes de cerrar ─────────────────────────────────────────

    def _hold_left(self, pos: Position) -> float:
        return pos.opened_ts + self.min_hold_s - time.time()

    def _wake_after_hold(self, sym: str, wait: float) -> None:
        """Vuelve a revisar las salidas por precio de `sym` al cumplirse la demora mínima."""
        loop = self.loop
        if loop is None or sym in self._hold_wakes or not self._on_loop():
            return

        def fire() -> None:
            self._hold_wakes.pop(sym, None)
            self._check_take_profit(sym)
            self._check_profit_target()

        self._hold_wakes[sym] = loop.call_later(max(wait, 0.0) + 0.02, fire)

    def _close_after_hold(self, trade_id: int, reason: str) -> None:
        """Cierre por una señal ya confirmada (ratio, RSI/VWAP): si la posición aún no cumple la
        demora mínima, se cierra en cuanto la cumpla, con el precio de ese momento."""
        _eng, pos = self._find(trade_id)
        if pos is None:
            return
        wait = self._hold_left(pos)
        if wait > 0 and self._on_loop():
            self.loop.call_later(wait + 0.02, self._close_after_hold, trade_id, reason)
            return
        if not self.store.can_act():
            return
        price = self._live_price(pos.symbol) or pos.last_price or pos.entry_price
        self._close(trade_id, price, reason)

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

    def _open_tp(self, pos: Position) -> dict:
        """Campo "tp" de la apertura. El executor pone un TP POR trade_id, con la cantidad de
        esa posición (su lote), aunque haya otros bots en el mismo par y dirección."""
        tp = 0.0 if pos.inverted else pos.tp_price          # invertida: tp_price es su SL
        if tp > 0 and self.send_tp:
            pos.ex_tp = tp
            return {"tp": tp}
        return {}

    def _sync_executor_tp(self, pos: Position) -> None:
        """Tras cambiar el TP de una abierta (o al recuperarla tras un reinicio): mueve su TP en
        Binance (open_tp de ese trade_id) o lo quita (close_tp) si ya no tiene TP por precio."""
        if not pos.sent:
            return
        tp = 0.0 if pos.inverted or not self.send_tp else pos.tp_price
        if tp == pos.ex_tp:
            return
        base = {"trade_id": pos.trade_id, "symbol": pos.symbol, "direction": pos.direction, "strategy": pos.strategy}
        if tp > 0:
            self.executor.send({"action": "open_tp", "trigger_price": tp, **base})
        elif pos.ex_tp > 0:
            self.executor.send({"action": "close_tp", **base})
        pos.ex_tp = tp

    def _sync_all_executor_tp(self) -> int:
        """Al encender/apagar el envío del TP: pone o quita el TP en Binance de las abiertas."""
        with self.lock:
            open_pos = [p for e in self.engines.values() for p in e.positions.values() if p.sent]
        n = 0
        for p in open_pos:
            before = p.ex_tp
            self._sync_executor_tp(p)
            n += p.ex_tp != before
        return n

    def _apply_ratio_exits(self, names: Tuple[str, ...] = RATIO_ENGINES) -> None:
        """Recalcula TP/SL por precio de las posiciones abiertas de los bots de ratio con la
        configuración vigente y las revisa ya (por si el nivel nuevo ya se tocó). Las de
        venganza conservan su TP/SL en USD."""
        syms, changed = set(), []
        with self.lock:
            for name in names:
                for p in self.engines[name].positions.values():
                    if not p.revenge:
                        p.tp_price, p.sl_price = self._ratio_exit_prices(p.entry_price, p.direction, name)
                        syms.add(p.symbol)
                        changed.append(p)
        for p in changed:
            self._sync_executor_tp(p)
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
        ref = self._price(sym) or price
        f = self.filters.get(sym, {})
        qty, _eff, why = size_order(
            lost.notional * c["rev_mult"], ref, f.get("stepSize", 0.001), f.get("minQty", 0.0),
            f.get("minNotional", 5.0), MAX_NOTIONAL_OVER)
        if qty <= 0:
            return note(f"venganza saltada: {why}")
        # Sale YA al executor: sin esperar una foto nueva del libro. En papel entra con el libro
        # si está reciente; si no, con el precio en vivo, y se corrige cuando llega el libro.
        px, src = self._fill_from_book(sym, side == LONG, qty, ref)
        self._open_revenge_at(eng, lost, side, qty, ref, px, src, price, loss, reason, note)

    def _open_revenge_at(self, eng: Engine, lost: Position, side: str, qty: float, ref: float, px: float,
                         src: str, price: float, loss: float, reason: str, note) -> None:
        c = self.rev_cfg
        sym = lost.symbol
        now = time.time()
        eff = qty * px
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
            signal_price=ref, entry_src=src,
        )
        with self.lock:
            eng.positions[pos.trade_id] = pos
            eng.opened_count += 1
            self.revenge_count += 1
        self._refresh_pos_syms()
        self.store.mark_dirty(True)
        note(f"venganza abierta #{pos.trade_id}")
        self.log(f"[{eng.label}] VENGANZA {side} {sym}{'' if pos.sent else ' [SIMULADA]'} tras #{lost.trade_id} "
                 f"({reason}, {-loss:+.4f} USDT) | px={px:g}{f' ({src})' if src else ''} qty={qty:g} ({eff:.2f} USDT, ×{c['rev_mult']:g}) | "
                 f"TP {tp_px:g} (+{tp_usd:.4f} USD) · SL {sl_px:g} (-{sl_usd:.4f} USD) | trade #{pos.trade_id}")
        if pos.sent:
            self.executor.send({
                "action": "open", "trade_id": pos.trade_id, "symbol": sym, "direction": side,
                "price": ref, "quantity": qty, "notional": round(eff, 4),
                "level": round(-loss, 4), "strategy": eng.name, **self._open_tp(pos),
            }, time.perf_counter())
        self._refine_entry(eng, pos)

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
                    if quick and (msgs >= 3 or (msgs >= 2 and added <= max(3, before // 100))):
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
                self.log("[WS] reintentando exchangeInfo…")
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
            pc.set_slow_symbols(self.watch if self.watch_live else [])

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

    def _rest_routes(self) -> List[str]:
        """Orden de rutas para el REST de Binance: la IP directa mientras no esté vetada; si lo
        está, las proxies rotando (una distinta en cada petición) y saltando las vetadas.
        Una ruta cuyo veto acaba de vencer va "a prueba": la reserva UNA sola petición (las demás,
        concurrentes, no la ven) hasta saber si Binance ya la deja pasar."""
        with self._route_lock:
            now = time.time()
            free = [r for r in [""] + REST_PROXIES if self._route_block.get(r, 0.0) <= now]
            for r in free:
                if r in self._route_probe:                    # reserva la prueba para esta petición
                    self._route_block[r] = now + REST_PROBE_HOLD_S
            if "" in free:
                return [""] + [r for r in free if r]
            if not free:
                return []
            k = self._proxy_rr % len(free)
            self._proxy_rr += 1
            return free[k:] + free[:k]

    def _ban_route(self, route: str, exc: urllib.error.HTTPError, path: str) -> float:
        """418/429 por esa ruta: descanso de 1-2 ciclos (REST_BAN_RETRY_CYCLES), no el baneo entero que
        anuncia Binance; luego queda a prueba (ver _rest_routes). Devuelve los segundos de descanso."""
        try:
            body = exc.read().decode("utf-8", "replace")[:300]
        except Exception:
            body = ""
        retry_after = exc.headers.get("Retry-After") if exc.headers else None
        cap = REST_BAN_RETRY_CYCLES * RATIO_REFRESH_S
        rest = route_ban_s(retry_after, cap, REST_BAN_MIN_S)
        until_bin = ban_until_from_body(body)
        try:
            ra = float(retry_after) if retry_after is not None else None
        except (TypeError, ValueError):
            ra = None
        if until_bin is None and ra is not None:
            until_bin = time.time() + ra
        with self._route_lock:
            now = time.time()
            self._route_block[route] = now + rest
            self._route_probe.add(route)
            self._route_bans[route] = self._route_bans.get(route, 0) + 1
            if until_bin:
                self._route_ban_until[route] = until_bin
            routes = [""] + REST_PROXIES
            left = [r for r in routes if self._route_block.get(r, 0.0) <= now]
            self._rest_block_until = 0.0 if left else min(self._route_block[r] for r in routes)
        weight = self._route_weight.get(route)
        binance_txt = (f"Binance pide {_fmt_wait(until_bin - time.time())} (hasta {_utc(until_bin, '%H:%M')} UTC)"
                       if until_bin else "Binance no dice hasta cuándo")
        self.log(f"[REST] Binance respondió {exc.code} por {proxy_label(route)} en {path}"
                 f"{f' (peso visto antes: {weight}/min)' if weight is not None else ''}: {binance_txt}; "
                 f"la vuelvo a probar en {_fmt_wait(rest)} con 1 petición"
                 f"{f' · mientras tanto {len(left)} ruta(s) libres' if left else ' · sin rutas libres'}"
                 + (f" · {body.strip()[:160]}" if body.strip() else ""))
        return rest

    def _route_ok_hit(self, route: str) -> None:
        """Respuesta correcta por esa ruta: si estaba a prueba, queda rehabilitada."""
        with self._route_lock:
            self._route_ok[route] = self._route_ok.get(route, 0) + 1
            was_probe = route in self._route_probe
            if was_probe:
                self._route_probe.discard(route)
                self._route_ban_until.pop(route, None)
                if self._route_block.get(route, 0.0) > time.time():
                    self._route_block[route] = 0.0
        if was_probe:
            self.log(f"[REST] ✓ {proxy_label(route)} vuelve a responder: el ratio sigue por ahí")

    def _route_fail_soft(self, route: str) -> None:
        """Fallo de red (no 418/429): si era la petición de prueba, suelta la reserva para reintentar luego."""
        with self._route_lock:
            self._route_errors[route] = self._route_errors.get(route, 0) + 1
            if route in self._route_probe and self._route_block.get(route, 0.0) > time.time():
                self._route_block[route] = time.time() + 5.0

    def _rest_get(self, path: str, params: Optional[dict] = None, timeout: float = 15.0):
        """GET público a Binance (gzip). Un 429/418 veta SOLO la ruta que lo recibió (es la IP la que
        queda bloqueada) durante 1-2 ciclos y se sigue por la siguiente; con todas vetadas no sale
        ninguna petición hasta que venza el primer veto (como mucho REST_BAN_RETRY_CYCLES ciclos)."""
        order = self._rest_routes()
        if not order:
            raise RuntimeError(f"REST de Binance en pausa {self._rest_block_until - time.time():.0f} s (418/429 en todas las rutas)")
        url = f"{BASE_URL}{path}" + (f"?{urlencode(params)}" if params else "")
        last_exc: Optional[Exception] = None
        for route in order:
            hdrs: dict = {}
            try:
                data, nbytes = http_get_json(url, route, timeout, headers_out=hdrs)
            except urllib.error.HTTPError as exc:
                if exc.code not in (418, 429):
                    self._route_fail_soft(route)
                    raise RuntimeError(f"HTTP {exc.code}")
                self._ban_route(route, exc, path)
                last_exc = RuntimeError(f"HTTP {exc.code}")
                continue
            except Exception as exc:
                last_exc = exc
                self._route_fail_soft(route)
                continue
            self.rx_rest_bytes += nbytes
            self._route_ok_hit(route)
            self._note_weight(route, hdrs.get("x-mbx-used-weight-1m"))
            if route != self._rest_last_route:
                if route or self._rest_last_route is not None:
                    self.log(f"[REST] Binance ahora por {proxy_label(route)}")
                self._rest_last_route = route
            return data
        if isinstance(last_exc, RuntimeError) and str(last_exc).startswith("HTTP"):
            raise last_exc
        raise RuntimeError(f"sin respuesta: {last_exc}")

    def _note_weight(self, route: str, used) -> None:
        """Peso del minuto que Binance cuenta para esa IP. Al pasar REST_WEIGHT_BUDGET la ruta no se usa
        hasta el minuto siguiente (el contador se reinicia al cambiar de minuto)."""
        try:
            used = int(used)
        except (TypeError, ValueError):
            return
        self._route_weight[route] = used
        if used < REST_WEIGHT_BUDGET:
            return
        now = time.time()
        until = (int(now) // 60 + 1) * 60 + 1.0
        if self._route_block.get(route, 0.0) < until:
            self._route_block[route] = until
            self.log(f"[REST] Binance: {proxy_label(route)} lleva peso {used}/min (tope {REST_WEIGHT_BUDGET}); "
                     f"descansa {until - now:.0f} s para no llegar al 429")
            routes = [""] + REST_PROXIES
            if not [r for r in routes if self._route_block.get(r, 0.0) <= now]:
                self._rest_block_until = min(self._route_block[r] for r in routes)

    def rest_routes_snapshot(self) -> List[dict]:
        now = time.time()
        return [{"route": proxy_label(r), "blocked_s": max(0.0, self._route_block.get(r, 0.0) - now),
                 "probe": r in self._route_probe, "bans": self._route_bans.get(r, 0),
                 "binance_ban_s": max(0.0, self._route_ban_until.get(r, 0.0) - now),
                 "ok": self._route_ok.get(r, 0), "errors": self._route_errors.get(r, 0),
                 "weight": self._route_weight.get(r),
                 "current": r == self._rest_last_route} for r in [""] + REST_PROXIES]

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
                    self._hold_price((sym,))
                    # Precio en vivo y libro a la vez: los 3 bots abren con el mismo libro, sin esperar REST.
                    await asyncio.gather(self._prefetch_price(sym), self._prefetch_book(sym))
                fresh[sym] = self._on_ratio_points(sym, pts)
            except Exception as exc:
                self.ratio_last_error = f"{sym}: {exc!r}"
            finally:
                if hold:
                    self._release_price((sym,))

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

    async def _prefetch_book(self, sym: str) -> None:
        if BOOK_FILL and not self._book_fresh(sym):
            await self._load_book(sym)

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
            self.log(f"[ratio] TP {p.direction} {sym} ({ENGINE_LABELS[p.strategy]}): ratio {best:g} alcanzó el objetivo "
                     f"{goal:g} (entrada {p.ratio_entry:g}, recupera {abs(p.ratio_change_pct):.1f}%)")
            self._close_after_hold(p.trade_id, "TP_RATIO")

    # ── TP por RSI + VWAP ±kσ (velas 1m: REST una vez, luego WebSocket) ──────

    @staticmethod
    def _load_ind_cfg(raw) -> dict:
        raw = raw if isinstance(raw, dict) else {}
        en = raw.get("enabled") if isinstance(raw.get("enabled"), dict) else {}

        def num(key, default, lo, hi, cast=float):
            try:
                v = cast(raw.get(key, default))
            except (TypeError, ValueError):
                return default
            return v if lo <= v <= hi else default

        cfg = {"enabled": {n: bool(en.get(n, False)) for n in RATIO_ENGINES},
               "rsi_period": num("rsi_period", IND_RSI_PERIOD, 2, 100, int),
               "rsi_level": num("rsi_level", IND_RSI_LEVEL, 0.1, 49.9),
               "vwap_k": num("vwap_k", IND_VWAP_K, 0.1, 10.0),
               "use_rsi": bool(raw.get("use_rsi", True)),
               "use_vwap": bool(raw.get("use_vwap", True))}
        if not (cfg["use_rsi"] or cfg["use_vwap"]):
            cfg["use_rsi"] = cfg["use_vwap"] = True
        return cfg

    def set_ind_config(self, enabled: Optional[Dict[str, bool]] = None, rsi_period: Optional[int] = None,
                       rsi_level: Optional[float] = None, vwap_k: Optional[float] = None,
                       use_rsi: Optional[bool] = None, use_vwap: Optional[bool] = None) -> dict:
        """En caliente: encender/apagar el TP por RSI/VWAP de cada bot de ratio y sus parámetros
        (comunes a los tres). Cambiar el periodo del RSI recarga las velas (1 REST por símbolo)."""
        old = self.ind_cfg
        new = {**old, "enabled": dict(old["enabled"])}
        for name, on in (enabled or {}).items():
            if name not in RATIO_ENGINES:
                raise ValueError(f"bot desconocido: {name}")
            new["enabled"][name] = bool(on)
        if rsi_period is not None:
            if not 2 <= int(rsi_period) <= 100:
                raise ValueError("el periodo del RSI va de 2 a 100")
            new["rsi_period"] = int(rsi_period)
        if rsi_level is not None:
            if not 0 < float(rsi_level) < 50:
                raise ValueError("el nivel del RSI va de 0 a 50 (SHORT: RSI < nivel · LONG: RSI > 100 − nivel)")
            new["rsi_level"] = float(rsi_level)
        if vwap_k is not None:
            if not 0 < float(vwap_k) <= 10:
                raise ValueError("las bandas del VWAP van de 0 a 10 σ")
            new["vwap_k"] = float(vwap_k)
        if use_rsi is not None:
            new["use_rsi"] = bool(use_rsi)
        if use_vwap is not None:
            new["use_vwap"] = bool(use_vwap)
        if not (new["use_rsi"] or new["use_vwap"]):
            raise ValueError("activa al menos el RSI o el VWAP")
        if new == old:
            return self.ind_snapshot()
        changes = [f"{ENGINE_LABELS[n]} {'ON' if new['enabled'][n] else 'OFF'}"
                   for n in RATIO_ENGINES if new["enabled"][n] != old["enabled"][n]]
        for key, label in (("rsi_period", "RSI periodos"), ("rsi_level", "nivel RSI"), ("vwap_k", "VWAP ±σ")):
            if new[key] != old[key]:
                changes.append(f"{label} {old[key]:g} → {new[key]:g}")
        for key, label in (("use_rsi", "RSI"), ("use_vwap", "VWAP")):
            if new[key] != old[key]:
                changes.append(f"{label} {'usado' if new[key] else 'ignorado'}")
        self.ind_cfg = new
        if new["rsi_period"] != old["rsi_period"]:
            self._ind_reset = True                     # otro periodo: se recargan las velas
        self._save_settings()
        self.log("[config] TP RSI/VWAP: " + " | ".join(changes))
        self._sync_indicators()
        return self.ind_snapshot()

    def _ind_symbols(self) -> set:
        names = [n for n in RATIO_ENGINES if self.ind_cfg["enabled"].get(n)]
        with self.lock:
            return {p.symbol for n in names for p in self.engines[n].positions.values() if not p.revenge}

    def _sync_indicators(self) -> None:
        """Velas 1m por WebSocket SOLO de los símbolos con posición abierta (no de venganza) en un
        bot con el TP por RSI/VWAP encendido. Corre en el loop del bot."""
        loop = self.loop
        if loop is not None and not self._on_loop():
            loop.call_soon_threadsafe(self._sync_indicators)
            return
        want = self._ind_symbols()
        if self._ind_reset:
            self._ind_reset = False
            self._ind.clear()
            self._ind_buf.clear()
            self._ind_src.clear()
        for sym in set(self._ind) | set(self._ind_buf):
            if sym not in want:
                self._ind.pop(sym, None)
                self._ind_buf.pop(sym, None)
                self._ind_err.pop(sym, None)
                self._ind_src.pop(sym, None)
        pc = self.price_cache
        if pc is not None:                             # kline del WS de Binance: solo los cargados de Binance
            pc.set_kline_symbols({x for x in want if self._ind_src.get(x) == "binance"}, self._on_ws_kline)
        for sym in sorted(want - set(self._ind)):
            self._ind_buf.setdefault(sym, [])
            if sym not in self._ind_seeding:          # p. ej. recuperada antes de arrancar el loop
                self._schedule_seed(sym)

    def _schedule_seed(self, sym: str, delay: float = 0.0) -> None:
        loop = self.loop
        if loop is None or not self.running:
            return                                     # sin loop (pruebas): se llama a _ind_seeded a mano
        self._ind_seeding.add(sym)

        def go() -> None:
            if sym in self._ind_buf:
                self._bg_tasks.add(t := loop.create_task(self._ind_seed(sym)))
                t.add_done_callback(self._bg_tasks.discard)
            else:
                self._ind_seeding.discard(sym)

        loop.call_later(delay, go) if delay > 0 else go()

    def _fetch_seed_klines(self, sym: str) -> Tuple[list, str]:
        """(velas [open_time, o, h, l, c, v, close_time] de la más vieja a la más nueva, exchange).
        Prueba los exchanges de IND_EXCHANGES en orden; Binance (REST) es el último recurso."""
        n = klines_needed(int(time.time() * 1000), self.ind_cfg["rsi_period"])
        errors = []
        for ex in IND_EXCHANGES:
            try:
                if ex == "binance":
                    return self._binance_klines(sym, n), "binance"
                return self._alt_klines(ex, sym, n), ex
            except Exception as exc:
                errors.append(f"{EX_LABEL[ex]}: {exc}")
        raise RuntimeError(" · ".join(errors))

    def _binance_klines(self, sym: str, n: int) -> list:
        """Las últimas `n` velas 1m de Binance en tramos de ≤BINANCE_KLINES_CHUNK con startTime/endTime
        (peso 2 cada uno en vez de 10 por 1500 de golpe), del más viejo al más nuevo."""
        now_min = int(time.time() * 1000) // 60_000 * 60_000
        t, rows = now_min - (n - 1) * 60_000, {}
        while t <= now_min:
            end = min(now_min, t + (BINANCE_KLINES_CHUNK - 1) * 60_000)
            self._rest_times.append(time.time())
            data = self._rest_get("/fapi/v1/klines", {"symbol": sym, "interval": "1m", "startTime": t,
                                                      "endTime": end + 59_999, "limit": BINANCE_KLINES_CHUNK}, 10.0)
            if not isinstance(data, list):
                raise RuntimeError("respuesta de velas inesperada")
            for r in data:
                rows[int(r[0])] = r
            t = end + 60_000
        if not rows:
            raise RuntimeError("respuesta de velas vacía")
        return [rows[k] for k in sorted(rows)]

    # ── Velas de otros exchanges (Bybit, KuCoin futuros, Coinbase, MEXC futuros) ──

    @staticmethod
    def _alt_candidates(ex: str, sym: str) -> List[str]:
        """Nombres posibles del par en cada exchange. Un "1000PEPE" se prueba también sin el
        prefijo: la escala del precio no importa (RSI y VWAP se comparan con velas del mismo sitio)."""
        base = sym[:-4] if sym.endswith("USDT") else sym
        bases = [base]
        for pre in ("1000000", "10000", "1000"):
            if base.startswith(pre) and len(base) > len(pre):
                bases.append(base[len(pre):])
                break
        if ex == "bybit":
            return [b + "USDT" for b in bases]
        if ex == "kucoin":
            return [("XBT" if b == "BTC" else b) + "USDTM" for b in bases]
        if ex == "mexc":
            return [b + "_USDT" for b in bases]
        return [f"{b}-USD" for b in bases] + [f"{b}-USDT" for b in bases]

    def _alt_http(self, url: str, timeout: float = 10.0):
        """GET a otro exchange por la IP directa (las proxies son solo para Binance)."""
        try:
            data, nbytes = http_get_json(url, "", timeout)
        except urllib.error.HTTPError as exc:
            if exc.code in (400, 404):
                raise LookupError(f"HTTP {exc.code}")
            raise RuntimeError(f"HTTP {exc.code}")
        self.rx_alt_bytes += nbytes
        return data

    def _alt_window(self, ex: str, symx: str, start_ms: int, end_ms: int) -> list:
        """Velas 1m con apertura en [start_ms, end_ms], normalizadas y de la más vieja a la más nueva.
        LookupError si el exchange no tiene el par."""
        base = ALT_URLS[ex]
        out = []
        if ex == "bybit":
            q = urlencode({"category": "linear", "symbol": symx, "interval": "1", "start": start_ms,
                           "end": end_ms, "limit": ALT_MAX_ROWS["bybit"]})
            d = self._alt_http(f"{base}/v5/market/kline?{q}")
            if not isinstance(d, dict) or d.get("retCode") != 0:
                raise LookupError(f"{(d or {}).get('retMsg') if isinstance(d, dict) else d}")
            for r in (d.get("result") or {}).get("list") or []:      # [inicio, o, h, l, c, vol, turnover]
                out.append((int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
        elif ex == "kucoin":
            q = urlencode({"symbol": symx, "granularity": 1, "from": start_ms, "to": end_ms})
            d = self._alt_http(f"{base}/api/v1/kline/query?{q}")
            if not isinstance(d, dict) or str(d.get("code")) != "200000":
                raise LookupError(f"{(d or {}).get('msg') if isinstance(d, dict) else d}")
            for r in d.get("data") or []:                             # [inicio, o, h, l, c, vol (lotes)]
                out.append((int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
        elif ex == "mexc":
            q = urlencode({"interval": "Min1", "start": start_ms // 1000, "end": end_ms // 1000})
            d = self._alt_http(f"{base}/api/v1/contract/kline/{symx}?{q}")
            if not isinstance(d, dict) or not d.get("success") or not isinstance(d.get("data"), dict):
                raise LookupError(f"{(d or {}).get('message') or (d or {}).get('code') if isinstance(d, dict) else d}")
            c = d["data"]                                              # columnas: time (s), open, high…
            for i, ts in enumerate(c.get("time") or []):
                out.append((int(ts) * 1000, float(c["open"][i]), float(c["high"][i]), float(c["low"][i]),
                            float(c["close"][i]), float(c["vol"][i])))
        else:
            iso = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            q = urlencode({"granularity": 60, "start": iso(start_ms), "end": iso(end_ms)})
            d = self._alt_http(f"{base}/products/{symx}/candles?{q}")
            if not isinstance(d, list):
                raise LookupError(f"{(d or {}).get('message') if isinstance(d, dict) else d}")
            for r in d:                                               # [inicio s, low, high, open, close, vol]
                out.append((int(r[0]) * 1000, float(r[3]), float(r[2]), float(r[1]), float(r[4]), float(r[5])))
        return [[t, o, h, lo, c, v, t + 59_999] for t, o, h, lo, c, v in sorted(out) if start_ms <= t <= end_ms]

    def _alt_symbol(self, ex: str, sym: str) -> str:
        """Nombre del par en `ex` (se prueba con las últimas velas y se recuerda). LookupError si no
        lo tiene (también se recuerda 6 h para no repetir peticiones inútiles)."""
        key = (ex, sym)
        if key in self._alt_sym:
            return self._alt_sym[key]
        if time.time() - self._alt_missing.get(key, 0.0) < 6 * 3600:
            raise LookupError("no tiene el par")
        now_min = int(time.time() * 1000) // 60_000 * 60_000
        for cand in self._alt_candidates(ex, sym):
            try:
                if self._alt_window(ex, cand, now_min - 10 * 60_000, now_min):
                    self._alt_sym[key] = cand
                    return cand
            except LookupError:
                continue
        self._alt_missing[key] = time.time()
        raise LookupError("no tiene el par")

    def _alt_klines(self, ex: str, sym: str, n: int) -> list:
        """Las últimas `n` velas 1m (incluida la en curso) de otro exchange, en tantas peticiones
        como pida su máximo por petición (Bybit 1000, KuCoin 500, Coinbase 290, MEXC 1000)."""
        symx = self._alt_symbol(ex, sym)
        now_min = int(time.time() * 1000) // 60_000 * 60_000
        t, rows = now_min - (n - 1) * 60_000, {}
        while t <= now_min:
            end = min(now_min, t + (ALT_MAX_ROWS[ex] - 1) * 60_000)
            for r in self._alt_window(ex, symx, t, end):
                rows[r[0]] = r
            t = end + 60_000
        if not rows:
            raise LookupError("sin velas")
        return [rows[k] for k in sorted(rows)]

    async def _ind_seed(self, sym: str) -> None:
        """Carga inicial por REST (una petición) de las velas que piden el RSI y el VWAP del día."""
        try:
            rows, src = await asyncio.to_thread(self._fetch_seed_klines, sym)
        except Exception as exc:
            self._ind_err[sym] = str(exc)
            self.log(f"[RSI/VWAP] {sym}: no pude cargar las velas ({exc}); reintento en 30 s")
            return self._schedule_seed(sym, 30.0)
        self._ind_seeding.discard(sym)
        self._ind_seeded(sym, rows, src)

    def _ind_seeded(self, sym: str, rows: list, src: str = "binance") -> None:
        buf = self._ind_buf.pop(sym, None)
        if buf is None:
            return                                     # se cerró la posición mientras tanto
        ind = CandleIndicators(self.ind_cfg["rsi_period"])
        now_ms = time.time() * 1000
        for r in rows:
            try:
                if int(r[6]) < now_ms:                 # la vela en curso (sin cerrar) no cuenta
                    ind.add(int(r[0]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))
            except (TypeError, ValueError, IndexError):
                continue
        if src == "binance":
            for c in sorted(buf):                      # las del WS que llegaron durante la carga
                ind.add(*c)
        self._ind[sym] = ind
        self._ind_src[sym] = src
        self._ind_err.pop(sym, None)
        self._ind_seed_ts[sym] = time.time()
        self.ind_seeds += 1
        b = ind.bands(self.ind_cfg["vwap_k"])
        self.log(f"[RSI/VWAP] {sym}: {ind.count} velas de {EX_LABEL.get(src, src)} por REST "
                 f"({ind.day_candles} de hoy UTC) | RSI {ind.rsi if ind.rsi is not None else float('nan'):.1f} | "
                 f"VWAP {b[1] if b else 0:g} ±{self.ind_cfg['vwap_k']:g}σ [{b[0] if b else 0:g} · {b[2] if b else 0:g}]; "
                 f"sigue vela a vela por {'WebSocket de Binance' if src == 'binance' else f'REST de {EX_LABEL.get(src, src)} cada minuto'}")
        self._sync_indicators()                        # kline del WS de Binance solo si vienen de Binance

    async def _ind_alt_loop(self) -> None:
        """Pares cargados de otro exchange: sus últimas velas cerradas, una petición pequeña por
        minuto al MISMO exchange (no se mezclan volúmenes de mercados distintos)."""
        while self.running:
            await asyncio.sleep(60 - time.time() % 60 + 2)     # 2 s después de cada cierre de minuto
            for sym, ex in list(self._ind_src.items()):
                if ex == "binance" or sym not in self._ind:
                    continue
                now_min = int(time.time() * 1000) // 60_000 * 60_000
                try:
                    symx = self._alt_sym[(ex, sym)]
                    got = await asyncio.to_thread(self._alt_window, ex, symx, now_min - 4 * 60_000, now_min)
                except Exception as exc:
                    self._ind_err[sym] = f"{EX_LABEL[ex]}: {exc}"
                    continue
                self._ind_err.pop(sym, None)
                for r in got:
                    if r[0] + 60_000 <= time.time() * 1000:
                        self._ind_candle(sym, r[0], r[2], r[3], r[4], r[5], ex)

    def _on_ws_kline(self, sym: str, t: int, o: float, h: float, lo: float, c: float, v: float) -> None:
        loop = self.loop                               # hilo del WS: al loop del bot
        if loop is not None:
            loop.call_soon_threadsafe(self._ind_candle, sym, t, h, lo, c, v)

    def _ind_candle(self, sym: str, t: int, h: float, lo: float, c: float, v: float, src: str = "binance") -> None:
        """Vela 1m cerrada: actualiza RSI y VWAP del símbolo y revisa el TP. Solo cuentan las velas
        del exchange del que se cargó (no se mezclan volúmenes de mercados distintos)."""
        buf = self._ind_buf.get(sym)
        if buf is not None:
            if src == "binance":
                buf.append((t, h, lo, c, v))
            return
        ind = self._ind.get(sym)
        if ind is None or t <= ind.last_open or self._ind_src.get(sym, "binance") != src:
            return
        if src != "binance" and t - ind.last_open <= 15 * 60_000:
            # Coinbase no publica minutos sin operaciones: se rellenan planos (sin volumen).
            for gap in range(ind.last_open + 60_000, t, 60_000):
                ind.add(gap, ind.close, ind.close, ind.close, 0.0)
        if t > ind.last_open + 60_000:                 # faltan velas (reconexión): se recarga por REST
            self.log(f"[RSI/VWAP] {sym}: hueco de {(t - ind.last_open) // 60_000 - 1} vela(s); recargo por REST")
            del self._ind[sym]
            self._ind_src.pop(sym, None)
            self._ind_buf[sym] = [(t, h, lo, c, v)] if src == "binance" else []
            self._schedule_seed(sym)
            return
        ind.add(t, h, lo, c, v)
        self._ind_check(sym, ind)

    def _ind_check(self, sym: str, ind: CandleIndicators) -> None:
        cfg = self.ind_cfg
        names = [n for n in RATIO_ENGINES if cfg["enabled"].get(n)]
        if not names or not self.store.can_act():
            return
        closed_ms = ind.last_open + 60_000
        with self.lock:                                # solo velas cerradas DESPUÉS de abrir
            targets = [p for n in names for p in self.engines[n].positions.values()
                       if p.symbol == sym and not p.revenge and closed_ms > p.opened_ts * 1000]
        for p in targets:
            why = indicator_exit(ind, p.direction, cfg)
            if not why:
                continue
            self.ind_hits += 1
            self.log(f"[RSI/VWAP] TP {p.direction} {sym} ({ENGINE_LABELS[p.strategy]}): {why} "
                     f"(vela {_utc(ind.last_open / 1000, '%H:%M')} UTC) | trade #{p.trade_id}")
            self._close_after_hold(p.trade_id, "TP_RSI_VWAP")

    def ind_snapshot(self) -> dict:
        cfg = self.ind_cfg
        rows = {}
        for sym in sorted(set(self._ind) | set(self._ind_buf)):
            ind = self._ind.get(sym)
            if ind is None:
                rows[sym] = {"state": "error" if sym in self._ind_err else "cargando", "error": self._ind_err.get(sym, "")}
                continue
            b = ind.bands(cfg["vwap_k"])
            rows[sym] = {"state": "listo" if ind.ready() else "calentando", "src": self._ind_src.get(sym, "binance"),
                         "rsi": ind.rsi, "close": ind.close,
                         "vwap": b[1] if b else None, "lo": b[0] if b else None, "hi": b[2] if b else None,
                         "candles": ind.count, "today": ind.day_candles,
                         "last": _utc(ind.last_open / 1000, "%H:%M") if ind.last_open else ""}
        pc = self.price_cache
        return {"cfg": cfg, "symbols": rows, "seeds": self.ind_seeds, "hits": self.ind_hits,
                "kline_msgs": pc.kline_ticks if pc is not None else 0,
                "next_seed": klines_needed(int(time.time() * 1000), cfg["rsi_period"]),
                "exchanges": [EX_LABEL[x] for x in IND_EXCHANGES]}

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
        # Sale ya al executor con el precio en vivo; la entrada en papel se corrige con el libro.
        px, src = self._fill_from_book(sym, side == LONG, qty, price)
        self._ratio_open_at(eng, sym, side, prev, last, chg, pct24, qty, price, px, src, skip)

    def _ratio_open_at(self, eng: Engine, sym: str, side: str, prev: Tuple[int, float], last: Tuple[int, float],
                       chg: float, pct24: float, qty: float, signal_px: float, price: float, src: str, skip) -> None:
        """Abre la posición del bot de ratio con la entrada en papel `price` (libro)."""
        with self.lock:
            if any(p.symbol == sym and p.direction == side and not p.revenge for p in eng.positions.values()):
                return skip("ya hay una abierta en este par y dirección")
        eff = qty * price
        # TP: el ratio recupera el mismo % que cayó (SHORT) / subió (LONG), desde el ratio de entrada.
        target = last[1] * (1 + abs(chg) / 100.0) if side == SHORT else last[1] * (1 - abs(chg) / 100.0)
        tp_px, sl_px = self._ratio_exit_prices(price, side, eng.name)
        pos = Position(
            trade_id=self._next_trade_id(), symbol=sym, direction=side,
            entry_price=price, qty=qty, notional=eff, tp_price=tp_px, sl_price=sl_px,
            signal_pct=chg, candle_open=prev[1], candle_close=last[1], candle_time=last[0],
            last_price=price, strategy=eng.name, inverted=False, sent=not self.executor_paused,
            ratio_trigger=prev[1], ratio_entry=last[1], ratio_change_pct=chg, ratio_ts=last[0],
            ratio_target=target, signal_price=signal_px, entry_src=src,
        )
        with self.lock:
            eng.positions[pos.trade_id] = pos
            eng.opened_count += 1
        self._refresh_pos_syms()                            # suscribe el precio en vivo del par
        self.store.mark_dirty(True)
        skip(f"abierta #{pos.trade_id}")
        by_ratio = (f" · TP ratio {'≥' if side == SHORT else '≤'} {target:g}"
                    if self._exit_cfg(eng.name)["ratio_exit"] else "")
        by_ind = " · TP RSI/VWAP" if self.ind_cfg["enabled"].get(eng.name) else ""
        self.log(f"[{eng.label}] ABRE {side} {sym}{'' if pos.sent else ' [SIMULADA: envíos al executor pausados]'} | "
                 f"ratio {prev[1]:g}→{last[1]:g} ({chg:+.1f}%) | 24 h {pct24:+.1f}% | px={price:g}"
                 f"{f' ({src})' if src else ''} qty={qty:g} ({eff:.2f} USDT) | "
                 f"TP {tp_px or '—'} · SL {sl_px or '—'}{by_ratio}{by_ind} | trade #{pos.trade_id}")
        if pos.sent:
            self.executor.send({
                "action": "open", "trade_id": pos.trade_id, "symbol": sym, "direction": side,
                "price": signal_px, "quantity": qty, "notional": round(eff, 4),
                "level": round(chg, 4), "strategy": eng.name, **self._open_tp(pos),
            }, time.perf_counter())
        self._refine_entry(eng, pos)

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
            "filters": len(self.filters), "ind": self.ind_snapshot(),
            "book": {"on": BOOK_FILL, "fetches": self.book_fetches, "errors": self.book_errors},
            "routes": self.rest_routes_snapshot(),
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

    # ── Bot de funding alto ───────────────────────────────────────────────────
    # Una foto por WebSocket (!markPrice@arr, todos los pares) unos segundos antes de cada hora en
    # punto. Los pares con |funding| ≥ umbral y cobro en esa hora se abren en el lado que cobra
    # FUNDING_OPEN_BEFORE_S antes del cobro (al executor YA, con el precio en vivo; la entrada en
    # papel se corrige después con el libro) y se cierran FUNDING_CLOSE_AFTER_S después.

    # Abrir con menos antelación que FUNDING_MIN_LEAD_S no entra nunca (_funding_open la descarta por tardía):
    # el mínimo de open_before_s queda por encima para que la web no acepte un valor que apaga el bot en silencio.
    _FUNDING_LIMITS = {"min_pct": (0.01, 10.0), "notional": (5.0, 100000.0),
                       "open_before_s": (FUNDING_OPEN_MIN_S, 60.0),
                       "close_after_s": (0.0, 60.0), "scan_before_s": (FUNDING_SCAN_MIN_S, 900.0)}

    @classmethod
    def _load_funding_cfg(cls, raw) -> dict:
        raw = raw if isinstance(raw, dict) else {}
        cfg = {"min_pct": FUNDING_MIN_PCT, "notional": FUNDING_NOTIONAL, "open_before_s": FUNDING_OPEN_BEFORE_S,
               "close_after_s": FUNDING_CLOSE_AFTER_S, "scan_before_s": FUNDING_SCAN_BEFORE_S}
        for key, (lo, hi) in cls._FUNDING_LIMITS.items():
            try:
                v = abs(float(raw[key])) if key == "min_pct" else float(raw[key])
            except (KeyError, TypeError, ValueError):
                continue
            if lo <= v <= hi:
                cfg[key] = v
        if cfg["scan_before_s"] < cfg["open_before_s"] + 3:
            cfg["scan_before_s"] = cfg["open_before_s"] + 3
        return cfg

    def _funding_desc(self, c: Optional[dict] = None) -> str:
        c = c or self.funding_cfg
        return (f"funding ≥ ±{c['min_pct']:g}% · {c['notional']:g} USDT · abre {c['open_before_s']:g} s antes del "
                f"cobro y cierra {c['close_after_s']:g} s después · consulta {c['scan_before_s']:g} s antes")

    def set_funding_config(self, **kw) -> dict:
        """Cambia en caliente el umbral, el notional y los tiempos del bot de funding."""
        new = dict(self.funding_cfg)
        for key, val in kw.items():
            if key not in self._FUNDING_LIMITS or val is None:
                continue
            lo, hi = self._FUNDING_LIMITS[key]
            v = abs(float(val)) if key == "min_pct" else float(val)
            if not lo <= v <= hi:
                raise ValueError(f"{key} debe estar entre {lo:g} y {hi:g}")
            new[key] = v
        if new["scan_before_s"] < new["open_before_s"] + 3:
            raise ValueError("la consulta debe hacerse al menos 3 s antes que la apertura")
        with self.lock:
            self.funding_cfg = new
        self._save_settings()
        self.log(f"[config] {ENGINE_LABELS[ENGINE_FUNDING]}: {self._funding_desc(new)}")
        return dict(new)

    def _srv_now(self) -> float:
        """Hora de Binance (la local corregida con la última medición de _funding_clock_sync)."""
        off = self._perf_to_srv
        if off is not None:                 # ancla monotónica: no la mueven saltos ni correcciones del reloj del sistema
            return time.perf_counter() + off
        return time.time() + self.funding_clock_off

    def _clock_off(self) -> float:
        """Hora de Binance − hora local ahora mismo (s)."""
        return self._srv_now() - time.time()

    def _funding_paid(self, p: Position, now: Optional[float] = None) -> float:
        """Funding cobrado por la posición: solo si estaba abierta en el momento del cobro."""
        if not p.funding_ts:
            return 0.0
        srv = (now if now is not None else time.time()) + self._clock_off()
        if srv < p.funding_ts or p.opened_ts + self._clock_off() >= p.funding_ts:
            return 0.0
        return funding_income_usd(p.notional, p.funding_rate, p.direction)

    async def _funding_ws_snapshot(self, max_s: Optional[float] = None, cap: Optional[int] = None) -> List[dict]:
        """Funding de TODOS los perpetuos por WebSocket (!markPrice@arr): se conecta y junta mensajes (el último valor
        de cada par gana) hasta que dos seguidos no traen pares nuevos (mínimo FUNDING_COLLECT_MIN_S, máximo `max_s`),
        y se cierra. Un solo mensaje no basta: puede llegar incompleto y entonces faltaban pares que cumplían.
        Sin REST. NO toca el reloj (ver _funding_clock_sync)."""
        max_s = FUNDING_COLLECT_MAX_S if max_s is None else max_s
        cap = max(cap or FUNDING_HOURLY_MAX_SYMBOLS, 1)
        t0 = time.monotonic()
        deadline = t0 + max(max_s, 1.0)
        last_exc: Optional[Exception] = None
        for url in sorted(FUNDING_WS_URLS, key=lambda u: u != self._funding_ws_ok):
            if deadline - time.monotonic() <= 0.5:
                break
            seen: Dict[str, dict] = {}
            msgs, stale = 0, 0
            agen = _ws_messages(url)
            try:
                while len(seen) < cap:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(agen.__anext__(), min(left, FUNDING_WS_TIMEOUT_S))
                    except (asyncio.TimeoutError, StopAsyncIteration) as exc:
                        if not seen:
                            last_exc = exc if isinstance(exc, asyncio.TimeoutError) else RuntimeError("cerrado")
                        break
                    rows = self._parse_mark_rows(raw)
                    if rows is None:
                        last_exc = RuntimeError("markPrice: respuesta inesperada")
                        break
                    msgs += 1
                    before = len(seen)
                    for r in rows:
                        if r["symbol"] in seen or len(seen) < cap:
                            seen[r["symbol"]] = r                   # el último mensaje gana
                    stale = stale + 1 if len(seen) == before else 0
                    if seen and stale >= 2 and time.monotonic() - t0 >= FUNDING_COLLECT_MIN_S:
                        break                                       # dos mensajes sin pares nuevos: ya están todos
            except Exception as exc:
                last_exc = exc
            finally:
                try:
                    await asyncio.wait_for(agen.aclose(), 2.0)
                except Exception:
                    pass
            if not seen:
                if last_exc is None:
                    last_exc = RuntimeError("markPrice: sin pares")
                continue
            self.funding_last_scan_ms = (time.monotonic() - t0) * 1000
            self._funding_ws_ok = url
            self._funding_snap_msgs = msgs
            return list(seen.values())
        raise RuntimeError(f"sin funding por WebSocket: {last_exc!r}")

    def _funding_rest_rows(self) -> List[dict]:
        """premiumIndex por REST (con las rutas/proxies de _rest_get): todos los perpetuos en una sola respuesta."""
        data = self._rest_get("/fapi/v1/premiumIndex", timeout=10.0)
        if not isinstance(data, list):
            raise RuntimeError("premiumIndex: respuesta inesperada")
        rows = []
        for d in data:
            try:
                sym = str(d["symbol"])
                if not sym.endswith(QUOTE_ASSET):
                    continue
                rows.append({"symbol": sym, "rate_pct": float(d.get("lastFundingRate") or 0) * 100.0,
                             "next_ts": int(d.get("nextFundingTime") or 0) / 1000.0,
                             "mark": float(d.get("markPrice") or 0)})
            except (KeyError, TypeError, ValueError):
                continue
        if not rows:
            raise RuntimeError("premiumIndex: sin pares")
        return rows

    async def _funding_full_snapshot(self, max_s: Optional[float] = None, cap: Optional[int] = None,
                                     rest: bool = False) -> List[dict]:
        """Pares por WebSocket y, solo si rest=True (consulta final) y no se pidió en la última
        FUNDING_REST_MIN_INTERVAL_S, también premiumIndex por REST en paralelo (lista completa). Se unen: de cada par
        vale el WebSocket si lo trajo (más reciente) y si no el REST. Con que uno responda, basta."""
        t0 = time.monotonic()
        tasks = [self._funding_ws_snapshot(max_s, cap)]
        if rest and FUNDING_REST_SNAPSHOT and time.time() - self._funding_rest_ts >= FUNDING_REST_MIN_INTERVAL_S:
            self._funding_rest_ts = time.time()
            tasks.append(asyncio.to_thread(self._funding_rest_rows))
        res = await asyncio.gather(*tasks, return_exceptions=True)
        ws = res[0] if isinstance(res[0], list) else []
        rest = res[1] if len(res) > 1 and isinstance(res[1], list) else []
        errs = [f"{n}: {r!r}" for n, r in zip(("WebSocket", "REST"), res) if isinstance(r, BaseException)]
        if not ws and not rest:
            raise RuntimeError(" | ".join(errs) or "sin datos")
        merged: Dict[str, dict] = {r["symbol"]: r for r in rest}
        merged.update({r["symbol"]: r for r in ws})
        only_rest = len(set(merged) - {r["symbol"] for r in ws}) if ws else 0
        self._funding_src = {"ws": len(ws), "rest": len(rest), "total": len(merged), "only_rest": only_rest,
                             "errors": errs}
        self.funding_last_scan_ms = (time.monotonic() - t0) * 1000
        if errs:
            self.log(f"[funding] consulta con una sola fuente ({len(merged)} pares): {' | '.join(errs)}")
        elif only_rest:
            self.log(f"[funding] el WebSocket trajo {len(ws)} pares y el REST {len(rest)}: "
                     f"{only_rest} solo llegaron por REST (se incluyen)")
        return list(merged.values())

    def _funding_remember(self, rows: List[dict]) -> List[dict]:
        """Suma `rows` a la memoria de pares vistos y devuelve todos los vistos en los últimos FUNDING_CACHE_TTL_S
        (de cada par, su valor más reciente)."""
        now = time.time()
        with self.lock:
            for r in rows:
                self._funding_seen[r["symbol"]] = (now, r)
            for sym in [k for k, (t, _r) in self._funding_seen.items() if now - t > FUNDING_CACHE_TTL_S]:
                del self._funding_seen[sym]
            return [r for _t, r in self._funding_seen.values()]

    def _clock_sync_thread(self, full: bool = True) -> dict:
        """Corre en un hilo con su propio event loop (el retraso del loop principal no entra en la medición).
        Todo se mide contra perf_counter (monotónico): [(rtt, hora_fuente − perf_counter)].
        full=True: Binance + proveedores externos (NTP / Cloudflare) en paralelo; False: solo Binance, rápido."""
        res: dict = {"binance": [], "providers": {}, "errors": []}
        n = max(FUNDING_CLOCK_SAMPLES, 4) if full else 6

        async def binance() -> List[Tuple[float, float]]:
            last_exc: Optional[Exception] = None
            for url in API_WS_URLS:
                samples: List[Tuple[float, float]] = []
                try:
                    async with _WsRpc(url) as ws:
                        for i in range(n):
                            rid = f"t{i}"
                            p0 = time.perf_counter()
                            raw = await ws.call(json.dumps({"id": rid, "method": "time"}), 3.0)
                            p1 = time.perf_counter()
                            msg = json.loads(raw)
                            if msg.get("id") != rid or msg.get("status") != 200:
                                continue
                            srv = float(msg["result"]["serverTime"]) / 1000.0
                            samples.append((p1 - p0, srv - (p0 + p1) / 2.0))     # latencia, Binance − perf
                            await asyncio.sleep(0.05)
                    if samples:
                        return samples
                except Exception as exc:
                    last_exc = exc
            raise RuntimeError(f"time por WebSocket falló: {last_exc!r}")

        try:
            res["binance"] = asyncio.run(binance())
        except Exception as exc:
            res["errors"].append(f"Binance: {exc}")
        if full and (FUNDING_NTP_HOSTS or FUNDING_HTTP_TIME_URLS):
            k = FUNDING_CLOCK_PROVIDER_SAMPLES
            jobs: Dict[concurrent.futures.Future, str] = {}
            ex = concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="clock")
            try:
                for h in FUNDING_NTP_HOSTS:
                    jobs[ex.submit(ntp_samples, h, k)] = f"ntp:{h}"
                for u in FUNDING_HTTP_TIME_URLS:
                    jobs[ex.submit(http_time_samples, u, k)] = f"http:{urlsplit(u).hostname}"
                done, _pending = concurrent.futures.wait(list(jobs), timeout=8.0)
                for fut, name in jobs.items():
                    if fut not in done:
                        res["errors"].append(f"{name}: sin respuesta a tiempo")
                        continue
                    try:
                        smp = fut.result()
                    except Exception as exc:
                        res["errors"].append(f"{name}: {exc!r}")
                        continue
                    if smp:
                        res["providers"][name] = smp
                    else:
                        res["errors"].append(f"{name}: sin muestras (¿UDP/HTTPS bloqueado?)")
            finally:
                ex.shutdown(wait=False)
        return res

    async def _clock_keeper_loop(self) -> None:
        """Guardián del reloj, aparte de _funding_loop (que a 3 min del cobro está ocupado con la consulta):
          • sync completa FUNDING_CLOCK_RECHECK_BEFORE_S (2 min) antes de cada cobro, además de la de 5 min;
          • re-medición rápida (solo Binance) cada FUNDING_CLOCK_RESYNC_S si la última es más vieja.
        Nada a menos de FUNDING_CLOCK_QUIET_S de la apertura: ahí está la verificación de último momento."""
        await asyncio.sleep(60.0)                            # la primera sync la hace _funding_loop al arrancar
        while self.running:
            now_srv = self._srv_now()
            target = self._funding_target_ts or next_funding_ts(now_srv, 0.0)
            left = target - now_srv                          # hasta el cobro
            to_open = left - self.funding_cfg["open_before_s"]
            quiet = -5.0 < to_open < FUNDING_CLOCK_QUIET_S
            recheck = FUNDING_CLOCK_RECHECK_BEFORE_S
            if (recheck > 0 and self._clock_rechecked_for != target and left <= recheck
                    and to_open > FUNDING_CLOCK_QUIET_S):
                self._clock_rechecked_for = target           # una vez por cobro, salga bien o no
                await self._funding_clock_sync(full=True)
                continue
            age = time.time() - self.funding_clock_ts if self.funding_clock_ts else 1e9
            if FUNDING_CLOCK_RESYNC_S > 0 and age >= FUNDING_CLOCK_RESYNC_S and not quiet and not self._clock_busy:
                await self._funding_clock_sync(full=False)
                if time.time() - self.funding_clock_ts > FUNDING_CLOCK_RESYNC_S:
                    await asyncio.sleep(30.0)                # falló: reintenta en 30 s, no en bucle
                    continue
                age = 0.0
            nxt = [60.0]
            if FUNDING_CLOCK_RESYNC_S > 0:
                nxt.append(max(FUNDING_CLOCK_RESYNC_S - age, 10.0))
            if recheck > 0 and self._clock_rechecked_for != target and left > recheck:
                nxt.append(left - recheck)
            await asyncio.sleep(max(min(nxt), 1.0))

    async def _funding_clock_sync(self, full: bool = True) -> bool:
        """Mide la hora de Binance (y, en la completa, la de proveedores externos), las compara y fija el ancla
        monotónica con la que opera el bot. Si no hay medición fiable conserva la anterior y avisa."""
        if self._clock_busy:                                 # otra medición en curso: no se pisan
            for _ in range(150):
                await asyncio.sleep(0.1)
                if not self._clock_busy:
                    break
            if not full:
                return bool(self.funding_clock_ts)
        self._clock_busy = True
        try:
            return await self._funding_clock_sync_run(full)
        finally:
            self._clock_busy = False

    async def _funding_clock_sync_run(self, full: bool) -> bool:
        try:
            raw = await asyncio.to_thread(self._clock_sync_thread, full)
        except Exception as exc:
            self.log(f"[reloj] ✗ no pude sincronizar: {exc}")
            return False
        bres = pick_clock_offset(raw["binance"], FUNDING_CLOCK_MAX_RTT_S)
        provs: Dict[str, Tuple[float, float, float]] = {}
        for name, smp in raw["providers"].items():
            r = pick_clock_offset(smp, FUNDING_CLOCK_MAX_RTT_S)
            if r is not None:
                provs[name] = r
        off, src, info = clock_compare(bres, provs, FUNDING_CLOCK_TOLERANCE_S)
        if off is None:
            fastest = min((r for r, _ in raw["binance"]), default=0.0) * 1000
            self.log(f"[reloj] ✗ medición descartada (Binance: latencia mínima {fastest:.0f} ms, "
                     f"{len(raw['binance'])} muestras; proveedores válidos: {len(provs)}"
                     f"{'; ' + ' | '.join(raw['errors']) if raw['errors'] else ''}) · "
                     f"conservo {self._clock_off() * 1000:+.0f} ms")
            return False
        old = self._perf_to_srv
        self._perf_to_srv = off
        wall = self._clock_off()                              # Binance − reloj local, ahora
        self.funding_clock_off = wall
        self.funding_clock_ts = time.time()
        rtt, spread = (bres[1], bres[2]) if bres else (0.0, 0.0)
        self.funding_clock_rtt_ms = rtt * 1000
        shift = off - (old if old is not None else off)
        adj = time.perf_counter() - time.time()               # perf → reloj local, para mostrar
        provtxt = ", ".join(f"{k} {(v[0] + adj) * 1000:+.0f} ms (lat {v[1] * 1000:.0f})" for k, v in provs.items())
        info.update({"source": src, "full": full, "errors": raw["errors"], "ts": time.time(),
                     "n_providers": len(provs)})
        if full:
            self.funding_clock_info = info
        else:                                                # la verificación rápida conserva la comparación completa
            self.funding_clock_info = {**self.funding_clock_info, "source": src, "ts": info["ts"]}
        self.log(f"[reloj] {'sync completa' if full else 'verificación'}: Binance {wall * 1000:+.0f} ms respecto a este "
                 f"servidor ({'lat mín ' + format(rtt * 1000, '.0f') + ' ms, disp ' + format(spread * 1000, '.0f') + ' ms, ' if bres else 'SIN medición propia, '}"
                 f"{len(raw['binance'])} muestras) · se opera con {src}"
                 + (f" · proveedores: {provtxt}" if provtxt else (" · proveedores: ninguno disponible" if full else ""))
                 + (f" · Binance − proveedores {info['diff_ms']:+d} ms {'✓' if info.get('agree') else '⚠ difieren'}"
                    if "diff_ms" in info else "")
                 + (f" · ajuste vs. medición anterior {shift * 1000:+.0f} ms" if old is not None else ""))
        if raw["errors"] and full:
            self.log(f"[reloj] avisos: {' | '.join(raw['errors'])}")
        if abs(wall) > 1.0:
            self.log(f"[reloj] ⚠ el reloj de este servidor está desfasado {wall:+.2f} s de Binance (sin NTP en el host); "
                     f"el bot usa un reloj monotónico anclado a Binance y lo compensa")
        if old is not None and abs(shift) > 0.1:
            self.log(f"[reloj] ⚠ el desfase cambió {shift * 1000:+.0f} ms desde la medición anterior")
        if "diff_ms" in info and not info.get("agree"):
            self.log(f"[reloj] ⚠ Binance y los proveedores externos difieren {info['diff_ms']:+d} ms: "
                     f"la ruta a Binance puede ser asimétrica (la medición lleva ±{rtt * 500:.0f} ms de incertidumbre)")
        return True

    async def _loop_lag_monitor(self) -> None:
        """Mide cuánto se retrasa el event loop (si pasa de ~1 s las entradas llegan tarde aunque el reloj esté bien)."""
        worst, window_start = 0.0, time.time()
        while self.running:
            t = time.perf_counter()
            await asyncio.sleep(0.25)
            lag = (time.perf_counter() - t - 0.25) * 1000
            worst = max(worst, lag)
            if lag > 1000:
                self.log(f"[loop] ⚠ el event loop se retrasó {lag:.0f} ms (CPU saturada o código bloqueante)")
            if time.time() - window_start >= 60:
                self.loop_lag_ms, worst, window_start = worst, 0.0, time.time()

    async def _funding_scan(self) -> List[dict]:
        try:
            rows = await self._funding_full_snapshot(rest=True)
        except Exception as exc:
            self.funding_errors += 1
            self.funding_last_error = str(exc)
            self.log(f"[funding] ✗ no pude leer el funding: {exc}")
            return []
        rows = self._funding_remember(rows)             # un par que esta vez no llegó no se pierde
        thr = self.funding_cfg["min_pct"]
        rows.sort(key=lambda r: abs(r["rate_pct"]), reverse=True)
        with self.lock:
            self._funding_rows = self._funding_table_rows(rows, thr)
            self._funding_over = sum(1 for r in rows if abs(r["rate_pct"]) >= thr)
            self.funding_scans += 1
            self.funding_last_scan_ts = time.time()
            self.funding_last_error = ""
        return rows

    @staticmethod
    def _funding_table_rows(rows: List[dict], thr: float) -> List[dict]:
        """Filas para la web: TODOS los pares que superan el umbral (hasta 150) y, si sobra sitio, los siguientes
        de mayor |funding| hasta completar 25. Antes se cortaba en 20 y un par que cumplía podía quedar fuera."""
        ordered = sorted(rows, key=lambda r: abs(r["rate_pct"]), reverse=True)
        over = [r for r in ordered if abs(r["rate_pct"]) >= thr][:150]
        rest = [r for r in ordered if abs(r["rate_pct"]) < thr][:max(25 - len(over), 0)]
        return over + rest

    def _funding_signal(self, sym: str, side: str, rate: float, ts: float, mark: float, action: str) -> None:
        eng = self.engines[ENGINE_FUNDING]
        now = time.time()
        with self.lock:
            eng.signals.appendleft({
                "ts": now, "time": _utc(now, "%H:%M:%S"), "symbol": sym, "direction": side,
                "body_pct": rate, "open": mark, "close": mark, "candle": _utc(ts, "%H:%M"),
                "candle_age_s": 0.0, "action": action,
            })

    async def _funding_loop(self) -> None:
        # La tabla va PRIMERO y en paralelo a la sync del reloj (que puede tardar ~10 s): exploración completa
        # (hasta 30 s, la misma del botón). Si falla o no trae pares, se reintenta cada 30 s (ver más abajo).
        self._rows_retry_at = time.time() + 30.0
        self._spawn(self._funding_hourly_scan(
            next_funding_ts(self._srv_now(), self.funding_cfg["open_before_s"] + 0.3), warm=False))
        await self._funding_clock_sync()                     # reloj de Binance desde el arranque
        while self.running:
            self._funding_overdue()
            c = self.funding_cfg
            now = self._srv_now()
            ts = next_funding_ts(now, c["open_before_s"] + 0.3)
            if not self._funding_rows and not self._scan_busy and time.time() >= self._rows_retry_at:
                self._rows_retry_at = time.time() + 30.0     # sin datos para la web: no esperar a la exploración horaria
                self._spawn(self._funding_hourly_scan(ts, warm=False))
            self._funding_target_ts = ts
            left = ts - now
            if self._funding_done_ts == ts:              # ya programado: espera a que pase este cobro
                vlead = FUNDING_CLOCK_VERIFY_BEFORE_S
                if vlead > 0 and self._clock_verified_for != ts:
                    if left > vlead:
                        await asyncio.sleep(min(max(left - vlead, 0.05), 5.0))
                        continue
                    self._clock_verified_for = ts
                    if left > c["open_before_s"] + 2.5:      # verificación rápida (Binance) justo antes de abrir
                        await self._funding_clock_sync(full=False)
                    continue
                await asyncio.sleep(min(max(left, 0.2), 5.0))
                continue
            sync_lead = max(FUNDING_CLOCK_SYNC_BEFORE_S, c["scan_before_s"] + 20.0)   # el reloj se sincroniza antes de consultar
            hourly_lead = max(FUNDING_HOURLY_BEFORE_S, sync_lead + 40.0)
            if FUNDING_HOURLY_SCAN and self._funding_hourly_for != ts and left <= hourly_lead:
                self._funding_hourly_for = ts
                if left > sync_lead + 5.0:                   # si el bot arrancó tarde, se omite (no choca con la sync)
                    await self._funding_hourly_scan(ts)
                continue
            if self._clock_synced_for != ts and left <= sync_lead and time.time() >= self._clock_next_try:
                if await self._funding_clock_sync():
                    self._clock_synced_for = ts
                else:
                    self._clock_next_try = time.time() + 4.0       # reintenta en unos segundos, no en bucle
                continue
            wait = ts - c["scan_before_s"] - now
            if wait > 0:
                nxt = [wait, 5.0]
                if FUNDING_HOURLY_SCAN and self._funding_hourly_for != ts:
                    nxt.append(max(left - hourly_lead, 0.05))
                if self._clock_synced_for != ts and left > sync_lead:
                    nxt.append(left - sync_lead)
                await asyncio.sleep(max(min(nxt), 0.05))
                continue
            self._funding_done_ts = ts
            await self._funding_cycle(ts)

    @staticmethod
    def _parse_mark_rows(raw) -> Optional[List[dict]]:
        try:
            data = json.loads(raw)
            data = data.get("data", data) if isinstance(data, dict) else data
        except ValueError:
            return None
        if not isinstance(data, list):
            return None
        rows = []
        for d in data:
            try:
                sym = str(d["s"])
                if not sym.endswith(QUOTE_ASSET):
                    continue
                rows.append({"symbol": sym, "rate_pct": float(d.get("r") or 0) * 100.0,
                             "next_ts": int(d.get("T") or 0) / 1000.0, "mark": float(d.get("p") or 0)})
            except (KeyError, TypeError, ValueError):
                continue
        return rows

    def _release_watch(self) -> None:
        """Suelta los precios en vivo calentados por la exploración horaria. Los pares que van a entrar ya
        tienen su propia petición (la toma _funding_cycle antes de llamar aquí), así que no se dan de baja
        ni se quedan suscritos para siempre si al final no se entra (bot pausado)."""
        with self.lock:
            drop, self._watch_hold = self._watch_hold, set()
            self._release_price(drop)
        if drop:
            self._refresh_pos_syms(force=True)

    async def _funding_hourly_scan(self, ts: float, manual: bool = False, warm: bool = True) -> None:
        if self._scan_busy:                                  # ya hay una exploración completa en curso
            return
        self._scan_busy = True
        try:
            await self._funding_hourly_scan_run(ts, manual, warm)
        finally:
            self._scan_busy = False

    def request_funding_rescan(self) -> dict:
        """Botón de la web: explora YA todos los pares (≤ FUNDING_HOURLY_MAX_SYMBOLS y ≤ FUNDING_HOURLY_MAX_S s).
        Se niega si ya hay una en curso, si hace menos de 5 s de la última o si la consulta final del cobro es inminente."""
        loop = self.loop
        if loop is None or not loop.is_running():
            return {"ok": False, "error": "El bot aún no arrancó"}
        if self._scan_busy:
            return {"ok": False, "error": "Ya hay una exploración en curso"}
        now = time.time()
        if now - self._rescan_last < 5.0:
            return {"ok": False, "error": "Espera unos segundos entre exploraciones"}
        c = self.funding_cfg
        ts = self._funding_target_ts or next_funding_ts(self._srv_now(), c["open_before_s"])
        left = ts - self._srv_now()
        if 0 < left <= c["scan_before_s"] + 5.0:
            return {"ok": False, "error": f"Faltan {left:.0f} s para el cobro: la consulta final ya está por correr"}
        self._rescan_last = now
        asyncio.run_coroutine_threadsafe(self._funding_hourly_scan(ts, manual=True), loop)
        return {"ok": True, "max_symbols": FUNDING_HOURLY_MAX_SYMBOLS, "max_s": FUNDING_HOURLY_MAX_S}

    async def _funding_hourly_scan_run(self, ts: float, manual: bool = False, warm: bool = True) -> None:
        """Exploración horaria PROPIA del bot de funding (no comparte nada con la de los demás bots): lee todos los
        pares por WebSocket, como máximo FUNDING_HOURLY_MAX_SYMBOLS pares y FUNDING_HOURLY_MAX_S segundos, y guarda
        cuáles cumplen el umbral para el cobro `ts`. A los de mayor |funding| les calienta el precio en vivo."""
        self._release_watch()
        t0 = time.monotonic()
        cap = max(FUNDING_HOURLY_MAX_SYMBOLS, 1)
        last_exc: Optional[Exception] = None
        try:
            got = await self._funding_full_snapshot(max_s=FUNDING_HOURLY_MAX_S, cap=cap)
        except Exception as exc:
            got, last_exc = [], exc
        seen: Dict[str, dict] = {r["symbol"]: r for r in got}
        elapsed = time.monotonic() - t0
        if not seen:
            self.funding_errors += 1
            self.funding_last_error = f"exploración horaria sin datos: {last_exc!r}"
            self.log(f"[funding] ✗ exploración {'manual' if manual else 'horaria' if warm else 'inicial'} sin datos tras {elapsed:.1f} s: {last_exc!r}")
            return
        thr = self.funding_cfg["min_pct"]
        rows = sorted(self._funding_remember(list(seen.values())), key=lambda r: abs(r["rate_pct"]), reverse=True)
        cands = [r for r in rows if abs(r["rate_pct"]) >= thr and abs(r["next_ts"] - ts) <= 1.5]
        hot = ([r["symbol"] for r in cands if r["symbol"] in self.filters][:max(FUNDING_WATCH_PRICE_MAX, 0)]
               if warm else [])                              # la de arranque/reintento solo llena la tabla
        with self.lock:
            self._funding_rows = self._funding_table_rows(rows, thr)
            self._funding_over = sum(1 for r in rows if abs(r["rate_pct"]) >= thr)
            self.funding_last_scan_ts = time.time()
            self._watch_hold = set(hot)
            self._hold_price(hot)
            self._funding_watch = {
                "ts": ts, "when": time.time(), "scanned": len(seen), "capped": len(seen) >= cap,
                "elapsed_s": round(elapsed, 1), "n": len(cands),
                "cands": [{"symbol": r["symbol"], "rate_pct": round(r["rate_pct"], 4)} for r in cands[:20]],
            }
        if hot:
            self._refresh_pos_syms(force=True)
        if warm:
            self._funding_ensure_filters([r["symbol"] for r in cands])
        self.log(f"[funding] exploración {'MANUAL' if manual else 'horaria' if warm else 'inicial'}: {len(seen)} pares en {elapsed:.1f} s"
                 f"{' (tope ' + str(cap) + ')' if len(seen) >= cap else ''} · cobro {_utc(ts, '%H:%M')} UTC: "
                 + (f"{len(cands)} ≥ ±{thr:g}%: " + ", ".join(f"{r['symbol']} {r['rate_pct']:+.3f}%" for r in cands[:10])
                    if cands else f"ninguno ≥ ±{thr:g}%"))

    def _funding_ensure_filters(self, syms) -> None:
        """Pares con funding alto sin filtros de cantidad (p. ej. listados después de la última caché de exchangeInfo):
        pide exchangeInfo otra vez en segundo plano (como mucho una vez cada FUNDING_FILTERS_RETRY_S)."""
        missing = sorted(s for s in syms if s not in self.filters)
        if not missing or self._filters_busy or time.time() - self._filters_retry_ts < FUNDING_FILTERS_RETRY_S:
            return
        if not self._on_loop():
            return
        self._filters_retry_ts = time.time()
        self._filters_busy = True

        async def run() -> None:
            try:
                self.log(f"[funding] sin filtros de cantidad para {', '.join(missing[:10])}: pido exchangeInfo de nuevo")
                got = await self._fetch_exchange_info()
                if self._filters_from_file:
                    self.log("[funding] exchangeInfo no respondió (solo el archivo de respaldo): sigo sin esos filtros")
                    got = {k: v for k, v in got.items() if k not in self.filters}   # el archivo solo suma, no pisa
                added = sorted(set(got) - set(self.filters))
                with self.lock:
                    self.filters.update(got)
                    self.symbols = sorted(self.filters)
                if added:
                    self.log(f"[funding] filtros nuevos: {', '.join(added[:15])}")
            except Exception as exc:
                self.log(f"[funding] ✗ exchangeInfo: {exc}")
            finally:
                self._filters_busy = False

        self._spawn(run())

    def _funding_launch(self, r: dict, ts: float, held: bool = False) -> None:
        """Programa la entrada de un par que cumple (una sola vez por cobro). held=True: quien llama ya tomó la
        petición de precio en vivo del par; si no se lanza la entrada, se suelta aquí."""
        sym = r["symbol"]
        side = funding_side(r["rate_pct"])
        if sym in self._funding_launched or side is None or self.engines[ENGINE_FUNDING].paused:
            if held:
                self._release_price((sym,))
            if side is not None and sym not in self._funding_launched:
                self._funding_launched.add(sym)
                self._funding_signal(sym, side, r["rate_pct"], ts, r["mark"], "pausado")
            return
        self._funding_launched.add(sym)
        self._spawn(self._funding_trade(sym, side, r["rate_pct"], ts, r["mark"], held=held))

    async def _funding_cycle(self, ts: float) -> None:
        """Consulta el funding de TODOS los pares antes del cobro `ts`, programa las entradas de los que superan el
        umbral y deja el seguimiento en vivo de los que están cerca (el funding cambia hasta el cobro)."""
        rows = await self._funding_scan()
        thr = self.funding_cfg["min_pct"]
        due = [r for r in rows if abs(r["next_ts"] - ts) <= 1.5]
        cands = [r for r in due if abs(r["rate_pct"]) >= thr]
        later = [r for r in rows if abs(r["rate_pct"]) >= thr and abs(r["next_ts"] - ts) > 1.5]
        with self.lock:
            self._funding_live = {r["symbol"]: dict(r) for r in due}
            self._funding_live_for = ts
            self._funding_launched = set()
        if not self.engines[ENGINE_FUNDING].paused:
            self._hold_price(r["symbol"] for r in cands)    # antes de soltar los calentados: sin cortes
        held = not self.engines[ENGINE_FUNDING].paused
        self._release_watch()
        self._funding_ensure_filters([r["symbol"] for r in cands])
        extra = (f" · {len(later)} con funding alto cobran más tarde (intervalo 4/8 h): "
                 + ", ".join(f"{r['symbol']} {_utc(r['next_ts'], '%H:%M')}" for r in later[:5])) if later else ""
        if rows:
            src = self._funding_src
            info = (f"{len(rows)} pares leídos (WebSocket {src.get('ws', 0)} en {self._funding_snap_msgs} mensajes"
                    f" · REST {src.get('rest', 0)})")
            if cands:
                self.log(f"[funding] cobro {_utc(ts, '%H:%M')} UTC · {info}: {len(cands)} con funding ≥ ±{thr:g}%: "
                         + ", ".join(f"{r['symbol']} {r['rate_pct']:+.3f}%" for r in cands[:10]) + extra)
            else:
                top = rows[0]
                self.log(f"[funding] cobro {_utc(ts, '%H:%M')} UTC · {info}: ninguno ≥ ±{thr:g}% "
                         f"(mayor {top['symbol']} {top['rate_pct']:+.4f}%){extra}")
        for r in cands:
            self._funding_launch(r, ts, held=held)
        if not FUNDING_TRACK or self.engines[ENGINE_FUNDING].paused:
            return
        near = sorted((r for r in due if abs(r["rate_pct"]) >= thr * max(FUNDING_TRACK_RATIO, 0.0)),
                      key=lambda r: abs(r["rate_pct"]), reverse=True)[:max(FUNDING_TRACK_MAX, 0)]
        if near:
            self._spawn(self._funding_track(ts, [r["symbol"] for r in near]))

    @staticmethod
    def _parse_mark_one(raw) -> Optional[dict]:
        """Un mensaje del stream combinado <par>@markPrice@1s → fila {symbol, rate_pct, next_ts, mark}."""
        try:
            d = json.loads(raw)
            d = d.get("data", d) if isinstance(d, dict) else None
            if not isinstance(d, dict) or "s" not in d:
                return None
            return {"symbol": str(d["s"]), "rate_pct": float(d.get("r") or 0) * 100.0,
                    "next_ts": int(d.get("T") or 0) / 1000.0, "mark": float(d.get("p") or 0)}
        except (ValueError, TypeError, KeyError):
            return None

    async def _funding_track(self, ts: float, syms: List[str]) -> None:
        """Sigue en vivo el funding de `syms` (solo esos pares, 1 msg/s cada uno) hasta FUNDING_TRACK_END_S antes de
        abrir. Un par que cruza el umbral entra; los que bajan se descartan al abrir (ver _funding_trade)."""
        streams = "/".join(f"{s.lower()}@markPrice@1s" for s in syms)
        thr0 = self.funding_cfg["min_pct"]
        joined: List[str] = []
        msgs = 0
        start_rates = {s: self._funding_live.get(s, {}).get("rate_pct", 0.0) for s in syms}
        bases = sorted(FUNDING_TRACK_WS_BASES, key=lambda b: b != self._funding_track_ok)
        tries = 0
        while tries < 6:
            end = ts - self.funding_cfg["open_before_s"] - FUNDING_TRACK_END_S
            if end - self._srv_now() <= 0.2:
                break
            base = bases[tries % len(bases)]
            tries += 1
            agen = _ws_messages(base + streams)
            try:
                while True:
                    left = (ts - self.funding_cfg["open_before_s"] - FUNDING_TRACK_END_S) - self._srv_now()
                    if left <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(agen.__anext__(), min(left, FUNDING_WS_TIMEOUT_S))
                    except (asyncio.TimeoutError, StopAsyncIteration):
                        break
                    r = self._parse_mark_one(raw)
                    if r is None or abs(r["next_ts"] - ts) > 1.5:
                        continue
                    msgs += 1
                    self._funding_track_ok = base
                    thr = self.funding_cfg["min_pct"]
                    with self.lock:
                        if self._funding_live_for != ts:
                            return                                  # ya es otro cobro
                        self._funding_live[r["symbol"]] = r
                        for row in self._funding_rows:              # la tabla de la web, al día
                            if row["symbol"] == r["symbol"]:
                                row["rate_pct"], row["mark"] = r["rate_pct"], r["mark"]
                                break
                    if abs(r["rate_pct"]) >= thr and r["symbol"] not in self._funding_launched:
                        joined.append(r["symbol"])
                        self.log(f"[funding] {r['symbol']} cruzó el umbral en el seguimiento: "
                                 f"{start_rates.get(r['symbol'], 0.0):+.4f}% → {r['rate_pct']:+.4f}% (≥ ±{thr:g}%)")
                        self._funding_launch(r, ts)
            except Exception as exc:
                self.log(f"[funding] seguimiento: {exc!r}")
            finally:
                try:
                    await asyncio.wait_for(agen.aclose(), 2.0)
                except Exception:
                    pass
            if msgs:
                break
            await asyncio.sleep(1.0)
        thr = self.funding_cfg["min_pct"]
        with self.lock:
            fell = [s for s in self._funding_launched
                    if s in self._funding_live and abs(self._funding_live[s]["rate_pct"]) < thr]
            self._funding_over = sum(1 for row in self._funding_rows if abs(row["rate_pct"]) >= thr)
        self.log(f"[funding] seguimiento en vivo de {len(syms)} par(es) cerca de ±{thr0:g}%: {msgs} actualizaciones"
                 + (f" · entraron {', '.join(joined)}" if joined else "")
                 + (f" · bajaron del umbral (no se abren): {', '.join(sorted(fell))}" if fell else "")
                 + ("" if msgs else " · ✗ el stream no respondió: se opera con la consulta de 3 min antes"))

    async def _funding_trade(self, sym: str, side: str, rate: float, ts: float, mark: float,
                             held: bool = False) -> None:
        """Precio en vivo del par desde ya (aggTrade) y, al llegar el momento, abre sin esperar nada. Usa el último
        funding visto en el seguimiento: si bajó del umbral no abre, y si cambió de signo cambia de lado.
        held=True: la petición de precio ya la tomó quien la lanzó; aquí solo se suelta al terminar."""
        if not held:
            self._hold_price((sym,))
        try:
            self._refresh_pos_syms(force=True)
            try:
                await self._prefetch_price(sym)
            except Exception:
                pass
            while True:                                 # se reevalúa el reloj: una re-sincronización se aplica sobre la marcha
                wait = ts - self.funding_cfg["open_before_s"] - self._srv_now()
                if wait <= 0:
                    break
                await asyncio.sleep(min(wait, 0.5))
            live = self._funding_live.get(sym) if self._funding_live_for == ts else None
            if live:
                rate, mark = live["rate_pct"], live["mark"] or mark
                thr = self.funding_cfg["min_pct"]
                if abs(rate) < thr:
                    self._funding_signal(sym, side, rate, ts, mark,
                                         f"descartada: el funding bajó a {rate:+.4f}% (< ±{thr:g}%) antes de abrir")
                    return
                side = funding_side(rate) or side
            self._funding_open(sym, side, rate, ts, mark)
        finally:
            self._release_price((sym,))
            self._refresh_pos_syms(force=True)

    def _funding_open(self, sym: str, side: str, rate: float, ts: float, mark: float) -> Optional[Position]:
        eng = self.engines[ENGINE_FUNDING]
        eng.signal_count += 1
        t0 = time.perf_counter()

        def skip(action: str) -> None:
            self._funding_signal(sym, side, rate, ts, mark, action)

        left = ts - self._srv_now()
        if left <= max(FUNDING_MIN_LEAD_S, 0.05):
            return skip(f"ignorada: llegó tarde al cobro (faltaban {left:.2f} s, mínimo {FUNDING_MIN_LEAD_S:g} s)")
        if eng.paused:
            return skip("pausado")
        if eng.blocked.get(side):
            return skip(f"bloqueada: {side} desactivados")
        if self.max_open > 0 and self.open_count() >= self.max_open:
            return skip(f"ignorada: límite de {self.max_open} abiertas")
        if not self.store.can_open():
            return skip("ignorada: estado sin confirmar")
        with self.lock:
            dup = any(p.symbol == sym and p.funding_ts == ts for p in eng.positions.values())
        if dup:
            return skip("ya hay una abierta para este cobro")
        if sym not in self.filters:
            return skip("saltada: sin filtros del símbolo (exchangeInfo no cargó)")
        price = self._live_price(sym)
        src = "precio en vivo"
        if not price:
            price, src = mark, "mark de la consulta"
        if not price:
            return skip("sin precio")
        f = self.filters[sym]
        qty, eff, why = size_order(self.funding_cfg["notional"], price, f.get("stepSize", 0.001),
                                   f.get("minQty", 0.0), f.get("minNotional", 5.0), MAX_NOTIONAL_OVER)
        if qty <= 0:
            self.log(f"[{eng.label}] {side} {sym} saltada: {why}")
            return skip(f"saltada: {why}")
        pos = Position(
            trade_id=self._next_trade_id(), symbol=sym, direction=side, entry_price=price, qty=qty,
            notional=eff, tp_price=0.0, sl_price=0.0, signal_pct=rate, candle_open=mark, candle_close=price,
            candle_time=int(ts * 1000), last_price=price, strategy=eng.name, sent=not self.executor_paused,
            signal_price=price, entry_src=src, funding_rate=rate, funding_ts=ts,
        )
        with self.lock:
            eng.positions[pos.trade_id] = pos
            eng.opened_count += 1
            eng._decide_ms.append((time.perf_counter() - t0) * 1000)
        if pos.sent:                                # al executor YA, con el precio en vivo
            self.executor.send({
                "action": "open", "trade_id": pos.trade_id, "symbol": sym, "direction": side,
                "price": price, "quantity": qty, "notional": round(eff, 4),
                "level": round(rate, 6), "strategy": eng.name,
            }, t0)
        self._refresh_pos_syms()
        self.store.mark_dirty(True)
        skip(f"abierta #{pos.trade_id}")
        gain = funding_income_usd(eff, rate, side)
        self.log(f"[{eng.label}] ABRE {side} {sym}{'' if pos.sent else ' [SIMULADA: envíos al executor pausados]'} | "
                 f"funding {rate:+.4f}% → cobra ~{gain:+.4f} USDT a las {_utc(ts, '%H:%M:%S')} UTC "
                 f"({left:.2f} s antes) | px={price:g} ({src}) qty={qty:g} ({eff:.2f} USDT) | trade #{pos.trade_id}")
        self._funding_schedule_close(pos)
        self._refine_entry(eng, pos)          # en papel: la entrada se corrige con el libro cuando llegue
        return pos

    def _funding_schedule_close(self, pos: Position) -> None:
        loop = self.loop
        if loop is None or not self._on_loop():
            return
        delay = pos.funding_ts + self.funding_cfg["close_after_s"] - self._srv_now()
        loop.call_later(max(delay, 0.0), self._funding_close, pos.trade_id)

    def _funding_close(self, trade_id: int) -> None:
        """Cierra tras el cobro, al precio en vivo (la orden sale ya; el libro, en paralelo)."""
        _eng, pos = self._find(trade_id)
        if pos is None:
            return
        if not self.store.can_act():
            if self._on_loop():
                self.loop.call_later(1.0, self._funding_close, trade_id)
            return
        price = self._live_price(pos.symbol) or pos.last_price or pos.entry_price
        self._close(trade_id, price, "FUNDING")

    def _funding_overdue(self) -> None:
        """Red de seguridad (p. ej. tras un reinicio): cierra las de funding cuyo cobro ya pasó."""
        late = self._srv_now() - self.funding_cfg["close_after_s"] - 3.0
        with self.lock:
            due = [t for t, p in self.engines[ENGINE_FUNDING].positions.items()
                   if p.funding_ts and p.funding_ts < late and t not in self._closing]
        for tid in due:
            self._funding_close(tid)

    def funding_snapshot(self, rows: bool = True) -> dict:
        c = dict(self.funding_cfg)
        with self.lock:
            top = [dict(r) for r in self._funding_rows[:150]] if rows else []
        now = self._srv_now()
        ts = self._funding_target_ts or next_funding_ts(now, c["open_before_s"])
        for r in top:
            r["next"] = _utc(r["next_ts"], "%H:%M") if r["next_ts"] else ""
            r["in_s"] = round(r["next_ts"] - now) if r["next_ts"] else None
            r["over"] = abs(r["rate_pct"]) >= c["min_pct"]
            r["due"] = bool(r["next_ts"]) and abs(r["next_ts"] - ts) <= 1.5   # cobra en el cobro que apunta el bot
        return {"cfg": c, "rows": top, "over": self._funding_over, "scans": self.funding_scans,
                "errors": self.funding_errors, "last_error": self.funding_last_error,
                "last_scan": _utc(self.funding_last_scan_ts, "%H:%M:%S") if self.funding_last_scan_ts else "",
                "scan_ms": round(self.funding_last_scan_ms), "clock_off_ms": round(self._clock_off() * 1000),
                "clock_src": self.funding_clock_info.get("source", ""),
                "clock_diff_ms": self.funding_clock_info.get("diff_ms"),
                "clock_agree": self.funding_clock_info.get("agree"),
                "clock_providers": self.funding_clock_info.get("providers", {}),
                "watch": dict(self._funding_watch), "scan_busy": self._scan_busy, "src": dict(self._funding_src),
                "clock_rtt_ms": round(self.funding_clock_rtt_ms),
                "clock_age_s": round(time.time() - self.funding_clock_ts) if self.funding_clock_ts else None,
                "loop_lag_ms": round(self.loop_lag_ms),
                "next": _utc(ts, "%H:%M"), "next_in_s": round(ts - now),
                "next_ts": ts, "srv_now": now,                      # para la cuenta regresiva del navegador
                "scan_in_s": round(ts - c["scan_before_s"] - now)}

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
                   "rev_parent", "rev_tp_usd", "rev_sl_usd", "funding_rate", "funding_ts")
    _CLOSED_FIELDS = ("symbol", "direction", "entry_price", "close_price", "move_pct", "pnl", "fees",
                      "mfe_pct", "mae_pct", "duration_s", "reason", "sent", "revenge", "funding_usd")
    _SIGNAL_FIELDS = ("time", "symbol", "direction", "candle", "open", "close", "body_pct",
                      "candle_age_s", "action")
    WEB_CLOSED_ROWS = 100
    WEB_SIGNAL_ROWS = 60
    # Partes pesadas del estado: la web pide solo las de la vista abierta (?view=).
    ALL_PARTS = frozenset({"signals", "closed", "exc", "eval", "ratio_rows", "events", "funding_rows"})
    VIEW_PARTS = {
        "resumen":     frozenset(),
        "operaciones": frozenset({"signals"}),
        "bots":        frozenset({"funding_rows"}),
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
                               if "eval" in parts and eng.name in KLINE_ENGINES else empty_eval),
        }

    def network_stats(self) -> dict:
        """Bytes recibidos/enviados desde el arranque, para vigilar el ancho de banda."""
        pc = self.price_cache
        ks = self.kline_stream.get_stats() if self.kline_stream else {}
        rx = {"rest": self.rx_rest_bytes + self.rx_alt_bytes, "ticker": self.rx_ticker_bytes,
              "prices": pc.rx_bytes if pc else 0, "klines": int(ks.get("rx_bytes") or 0)}
        up = max(time.time() - self.started_at, 1.0)
        total = sum(rx.values())
        return {"rx": rx, "rx_total": total, "web_tx": self.tx_web_bytes,
                "rx_per_hour": total / up * 3600, "web_per_hour": self.tx_web_bytes / up * 3600,
                "price_stream": pc.symbol_stream if pc else "", "ticker_mode": TICKER_MODE,
                "price_symbols": len(pc.symbols) if pc else 0,
                "kline_symbols": len(pc.kline_symbols) if pc else 0}

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
            "funding":         self.funding_snapshot("funding_rows" in parts),
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
    if eng.name == ENGINE_FUNDING:
        return jsonify({"ok": False, "error": "El bot de funding se ajusta en /api/funding"}), 400
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
    for key in ("watch_live_price", "fast_prices", "send_tp"):
        if key in data:
            vals[key] = _flag(data[key])
    if data.get("max_open") not in (None, ""):
        try:
            v = int(float(data["max_open"]))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "max_open inválido"}), 400
        if not 0 <= v <= 10_000:
            return jsonify({"ok": False, "error": "max_open debe estar entre 0 y 10000"}), 400
        vals["max_open"] = v
    for key, hi in (("profit_target_usd", 1_000_000), ("fee_pct", 1), ("min_hold_s", 60)):
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


@app.get("/api/indicators")
def api_indicators_get():
    return _no_store(jsonify({"ok": True, **bot.ind_snapshot()}))


@app.post("/api/indicators")
def api_indicators_set():
    """TP por RSI + VWAP ±kσ (UTC) de los bots de ratio, en caliente. Body JSON (todo opcional):
    {"engine": "ratio", "enabled": true} enciende/apaga el de un bot; parámetros comunes:
    {"rsi_period": 14, "rsi_level": 13, "vwap_k": 2.5, "use_rsi": true, "use_vwap": true}.
    SHORT cierra con RSI < nivel y cierre ≤ VWAP − kσ; LONG con RSI > 100 − nivel y cierre ≥ VWAP + kσ."""
    busy = _guard()
    if busy:
        return busy
    data = request.get_json(silent=True) or {}
    vals: dict = {}
    if "enabled" in data:
        name = data.get("engine")
        if name not in RATIO_ENGINES:
            return jsonify({"ok": False, "error": "engine debe ser ratio, ratio_tp o ratio_rev"}), 400
        vals["enabled"] = {name: _flag(data["enabled"])}
    for key, cast in (("rsi_period", int), ("rsi_level", float), ("vwap_k", float)):
        if data.get(key) in (None, ""):
            continue
        try:
            vals[key] = cast(float(data[key]))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": f"{key} inválido"}), 400
    for key in ("use_rsi", "use_vwap"):
        if key in data:
            vals[key] = _flag(data[key])
    if not vals:
        return jsonify({"ok": False, "error": "Nada que cambiar"}), 400
    try:
        return jsonify({"ok": True, **bot.set_ind_config(**vals)})
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


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


@app.get("/api/funding")
def api_funding_get():
    return _no_store(jsonify({"ok": True, **bot.funding_snapshot()}))


@app.post("/api/funding")
def api_funding_set():
    """Bot de funding alto, en caliente. Body JSON (todo opcional): {"min_pct": 0.3, "notional": 20,
    "open_before_s": 2, "close_after_s": 1, "scan_before_s": 180}. Pausar con /api/pause y bloquear
    LONG/SHORT con /api/block ({"engine": "funding", ...})."""
    busy = _guard()
    if busy:
        return busy
    data = request.get_json(silent=True) or {}
    vals = {}
    for key in KlineBot._FUNDING_LIMITS:
        if data.get(key) in (None, ""):
            continue
        try:
            vals[key] = float(data[key])
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": f"{key} inválido"}), 400
    if not vals:
        return jsonify({"ok": False, "error": "Nada que cambiar"}), 400
    try:
        cfg = bot.set_funding_config(**vals)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, **cfg})


@app.post("/api/funding/rescan")
def api_funding_rescan():
    """Explora ahora todos los pares del funding (botón de la web). No cambia ajustes ni abre operaciones."""
    busy = _guard()
    if busy:
        return busy
    res = bot.request_funding_rescan()
    return jsonify(res), (200 if res.get("ok") else 409)


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
    "revenge", "rev_parent", "rev_tp_usd", "rev_sl_usd", "signal_price", "entry_src", "trigger_price", "close_src",
    "funding_rate", "funding_ts", "funding_usd",
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
        <div class="box" style="margin-top:14px">
          <h4>Ahorro de datos <span class="muted" style="font-weight:400">· Render cobra el tráfico</span></h4>
          <div class="row c">
            <button type="button" data-act="watchLive" id="btnWatchLive">—</button>
            <button type="button" data-act="fastPx" id="btnFastPx">—</button>
            <button type="button" data-act="sendTp" id="btnSendTp">—</button>
          </div>
          <div class="note" id="ecoNote"></div>
        </div>
        <div class="box" style="margin-top:14px" id="indBox">
          <h4>TP por RSI + VWAP ±σ <span class="muted" style="font-weight:400">· velas 1m, VWAP del día UTC · también para las abiertas</span></h4>
          <div class="row c" id="indBots"></div>
          <form id="indForm" style="margin-top:10px">
            <label class="f">RSI, periodos <span id="indCurP"></span>
              <input id="indPeriod" type="number" step="1" min="2" max="100" placeholder="ej. 14"></label>
            <label class="f">Nivel RSI (SHORT &lt; nivel · LONG &gt; 100 − nivel) <span id="indCurL"></span>
              <input id="indLevel" type="number" step="0.1" min="0.1" max="49.9" placeholder="ej. 13"></label>
            <label class="f">Bandas VWAP, ±σ <span id="indCurK"></span>
              <input id="indK" type="number" step="0.1" min="0.1" max="10" placeholder="ej. 2.5"></label>
            <button type="submit">Guardar</button>
          </form>
          <div class="row c" style="margin-top:10px">
            <button type="button" class="small" data-act="indUse" data-key="use_rsi" id="indUseRsi">—</button>
            <button type="button" class="small" data-act="indUse" data-key="use_vwap" id="indUseVwap">—</button>
          </div>

          <div class="note" id="indNote"></div>
          <div class="scroll" id="indTblWrap" style="margin-top:10px" hidden><table><thead><tr><th>Par</th><th>Estado</th><th>Velas de</th><th>RSI</th><th>Cierre</th><th>VWAP −σ</th><th>VWAP</th><th>VWAP +σ</th><th>Velas</th><th>Última</th></tr></thead><tbody id="indTbl"></tbody></table></div>
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

    <details class="panel" data-key="b_funding" open>
      <summary>Bot funding alto <span class="hint" id="fdSum"></span><span class="right" id="fdState"></span></summary>
      <div class="body">
        <div class="cols2">
          <div class="box">
            <h4>Parámetros <span class="muted" style="font-weight:400">· se aplican desde el próximo cobro</span></h4>
            <form id="fdForm">
              <label class="f">Funding mínimo, ± % <span id="fdCurMin"></span>
                <input id="fdMin" type="number" step="0.01" min="0.01" max="10" placeholder="ej. 0.3"></label>
              <label class="f">Notional por entrada, USDT <span id="fdCurNot"></span>
                <input id="fdNot" type="number" step="1" min="5" max="100000" placeholder="ej. 20"></label>
              <label class="f">Abrir, segundos antes del cobro <span id="fdCurOpen"></span>
                <input id="fdOpen" type="number" step="0.1" min="__FD_OPEN_MIN__" max="60" placeholder="ej. 2"></label>
              <label class="f">Cerrar, segundos después del cobro <span id="fdCurClose"></span>
                <input id="fdClose" type="number" step="0.1" min="0" max="60" placeholder="ej. 1"></label>
              <label class="f">Consultar el funding, segundos antes del cobro <span id="fdCurScan"></span>
                <input id="fdScan" type="number" step="1" min="180" max="900" placeholder="ej. 180 (mínimo 180)"></label>
              <button type="submit">Guardar</button>
            </form>
            <h4 style="margin-top:16px">Direcciones y estado</h4><div class="row c" id="rb_funding"></div>
          </div>
          <div class="box">
            <h4>Cómo funciona</h4>
            <div class="note" style="margin-top:0" id="fdNote"></div>
            <div style="margin-top:10px"><button type="button" id="fdRescan">Reescanear todos los símbolos ahora</button>
              <span class="muted" id="fdRescanMsg" style="margin-left:8px"></span></div>
            <div class="scroll" style="margin-top:10px"><table><thead><tr><th>Par</th><th>Funding</th><th>Cobro (UTC)</th><th>Cobra en</th><th>Mark</th><th>Elegido</th></tr></thead><tbody id="fdTbl"></tbody></table></div>
          </div>
        </div>
        <div id="fdMsg" class="muted msg"></div>
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
          <label class="f">Demora mínima antes de cerrar, s (TP/SL, ratio, RSI/VWAP) <span id="curHold"></span>
            <input id="glbHold" type="number" step="0.5" min="0" max="60" placeholder="ej. 2"></label>
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
const ENGINES = ['ratio', 'ratio_tp', 'ratio_rev', 'funding', 'close', 'live'];
const RATIO = ['ratio', 'ratio_tp', 'ratio_rev'];
const SHORT_LABEL = {ratio: 'Ratio TP/SL', ratio_tp: 'Ratio solo TP', ratio_rev: 'Ratio venganza', funding: 'Funding', close: 'Al cierre', live: 'En vivo'};
const PANEL_OF = {ratio: 'b_ratio1', ratio_tp: 'b_ratio_tp', ratio_rev: 'b_ratio_rev', funding: 'b_funding', close: 'b_klines', live: 'b_klines'};
const hms = ts => ts ? new Date(ts * 1000).toISOString().slice(11, 19) : '—';
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
// Cuenta regresiva del funding: hora de Binance en el navegador = hora del navegador + srvSkew (se recalcula en cada
// respuesta con el punto medio de la petición) y se repinta cada 250 ms sin esperar al siguiente refresco.
let srvSkew = 0, fetchMid = 0;
const srvNow = () => Date.now() / 1000 + srvSkew;
const cdFmt = s => { s = Math.max(0, Math.ceil(s)); const h = Math.floor(s / 3600), m = Math.floor(s % 3600 / 60), x = s % 60;
                     return (h ? h + ':' + String(m).padStart(2, '0') : String(m)) + ':' + String(x).padStart(2, '0'); };
function tickCountdowns() {
  const now = srvNow();
  document.querySelectorAll('[data-cd]').forEach(el => {
    const left = (+el.dataset.cd) - (+el.dataset.off || 0) - now;
    const done = el.dataset.done || 'ya pasó';
    el.textContent = left > 0 ? cdFmt(left) : done;
    el.classList.toggle('neg', left > 0 && left <= 30);
  });
}
setInterval(tickCountdowns, 250);
function detailEngine() {
  if (view === 'mercado') return evEngine;
  return cur;
}
async function refresh() {
  clearTimeout(timer);
  if (inflight) { again = true; return; }
  inflight = true;
  try {
    const t0 = Date.now();
    const r = await fetch(`/api/status?view=${view}&engine=${encodeURIComponent(detailEngine())}`, {cache: 'no-store'});
    last = await r.json();
    fetchMid = (t0 + Date.now()) / 2;
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
            : p._eng === 'funding' ? `funding ${pct(p.funding_rate || 0)} · cobro ${hms(p.funding_ts)} UTC`
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
    const isF = n === 'funding', fc = (d.funding || {}).cfg || {};
    const off = (n === 'close' || n === 'live') && !k.running;
    const state = bc.paused ? ['off', 'PAUSADO'] : off ? ['idle', 'SIN VELAS'] : ['on', 'EN MARCHA'];
    return `<div class="bot ${bc.paused ? 'paused' : ''} ${off ? 'off' : ''}">
      <div class="top"><h3>${esc(b.label)}</h3><span class="st ${state[0]}">${state[1]}</span></div>
      <div class="hint">${esc(b.hint)}</div>
      <div class="kv">
        ${isF ? `<div>Funding<b>≥ ±${fc.min_pct}%</b></div><div>Entra / sale<b>−${fc.open_before_s} s / +${fc.close_after_s} s · ${fc.notional} USDT</b></div>`
        : isR ? `<div>Salida<b>${n === 'ratio_tp' ? 'solo TP ratio' : tpsl(xc)}</b></div><div>${n === 'ratio_rev' ? 'Venganza' : 'Ratio ↓ SHORT / ↑ LONG'}<b>${n === 'ratio_rev' ? ((r.rev || {}).revenge ? `ON ×${r.rev.rev_mult} · ${r.revenge_count || 0} disparadas` : 'OFF') : rc.drop_min_pct + '–' + rc.drop_max_pct + '%'}</b></div>`
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
    const isF = row._eng === 'funding';
    const exits = isF ? `<span class="muted">sale al cobro +${((d.funding || {}).cfg || {}).close_after_s} s</span>` : isR ? [row.tp_price > 0 ? `<span class="pos">TP</span> ${num(row.tp_price, 8)}` : '', row.sl_price > 0 ? `<span class="neg">SL</span> ${num(row.sl_price, 8)}` : '',
                         !row.revenge && exitOf(d, row._eng).ratio_exit ? `<span class="muted">ratio ${row.direction === 'SHORT' ? '≥' : '≤'}</span> ${num(row.ratio_target || row.ratio_trigger, 4)}${rn[row.symbol] && rn[row.symbol].last != null ? ` <span class="muted">(ahora ${num(rn[row.symbol].last, 4)})</span>` : ''}` : ''].filter(Boolean).join(' · ')
                      : `<span class="muted">${row.inverted ? 'SL' : 'TP'}</span> ${num(row.tp_price, 8)}`;
    return `<tr><td><b>${esc(row.symbol)}</b> <span class="muted">#${row.trade_id}</span>${row.sent === false ? ' <span class="st off">SIMULADA</span>' : ''}</td>
      <td>${esc(SHORT_LABEL[row._eng])}</td><td>${tag(row.direction)}${row.revenge ? ' <span class="tag REV">VENGANZA</span>' : ''}</td>
      <td>${row.revenge ? `venganza de #${row.rev_parent}` : isF ? `funding ${pct(row.funding_rate || 0)} · ${hms(row.funding_ts)}` : isR ? `${num(row.ratio_trigger, 4)}→${num(row.ratio_entry, 4)} ${pct(row.ratio_change_pct || 0)}` : pct(row.signal_pct)}</td>
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
  const isFund = cur === 'funding';
  q('thSigC').textContent = isFund ? 'Cobro · mark' : isRatio ? 'Ratio' : 'Vela';
  q('thSigB').textContent = isFund ? 'Funding' : isRatio ? 'Cambio' : 'Cuerpo';
  q('thSigS').textContent = isRatio || isFund ? '' : 'Segundo';
  if (view === 'operaciones') q('tSig').innerHTML = e.signals.map(s => `<tr><td>${esc(s.time)}</td><td>${esc(s.symbol)}</td><td>${tag(s.direction)}</td>
    <td>${esc(s.candle)} <span class="muted">${num(s.open, 8)}→${num(s.close, 8)}</span></td><td>${pct(s.body_pct)}</td>
    <td class="muted">${isRatio || isFund ? '' : Math.round(s.candle_age_s || 0) + 's'}</td>
    <td class="${/^(venganza )?abierta/.test(s.action) ? 'pos' : 'muted'}">${esc(s.action)}</td></tr>`).join('')
    || `<tr><td colspan="7" class="muted">${isFund ? 'Esperando un funding ≥ ±' + (((d.funding || {}).cfg || {}).min_pct) + '% antes de un cobro' : isRatio ? 'Esperando una caída (SHORT) o subida (LONG) del ratio dentro del rango' : (k.running ? 'Esperando velas con cuerpo ≥ ±' + ec.threshold_pct + '%' : 'Las velas están apagadas (Bots → Encender velas)')}</td></tr>`;

  // ── Bots ──
  q('rtSum').textContent = `24 h ≥ ±${rc.watch_min_pct}% · ratio ↓ SHORT / ↑ LONG ${rc.drop_min_pct}–${rc.drop_max_pct}%`;
  const stateOf = n => `<span class="st ${E[n].config.paused ? 'off' : 'on'}">${E[n].config.paused ? 'PAUSADO' : 'EN MARCHA'}</span>`;
  q('r1State').innerHTML = stateOf('ratio'); q('r2State').innerHTML = stateOf('ratio_tp'); q('r3State').innerHTML = stateOf('ratio_rev');
  q('r1Sum').textContent = tpsl(rc);
  const dirBtns = n => { const bc = E[n].config;
    return ['SHORT', 'LONG'].map(dir => { const bl = dir === 'LONG' ? bc.block_long : bc.block_short;
      return `<button type="button" class="small ${bl ? 'danger' : 'ok'}" data-block="${dir}" data-eng="${n}" data-on="${bl ? '1' : ''}">${dir} ${bl ? 'desactivado · activar' : 'activo · desactivar'}</button>`; }).join('')
      + `<button type="button" class="small ${bc.paused ? 'ok' : 'warnb'}" data-pause="${n}">${bc.paused ? 'Reanudar bot' : 'Pausar bot'}</button>`; };
  for (const n of [...RATIO, 'funding']) q('rb_' + n).innerHTML = dirBtns(n);
  const fd = d.funding || {}, fdc = fd.cfg || {};
  if (fd.srv_now && fetchMid) srvSkew = fd.srv_now - fetchMid / 1000;
  q('fdState').innerHTML = stateOf('funding');
  q('fdSum').textContent = `≥ ±${fdc.min_pct}% · ${fdc.notional} USDT · −${fdc.open_before_s} s / +${fdc.close_after_s} s`;
  q('fdCurMin').textContent = `(ahora ±${fdc.min_pct}%)`; q('fdCurNot').textContent = `(ahora ${fdc.notional})`;
  q('fdCurOpen').textContent = `(ahora ${fdc.open_before_s} s)`; q('fdCurClose').textContent = `(ahora ${fdc.close_after_s} s)`;
  q('fdCurScan').textContent = `(ahora ${fdc.scan_before_s} s)`;
  q('fdNote').innerHTML = `Antes de cada cobro (horas en punto UTC) consulta el funding de <b>todos</b> los pares ${fdc.scan_before_s} s antes (WebSocket, sin REST) y sigue en vivo hasta abrir los que están cerca del umbral: si uno lo cruza entra, si baja no se abre. `
    + `Si un par tiene funding ≥ +${fdc.min_pct}% abre <b>SHORT</b> (los LONG le pagan); si es ≤ −${fdc.min_pct}%, <b>LONG</b>. Entra ${fdc.open_before_s} s antes del cobro, `
    + `manda la orden al executor al instante con el precio en vivo (la entrada en papel se corrige después con el libro) y cierra ${fdc.close_after_s} s después. El PnL suma el funding cobrado. `
    + `Próximo cobro ${esc(fd.next || '—')} UTC (en <b data-cd="${fd.next_ts || 0}" data-off="0" data-done="cobrando">${cdFmt(fd.next_in_s || 0)}</b>) · última consulta ${esc(fd.last_scan || '—')}${fd.scan_ms ? ' (' + fd.scan_ms + ' ms)' : ''}${fd.src && fd.src.total ? ': ' + fd.src.total + ' pares (WebSocket ' + fd.src.ws + ' · REST ' + fd.src.rest + ')' : ''} · `
    + `${fd.over || 0} par(es) ≥ umbral · reloj Binance ${fd.clock_off_ms >= 0 ? '+' : ''}${fd.clock_off_ms || 0} ms${fd.clock_age_s == null ? ' <span class="neg">(sin sincronizar)</span>' : ' (medido hace ' + dur(fd.clock_age_s) + ', latencia ' + (fd.clock_rtt_ms || 0) + ' ms)'}${fd.clock_diff_ms != null ? ' · vs proveedores ' + (fd.clock_diff_ms >= 0 ? '+' : '') + fd.clock_diff_ms + ' ms ' + (fd.clock_agree ? '✓' : '<span class=\"neg\">⚠ difieren</span>') : (fd.clock_age_s != null ? ' · sin proveedores externos' : '')}${fd.watch && fd.watch.ts ? ' · exploración horaria: ' + fd.watch.scanned + ' pares en ' + fd.watch.elapsed_s + ' s, ' + fd.watch.n + ' ≥ umbral' : ''}${fd.loop_lag_ms > 500 ? ` · <span class="neg">loop con retraso ${fd.loop_lag_ms} ms</span>` : ''}${fd.last_error ? ` · <span class="neg">${esc(fd.last_error)}</span>` : ''}`;
  if (view === 'bots') q('fdTbl').innerHTML = (fd.rows || []).map(z => {
      const cobro = z.next_ts ? `<b data-cd="${z.next_ts}" data-off="0" data-done="cobrado">${cdFmt(z.in_s || 0)}</b>` : '—';
      const pick = z.over && z.due;
      const abre = pick ? ` <span class="muted">abre en <span data-cd="${z.next_ts}" data-off="${fdc.open_before_s || 0}" data-done="abriendo">${cdFmt((z.in_s || 0) - (fdc.open_before_s || 0))}</span></span>` : '';
      return `<tr><td><b>${esc(z.symbol)}</b></td><td class="${z.over ? (z.rate_pct > 0 ? 'neg' : 'pos') : 'muted'}">${z.rate_pct > 0 ? '+' : ''}${(+z.rate_pct).toFixed(4)}%</td>
      <td>${esc(z.next)}</td><td>${cobro}</td><td>${num(z.mark, 8)}</td><td>${pick ? tag(z.rate_pct > 0 ? 'SHORT' : 'LONG') + abre : z.over ? '<span class="muted">funding alto, pero cobra más tarde</span>' : '<span class="muted">no cumple</span>'}</td></tr>`;
    }).join('')
    || '<tr><td colspan="6" class="muted">Aún no hay consulta del funding</td></tr>';
  tickCountdowns();
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

  const bw = q('btnWatchLive'), bf = q('btnFastPx');
  bw.dataset.on = g.watch_live_price ? '1' : ''; bf.dataset.on = g.fast_prices ? '1' : '';
  bw.textContent = g.watch_live_price ? 'Vigilados: precio en vivo (3 s) · apagar' : 'Vigilados: sin precio en vivo · encender';
  bf.textContent = g.fast_prices ? 'Abiertas: precio rápido (~100 ms) · pasar a 1 s' : 'Abiertas: precio cada 1 s · pasar a rápido';
  bw.className = 'small ' + (g.watch_live_price ? 'ok' : 'ghost'); bf.className = 'small ' + (g.fast_prices ? 'ok' : 'ghost');
  const bt = q('btnSendTp');
  bt.dataset.on = g.send_tp ? '1' : '';
  bt.textContent = g.send_tp ? 'TP al executor con cada apertura · apagar' : 'TP al executor apagado · encender';
  bt.className = 'small ' + (g.send_tp ? 'ok' : 'ghost');
  q('ecoNote').textContent = `Vigilados ahora: ${r.watch_count || 0}. Con su precio en vivo apagado, su 24 h se lee en la exploración de cada 5 min y el precio para entrar se pide solo cuando el ratio da señal. Con las abiertas a 1 s, el TP que cada posición lleva puesto en Binance cubre los picos.`;
  const ind = r.ind;
  if (ind) {
    const ic = ind.cfg;
    q('indBots').innerHTML = RATIO.map(n => { const on = !!ic.enabled[n];
      return `<button type="button" class="small ${on ? 'ok' : 'ghost'}" data-act="indBot" data-eng="${n}" data-on="${on ? '1' : ''}">${esc(SHORT_LABEL[n])}: ${on ? 'ON · apagar' : 'OFF · encender'}</button>`; }).join('');
    q('indCurP').textContent = `(ahora ${ic.rsi_period})`;
    q('indCurL').textContent = `(ahora ${ic.rsi_level} → LONG > ${+(100 - ic.rsi_level).toFixed(2)})`;
    q('indCurK').textContent = `(ahora ±${ic.vwap_k}σ)`;
    for (const [id, key, label] of [['indUseRsi', 'use_rsi', 'RSI'], ['indUseVwap', 'use_vwap', 'VWAP']]) {
      const bt = q(id), on = !!ic[key];
      bt.dataset.on = on ? '1' : ''; bt.className = 'small ' + (on ? 'ok' : 'ghost');
      bt.textContent = `${label}: ${on ? 'se exige · ignorar' : 'ignorado · exigir'}`;
    }
    const cond = [ic.use_rsi ? `RSI(${ic.rsi_period}) < ${ic.rsi_level}` : '', ic.use_vwap ? `cierre ≤ VWAP − ${ic.vwap_k}σ` : ''].filter(Boolean).join(' y ');
    const condL = [ic.use_rsi ? `RSI > ${+(100 - ic.rsi_level).toFixed(2)}` : '', ic.use_vwap ? `cierre ≥ VWAP + ${ic.vwap_k}σ` : ''].filter(Boolean).join(' y ');
    q('indNote').innerHTML = `Cierra el <b>SHORT</b> cuando una vela 1m cierra con ${cond}; el <b>LONG</b> con ${condL}. Solo cuentan las velas cerradas después de abrir; las venganzas no lo usan. `
      + `Al abrir se piden por REST <b>una vez</b> ${ind.next_seed} velas (las de hoy desde las 00:00 UTC, mínimo ${10 * ic.rsi_period} para el RSI) a ${esc((ind.exchanges || []).join(' → '))} (el primero que tenga el par; Binance solo como último recurso) y luego se sigue vela a vela con una petición pequeña por minuto a ese mismo exchange (o por WebSocket si vinieron de Binance). Solo los pares con posición. Cargas: ${ind.seeds} · TP disparados: ${ind.hits}.`;
    const rows = Object.entries(ind.symbols || {});
    q('indTblWrap').hidden = !rows.length;
    q('indTbl').innerHTML = rows.map(([sym, v]) => `<tr><td>${esc(sym)}</td><td>${esc(v.state)}${v.error ? ' <span class="neg">' + esc(v.error) + '</span>' : ''}</td><td>${esc(v.src || '—')}</td>
      <td>${v.rsi != null ? num(v.rsi, 1) : '—'}</td><td>${v.close ? num(v.close, 8) : '—'}</td><td>${v.lo != null ? num(v.lo, 8) : '—'}</td>
      <td>${v.vwap != null ? num(v.vwap, 8) : '—'}</td><td>${v.hi != null ? num(v.hi, 8) : '—'}</td><td>${v.candles != null ? v.candles + ' (' + v.today + ' hoy)' : '—'}</td><td>${esc(v.last || '')}</td></tr>`).join('');
  }
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
      + (r.rest_blocked_s > 0 ? ` · <span class="neg">ratio en pausa ${dur(r.rest_blocked_s)} (418/429 en todas las rutas)</span>` : '')
      + ((r.routes || []).length ? ` · REST Binance: ${esc((r.routes.find(x => x.current) || r.routes[0]).route)}${(r.routes.find(x => x.current) || {}).weight != null ? ` (peso ${(r.routes.find(x => x.current)).weight}/min)` : ''}, ${r.routes.filter(x => !x.blocked_s).length}/${r.routes.length} rutas libres`
        + (r.routes[0] && r.routes[0].blocked_s > 0 ? ` · <span class="neg">IP pública vetada por Binance, se reintenta en ${dur(r.routes[0].blocked_s)}</span>` : '') : '')
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
      <td>${dur(t.duration_s || 0)}</td><td>${esc(t.reason)}${t.funding_usd ? ` <span class="muted">funding ${usd(t.funding_usd)}</span>` : ''}${t.sent === false ? ' <span class="st off">SIMULADA</span>' : ''}</td></tr>`).join('')
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
  q('curHold').textContent = `(ahora ${g.min_hold_s} s)`;
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
      ['REST (ratio)', bytes(nt.rx.rest)],
      ['Web enviada', bytes(nt.web_tx)], ['Web por hora', bytes(nt.web_per_hour)],
    ].map(([a, b]) => `<div>${a}<b>${b}</b></div>`).join('');
  }
  if (view === 'sistema') q('events').textContent = d.events.join('\n');
}

// ── Acciones ──
const glbText = r => `objetivo ${r.profit_target_usd > 0 ? '+' + r.profit_target_usd + ' USD' : 'apagado'} · límite ${r.max_open > 0 ? r.max_open + ' abiertas' : 'sin límite'} · fee ${r.fee_pct}% por lado · demora mínima ${r.min_hold_s} s`;
const indText = r => { const c = r.cfg; return `TP RSI/VWAP: ${RATIO.filter(n => c.enabled[n]).map(n => SHORT_LABEL[n]).join(', ') || 'ningún bot'} · `
  + [c.use_rsi ? `RSI(${c.rsi_period}) < ${c.rsi_level} / > ${+(100 - c.rsi_level).toFixed(2)}` : '', c.use_vwap ? `VWAP ±${c.vwap_k}σ` : ''].filter(Boolean).join(' y '); };
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
  watchLive(b) { const on = !b.dataset.on; glb({watch_live_price: on}, '/api/global', 'rtMsg', r => r.watch_live_price ? 'Precio en vivo de los vigilados encendido (cada 3 s).' : 'Precio en vivo de los vigilados apagado: menos tráfico.'); },
  sendTp(b) {
    const on = !b.dataset.on;
    if (confirm(on ? '¿Mandar el TP con cada apertura (todos los bots)? Las abiertas reciben su TP en Binance ahora.'
                   : '¿Dejar de mandar el TP con las aperturas (todos los bots)? Se quita el TP en Binance de las abiertas; el bot sigue cerrando por su TP en papel.'))
      glb({send_tp: on}, '/api/global', 'rtMsg', r => r.send_tp ? 'TP con las aperturas encendido.' : 'TP con las aperturas apagado.');
  },
  fastPx(b) { const on = !b.dataset.on; glb({fast_prices: on}, '/api/global', 'rtMsg', r => r.fast_prices ? 'Abiertas con precio rápido (~100 ms).' : 'Abiertas con precio cada 1 s: menos tráfico.'); },
  indBot(b) {
    const on = !b.dataset.on;
    glb({engine: b.dataset.eng, enabled: on}, '/api/indicators', 'rtMsg', indText);
  },
  indUse(b) { glb({[b.dataset.key]: !b.dataset.on}, '/api/indicators', 'rtMsg', indText); },
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
q('fdRescan').addEventListener('click', async () => {
  const btn = q('fdRescan'), msg = q('fdRescanMsg');
  btn.disabled = true; msg.textContent = 'Explorando todos los pares…';
  try {
    const r = await post('/api/funding/rescan', {});
    msg.innerHTML = r.ok ? `<span class="pos">Explorando</span> (hasta ${r.max_symbols} pares y ${r.max_s} s)` : `<span class="neg">${esc(r.error)}</span>`;
    if (r.ok) { setTimeout(refresh, 2500); setTimeout(refresh, 6000); setTimeout(() => { msg.textContent = ''; }, 12000); }
  } catch (e) { msg.innerHTML = `<span class="neg">${esc(e)}</span>`; }
  finally { setTimeout(() => { btn.disabled = false; }, 3000); }
});
q('fdForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  const ids = {min_pct: 'fdMin', notional: 'fdNot', open_before_s: 'fdOpen', close_after_s: 'fdClose', scan_before_s: 'fdScan'};
  const body = {}; for (const [k, id] of Object.entries(ids)) if (q(id).value !== '') body[k] = q(id).value;
  if (await glb(body, '/api/funding', 'fdMsg', r => `funding ≥ ±${r.min_pct}% · ${r.notional} USDT · abre ${r.open_before_s} s antes y cierra ${r.close_after_s} s después · consulta ${r.scan_before_s} s antes`))
    for (const id of Object.values(ids)) q(id).value = '';
});
q('indForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  if (await glb({rsi_period: q('indPeriod').value, rsi_level: q('indLevel').value, vwap_k: q('indK').value}, '/api/indicators', 'rtMsg', indText)) {
    q('indPeriod').value = ''; q('indLevel').value = ''; q('indK').value = '';
  }
});
q('glbForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  if (await glb({profit_target_usd: q('glbTarget').value, max_open: q('glbMax').value, fee_pct: q('glbFee').value, min_hold_s: q('glbHold').value})) {
    q('glbTarget').value = ''; q('glbMax').value = ''; q('glbFee').value = ''; q('glbHold').value = '';
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

PAGE = (HTML.replace("__POLL_MS__", str(STATUS_POLL_MS))
        .replace("__FD_OPEN_MIN__", f"{FUNDING_OPEN_MIN_S:g}")).encode("utf-8")
PAGE_ETAG = hashlib.sha1(PAGE).hexdigest()[:16]


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), threaded=True)

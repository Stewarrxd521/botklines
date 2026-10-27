# BOTKLINE · bot de ratio de top traders (Binance USDⓈ-M Futures)

Servicio web para Render que vigila los perpetuos USDT que más se mueven en 24 h, consulta el *Top Trader Long/Short Ratio* y manda señales de apertura y cierre al **Executor** externo, que es quien abre las órdenes en Binance. En el mismo proceso viven además dos bots de velas 1m, apagados por defecto (`KLINE_BOTS_ENABLED=false`).

## Estructura

| Archivo | Qué hace |
| --- | --- |
| `app.py` | Todo el bot: estrategia (reglas puras, con pruebas en `tests/`), bots, API Flask y panel web (tema claro / oscuro / automático). |
| `WS.py` | Precio en vivo por WebSocket, solo de los símbolos con posición abierta. |
| `KlineWebSocketCache_v4.py` | Caché de velas (versión anterior, no la usa `app.py`). |
| `kline_ws.py` | Velas 1m por WebSocket (solo si los bots de velas están activos). |
| `state_store.py` | Copia en disco de posiciones e historial para recuperarse tras un reinicio. |

## Bot de ratio de top traders

1. **Exploración por WebSocket**: en cada ciclo de 5 min se abre `!miniTicker@arr` durante 30 s (`TICKER_SCAN_S`) y se lee el 24 h de todo el mercado. La misma exploración descubre los símbolos: una vez al día la lista se rehace (salen los deslistados) y entre medias se añaden los nuevos; se guarda en `KNOWN_SYMBOLS_FILE`.
   **Vigilados**: los perpetuos con variación 24 h ≥ +20 % (`RATIO_WATCH_MIN_PCT`, editable en la web). Con LONG activo, también los ≤ −20 %.
2. **Entrada**: cada 5 min se consulta el ratio de cuentas de los top traders de cada vigilado. Si cae entre 25 % y 30 % de un punto de 5 min al siguiente → **SHORT** (LONG es el espejo: el ratio sube en un perdedor).
3. **Salida** (lo que llegue antes):
   - **Take profit por precio**: **+10 %** a favor desde la entrada (`RATIO_TP_PCT`).
   - **Stop loss por precio**: **−5 %** en contra desde la entrada (`RATIO_SL_PCT`).
   - **TP por ratio** (desactivable): el ratio recupera el mismo % que cayó.

El TP, el SL y la salida por ratio se cambian **en caliente** desde el bloque *Ratio top traders → Salida de las operaciones* de la web, o con:

```bash
curl -X POST https://<bot>/api/ratio -H 'Content-Type: application/json' \
     -d '{"tp_pct": 10, "sl_pct": -5, "ratio_exit": true}'
```

`0` apaga el TP o el SL; el SL se puede mandar como `5` o `-5`. Los niveles nuevos se aplican también a las posiciones abiertas del bot de ratio (se recalculan desde su precio de entrada) y se guardan en `SETTINGS_FILE`. El cierre sale con motivo `TP`, `SL` o `TP_RATIO`.

## Ancho de banda

El tráfico de red venía casi todo de streams de *todo el mercado* abiertos las 24 h y del panel pidiendo el estado completo cada 1,5 s. Ahora:

| Antes | Ahora |
| --- | --- |
| `!markPrice@arr@1s` + `!ticker@arr` en `WS.py` (~250 KB/s siempre) | Apagados por defecto (`WS_ALL_MARKET`, `WS_ALL_TICKER`). |
| `!miniTicker@arr` abierto siempre para el 24 h (~80 KB/s) | Exploración de 30 s por WebSocket en cada ciclo de 5 min (`TICKER_MODE=scan`, ~10 % del tráfico; `ws` = abierto siempre). |
| `bookTicker` de cada vigilado, sin darlo de baja nunca | Precio en vivo **solo** de los símbolos con posición, con `markPrice@1s` (1 msg/s); se da de baja al cerrar. Los bots de velas usan `bookTicker`. |
| REST sin comprimir (`exchangeInfo` ≈ 1,5 MB en cada arranque) | Solo queda REST para el ratio (no existe por WebSocket) y para los filtros de cantidad de `exchangeInfo` (al arrancar si no hay caché, y luego cada 10 días), todo con gzip. |
| Panel: estado completo de los 3 bots cada 1,5 s, sin comprimir | Cada 3 s, gzip, solo el detalle del bot que se ve, y pausa con la pestaña oculta. La página tiene ETag (recarga = 304). |
| Executor: `GET /api/state` cada 20 s por hilo | `HEAD` (sin cuerpo). |

El bloque *Red* de la web (y `GET /health`) muestra los bytes recibidos y enviados desde el arranque y el ritmo por hora.

## Bots de velas 1m (opcionales)

Se activan con `KLINE_BOTS_ENABLED=true`.

### Dos bots en paralelo

En el mismo proceso y la misma web corren dos bots con la misma estrategia y distinto momento de entrada:

| Bot | Entra | Nombre en la API |
| --- | --- | --- |
| **Bot al cierre** | cuando la vela 1m **cierra** con cuerpo ≥ umbral | `close` |
| **Bot en vivo** | en cuanto la vela 1m **en curso** supera el umbral, sin esperar al cierre (Binance manda la vela abierta cada ~250 ms) | `live` |

Cada bot tiene su umbral, su TP, sus bloqueos LONG/SHORT, sus posiciones, sus señales, su MFE/MAE y su historial. Comparten los WebSockets, el executor y la web. El bot en vivo entra una sola vez por símbolo y vela; si el umbral se cruza justo en el último tick, entra con el cierre. Como el bot en vivo parsea todos los mensajes de velas (no solo los cierres), usa algo más de CPU.

En la web, las pestañas *Bot al cierre* / *Bot en vivo* eligen qué bot se ve y se configura; la tabla *Los dos bots* los compara. El executor y los eventos son comunes.

### Estrategia de velas

1. **Velas por WebSocket** (`kline_ws.py`): una conexión con `<símbolo>@kline_1m` de todos los perpetuos. Se guardan las dos últimas velas cerradas de cada símbolo. Sin REST.
2. **Señal**: se mide el cuerpo de la vela, `(cierre o último precio − apertura) / apertura` (al cierre para el bot al cierre; en cada actualización para el bot en vivo).
   - Sube **≥ 3 %** → abre **LONG**.
   - Baja **≥ 3 %** → abre **SHORT**.
3. **Tamaño**: notional fijo de **5 USDT**. La cantidad se redondea hacia arriba al `stepSize` del par para no quedar por debajo del mínimo de Binance. Los pares que exigen más de 5 USDT, o cuyo `stepSize` obliga a más de 1,5× el notional, se saltan y quedan anotados en la tabla de señales.
4. **Varias posiciones por símbolo**: cada señal abre su propia posición aunque ya haya otras abiertas en ese par, en la misma dirección o en la contraria (cada una con su `trade_id`, su TP y su cierre). Solo se evita repetir la señal de una misma vela.
5. **Salida**: se cierra cuando el precio avanza **2 % o más** a favor desde la entrada (precio en tiempo real de `WS.py`: bookTicker del símbolo + `!markPrice@arr`). **No hay stop loss**; solo cierre manual desde la web.

Las cifras se configuran con variables de entorno (tabla abajo). El **% de la señal** y el **% del TP** de cada bot también se cambian en caliente desde el bloque *Parámetros* de la web o con `POST /api/config` (`{"engine": "live", "threshold_pct": 3, "tp_pct": 2}`; sin `engine` se cambia el bot al cierre). El umbral aplica desde la siguiente señal; el TP nuevo también se aplica a las posiciones abiertas de ese bot. Los valores se guardan en `SETTINGS_FILE` (los ajustes de la versión anterior pasan al bot al cierre).

### Modo invertido (botón, para los dos bots)

En el bloque *Modo y objetivo* de la web hay un botón **Invertir** (o `POST /api/global` con `{"inverted": true}`):

| | Modo normal | Modo invertido |
| --- | --- | --- |
| Vela verde ≥ umbral | LONG | **SHORT** |
| Vela roja ≥ umbral | SHORT | **LONG** |
| El % de *TP* de cada bot | take profit | **stop loss** |
| Take profit | sí | **no** |

El cambio aplica a las entradas nuevas de los dos bots. Cada posición guarda el modo con el que se abrió: las abiertas en modo normal siguen con su TP, y las invertidas con su SL, aunque se vuelva a cambiar. Al cerrar por SL el motivo es `SL`.

### Objetivo global de ganancia

Si **PnL realizado + PnL abierto** (de los dos bots, netos de fees, contados desde el último objetivo) llega a **+12 USD**, se cierran **todas** las posiciones de los dos bots (motivo `OBJETIVO`) y el contador vuelve a cero. El objetivo se cambia en la web o con `POST /api/global` (`{"profit_target_usd": 12}`; `0` lo apaga). El botón *Poner a cero* (o `POST /api/session/reset`) reinicia el contador a mano.

### Fees

Todo el PnL (abierto, realizado, el enviado al executor y el del objetivo) es **neto**: resta la comisión de entrada y la de salida, `FEE_PCT` % del notional en cada lado (0,05 %, taker de Binance). Se cambia en la web o con `POST /api/global` (`{"fee_pct": 0.05}`). El historial guarda `pnl` (neto), `pnl_gross` y `fees`.

### Pausar un bot, cerrar todo y límite de abiertas

- **Pausar**: cada bot tiene su botón *Pausar / Reanudar* en el bloque *Control* (o `POST /api/pause` con `{"engine": "live", "paused": true}`). Un bot pausado no abre entradas nuevas (sus señales salen como *pausado*); sus posiciones abiertas siguen hasta su TP/SL.
- **Cerrar todo**: *Cerrar TODAS las posiciones* cierra a mercado las de los dos bots, y *Cerrar todas las de este bot* las del bot elegido (`POST /api/close-all`, con `{"engine": "close"}` para un solo bot).
- **Límite de abiertas**: máximo de posiciones abiertas a la vez sumando los dos bots, editable en caliente (`POST /api/global` con `{"max_open": 10}`; `0` = sin límite). Al llegar al límite las señales salen como *ignorada: límite de N abiertas*.

- **Pausar entradas al executor (modo prueba)**: con el botón *Pausar entradas al executor* (o `POST /api/global` con `{"executor_paused": true}`) los bots siguen operando igual pero en simulación: las entradas nuevas aparecen como *SIMULADA* y no se manda nada al executor, ni la apertura ni su cierre. Las posiciones que ya se habían mandado siguen y su cierre sí se envía. Al reanudar, las simuladas que sigan abiertas tampoco se mandan.

La web agrupa todo en secciones plegables; recuerda cuáles dejaste abiertas y qué bot estabas viendo.

### Bloquear longs o shorts

En el bloque *Parámetros* de cada bot hay un botón por dirección (o `POST /api/block` con `{"engine": "close", "direction": "LONG", "blocked": true}`). Solo afecta a ese bot. Una dirección bloqueada no abre entradas nuevas; sus señales aparecen como *bloqueada* en la tabla de señales. Las posiciones ya abiertas siguen hasta su TP o cierre manual. El estado se guarda en `SETTINGS_FILE`.

### MFE / MAE

Cada posición registra su máximo a favor (**MFE**) y su máximo en contra (**MAE**), en % y en USDT, con el momento en que se marcaron y el precio máximo y mínimo vistos. La web los muestra en las posiciones abiertas y en las cerradas, más un resumen (medias, mejor MFE, peor MAE, tiempo hasta el MAE) total y por dirección. `GET /api/trades.csv?engine=close|live` descarga el historial de un bot con estas columnas (sin `engine`, los dos, con la columna `strategy`).

### Evaluaciones

La web muestra todas las velas evaluadas, no solo las que generan señal:
- **Mayores cuerpos** de la última vela 1m cerrada (los `EVAL_TOP_N` primeros, por defecto 15), con la señal que daría con el umbral actual.
- **Resumen por minuto** (últimos `EVAL_MINUTES_KEPT`, por defecto 30): velas evaluadas, cuántas pasaron el umbral en cada dirección y la mayor subida y caída.

Se cuentan con el umbral del bot seleccionado y miden el cuerpo al cierre.

## Executor

Cada apertura y cierre se manda con `POST <EXECUTOR_URL>/signal` y la cabecera `X-Signal-Secret`, con el mismo formato de antes:

```json
{"action": "open",  "trade_id": 1791908572, "symbol": "FOOUSDT", "direction": "LONG",
 "price": 1.04, "quantity": 5, "notional": 5.2, "level": 4.0, "strategy": "live"}
{"action": "close", "trade_id": 1791908572, "symbol": "FOOUSDT", "direction": "LONG",
 "reason": "TP", "close_price": 1.0609, "pnl": 0.1045, "strategy": "live"}
```

`direction` puede ser `LONG` o `SHORT`, `level` lleva el % del cuerpo de la vela que disparó la señal y `strategy` dice qué bot la mandó (`close` o `live`). Los `trade_id` son únicos entre los dos bots, así que el executor puede tener a la vez una posición de cada bot en el mismo par.

### Latencia

Las señales salen por conexiones HTTP persistentes (keep-alive): `EXECUTOR_WORKERS` hilos (4 por defecto) mantienen abierta su conexión con el executor y la refrescan con un `HEAD /api/state` cada `EXECUTOR_WARM_S` segundos (20) si no hay señales. Así cada señal paga solo el viaje de red, sin abrir TCP+TLS de nuevo. La entrada usa el último precio de la vela. La web muestra los ms de decisión de cada bot y, para la última señal, los ms del HTTP al executor y el total desde que llegó el mensaje de Binance.

### Cambiar el link en caliente

- **Web**: bloque *Executor* → pegar el link nuevo (y el secreto si cambia) → *Cambiar link*. La web avisa si el executor nuevo responde.
- **API**:

  ```bash
  curl -X POST https://<bot>/api/executor -H 'Content-Type: application/json' \
       -d '{"url": "https://nuevo-executor.onrender.com", "secret": "opcional"}'
  ```

  `url` vacío deja de mandar señales. Si `ADMIN_TOKEN` está definido, añade `-H 'X-Admin-Token: <token>'`.

El cambio aplica a la siguiente señal, sin reiniciar, y se guarda en disco local (`SETTINGS_FILE`). En Render free ese disco se borra al reiniciar, así que tras un redeploy vuelve a `EXECUTOR_URL`. Los cierres de posiciones ya abiertas van al link vigente en el momento del cierre.

## Variables de entorno

| Variable | Default | Descripción |
| --- | --- | --- |
| `SIGNAL_THRESHOLD_PCT` | `3` | Bot al cierre: cuerpo mínimo de la vela 1m para abrir. |
| `TAKE_PROFIT_PCT` | `2` | Bot al cierre: avance a favor para cerrar. |
| `LIVE_SIGNAL_THRESHOLD_PCT` | = `SIGNAL_THRESHOLD_PCT` | Bot en vivo: cuerpo de la vela en curso para abrir. |
| `LIVE_TAKE_PROFIT_PCT` | = `TAKE_PROFIT_PCT` | Bot en vivo: avance a favor para cerrar. |
| `NOTIONAL_USDT` | `5` | USDT por entrada. |
| `MAX_NOTIONAL_OVER` | `1.5` | Si el redondeo al `stepSize` supera notional × esto, se salta el par. |
| `INVERT_SIGNALS` | `false` | Arrancar en modo invertido (luego manda el botón de la web). |
| `FEE_PCT` | `0.05` | Comisión por lado (% del notional) que se resta al PnL. |
| `PROFIT_TARGET_USD` | `12` | Realizado + abierto con el que se cierra todo. `0` = apagado. |
| `MAX_OPEN_POSITIONS` | `0` | Límite inicial de posiciones abiertas (los dos bots). `0` = sin límite. |
| `COOLDOWN_SECONDS` | `0` | Pausa tras un cierre antes de que ese bot vuelva a operar el símbolo. |
| `SIGNAL_MAX_DELAY_S` | `10` | Un cierre de vela que llega más tarde que esto no genera señal. |
| `PRICE_MAX_AGE_S` | `5` | Antigüedad máxima del precio usado para el TP. |
| `EXECUTOR_URL` | `https://executor-5lu0.onrender.com` | Link inicial del Executor (se puede cambiar desde la web). |
| `EXECUTOR_SECRET` | `clave-secreta-aleatoria` | Valor de `X-Signal-Secret`. |
| `ADMIN_TOKEN` | vacío | Si se define, cambiar el executor y cerrar desde la web lo exige. |
| `KLINE_HISTORY` | `2` | Velas cerradas guardadas por símbolo. |
| `REST_PROXY_URL` | — | Proxy para la única petición REST (`exchangeInfo`, al arrancar). |
| `SYMBOL_REFRESH_HOURS` | `240` | Cada cuánto se renueva `exchangeInfo` (filtros de cantidad): 10 días. |
| `RATIO_TP_PCT` | `10` | Bot de ratio: take profit por precio, % a favor (0 = sin TP). |
| `RATIO_SL_PCT` | `5` | Bot de ratio: stop loss por precio, % en contra (0 = sin SL). |
| `RATIO_EXIT_BY_RATIO` | `true` | Bot de ratio: cerrar también cuando el ratio recupera el % que cayó. |
| `KLINE_BOTS_ENABLED` | `false` | Activa los dos bots de velas 1m (abre el stream de velas). |
| `TICKER_MODE` | `scan` | 24 h por WebSocket: exploración cada 5 min (`scan`) o abierto siempre (`ws`). |
| `TICKER_SCAN_S` | `30` | Segundos de cada exploración del mercado. |
| `SYMBOL_SCAN_HOURS` | `24` | Cada cuánto la exploración rehace la lista de símbolos. |
| `WS_SYMBOL_STREAM` | auto | Stream de precio por posición: `markPrice@1s` (solo ratio) o `bookTicker` (con velas). |
| `WS_ALL_MARKET` / `WS_ALL_TICKER` | `false` | Streams de todo el mercado en `WS.py` (mucho tráfico). |
| `STATUS_POLL_MS` | `3000` | Intervalo de consulta del panel. |
| `RECOVERY_FILE` | `/tmp/botkline_recovery.json` | Copia local de posiciones abiertas e historial. Upstash está desactivado por ahora: en Render free las posiciones abiertas se pierden al reiniciar. |

## Endpoints

| Ruta | Uso |
| --- | --- |
| `GET /` | Panel con los dos bots: posiciones, señales, cierres, eventos y link del executor. |
| `GET /api/status` | Estado en JSON. `?engine=ratio` manda el detalle (señales, cierres, MFE/MAE) solo de ese bot. |
| `GET/POST /api/executor` | Ver o cambiar el link del Executor (común). |
| `GET/POST /api/config` | Ver o cambiar el % de la señal y del TP de un bot (`engine`). |
| `POST /api/close/<trade_id>` | Cierre manual de una posición (`/api/close/<SÍMBOLO>?engine=live` cierra todas las del par en ese bot). |
| `GET /api/trades?engine=` | Historial de cierres de un bot, o de los dos. |
| `GET /api/trades.csv?engine=` | Historial con MFE/MAE en CSV. |
| `POST /api/block` | Bloquear o permitir LONG o SHORT en un bot. |
| `GET/POST /api/ratio` | Ver o cambiar el bot de ratio: vigilancia, rango, `tp_pct`, `sl_pct`, `ratio_exit`. |
| `GET/POST /api/global` | Modo invertido, objetivo en USD y fee (común a los dos bots). |
| `POST /api/pause` | Pausar o reanudar un bot. |
| `POST /api/close-all` | Cerrar todas las posiciones (de un bot con `engine`). |
| `POST /api/session/reset` | Poner a cero el contador del objetivo. |
| `GET /health` | Chequeo ligero con el tráfico de red. |
| `GET /api/recovery` | Diagnóstico de la persistencia. |

## Ejecutar local

```bash
pip install -r requirements.txt
python app.py          # http://localhost:8000
python -m pytest tests
```

## Deploy en Render

`render.yaml` define el servicio y las variables. La estrategia, los bots, la API y el panel están en `app.py` (las reglas puras tienen pruebas en `tests/test_strategy.py`).

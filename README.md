# BOTKLINE · bots de ratio de top traders (Binance USDⓈ-M Futures)

Servicio web para Render que vigila los perpetuos USDT que más se mueven en 24 h, consulta el *Top Trader Long/Short Ratio* y manda señales de apertura y cierre al **Executor** externo, que es quien abre las órdenes en Binance. En el mismo proceso viven además dos bots de velas 1m, apagados por defecto (`KLINE_BOTS_ENABLED=false`).

## Estructura

| Archivo | Qué hace |
| --- | --- |
| `app.py` | Todo el bot: estrategia (reglas puras, con pruebas en `tests/`), bots, API Flask y panel web (tema claro / oscuro / automático). |
| `WS.py` | Precio en vivo por WebSocket, solo de los símbolos con posición abierta. |
| `KlineWebSocketCache_v4.py` | Caché de velas (versión anterior, no la usa `app.py`). |
| `kline_ws.py` | Velas 1m por WebSocket (solo si los bots de velas están activos). |
| `state_store.py` | Copia en disco de posiciones e historial para recuperarse tras un reinicio. |

## Bots de ratio de top traders

Tres bots de ratio operan en paralelo con **la misma entrada** y salidas distintas. Con cada señal, cada uno abre su propia operación (si no está pausado ni tiene esa dirección bloqueada):

| Bot | Salida |
| --- | --- |
| **Ratio TP/SL** (`ratio`) | TP y SL por precio + TP por ratio, todo configurable. |
| **Ratio solo TP ratio** (`ratio_tp`) | Sin TP ni SL de precio: cierra solo cuando el ratio recupera el % que se movió. |
| **Ratio + venganza** (`ratio_rev`) | TP/SL propios (independientes del primero) + trade de venganza. |

1. **Exploración por WebSocket**: en cada ciclo de 5 min se abre `!miniTicker@arr` y se lee el 24 h de todo el mercado; se cierra en cuanto un mensaje ya casi no trae símbolos nuevos (2-4 s). Solo la exploración diaria dura los 30 s completos (`TICKER_SCAN_S`). La misma exploración descubre los símbolos: una vez al día la lista se rehace (salen los deslistados) y entre medias se añaden los nuevos; se guarda en `KNOWN_SYMBOLS_FILE`.
   **Vigilados**: los perpetuos con variación 24 h ≥ +10 % (`RATIO_WATCH_MIN_PCT`, editable en la web). Con LONG activo, también los ≤ −10 %.
2. **Entrada**: cada 5 min se consulta el ratio de cuentas de los top traders de cada vigilado, **3 min después** del cierre del bloque de 5 min (a las :03 y :08; `RATIO_POLL_DELAY_S=180`), porque Binance tarda unos 3 min en publicar el punto de todos los pares; los que aún falten se reintentan cada 25 s hasta antes del siguiente ciclo. Si **cae** entre 13 % y 100 % de un punto de 5 min al siguiente en un ganador (24 h ≥ +X %) → **SHORT**. Si **sube** entre 13 % y 100 % en un perdedor (24 h ≤ −X %) → **LONG**. Las dos direcciones están activas por defecto (`RATIO_LONG_DEFAULT=false` deja el LONG bloqueado al arrancar) y cada una se bloquea desde la web.
3. **Salida** (lo que llegue antes):
   - **Take profit por precio**: **+10 %** a favor desde la entrada (`RATIO_TP_PCT`).
   - **Stop loss por precio**: **−4,8 %** en contra desde la entrada (`RATIO_SL_PCT`).
   - **TP por ratio** (desactivable): el ratio recupera el mismo % que cayó.

El TP, el SL y la salida por ratio del bot **Ratio TP/SL** se cambian **en caliente** desde la web (*Bots → Bot ratio TP/SL*), o con:

```bash
curl -X POST https://<bot>/api/ratio -H 'Content-Type: application/json' \
     -d '{"tp_pct": 10, "sl_pct": -5, "ratio_exit": true}'
```

`0` apaga el TP o el SL; el SL se puede mandar como `5` o `-5`. Los niveles nuevos se aplican también a las posiciones abiertas de ese bot (se recalculan desde su precio de entrada) y se guardan en `SETTINGS_FILE`. El cierre sale con motivo `TP`, `SL`, `TP_RATIO` o `TP_RSI_VWAP`.

### TP por RSI + VWAP ±σ (los tres bots de ratio)

Una salida más, que se enciende y apaga **por bot** y en caliente (*Bots → Bots de ratio → TP por RSI + VWAP*; apagada por defecto):

- **SHORT**: cierra cuando una vela 1m cierra con **RSI(14) < 13** y el cierre **≤ VWAP − 2,5σ**.
- **LONG**: lo simétrico, **RSI > 87** y cierre **≥ VWAP + 2,5σ**.
- El VWAP es el del **día UTC** (se reinicia a las 00:00 UTC), con precio típico (H+L+C)/3 y su desviación típica ponderada por volumen. El RSI es el de Wilder.
- Periodo, nivel y σ son comunes a los tres bots y editables en la web; también se puede exigir solo el RSI o solo el VWAP. Solo cuentan las velas que cierran **después** de abrir; las venganzas no lo usan.

**Velas**: al abrir una posición en un bot con esta salida encendida se piden por REST, **una sola vez**, `max(minutos desde las 00:00 UTC, 10 × periodo) + 1` velas 1m (las de hoy para el VWAP y al menos 140 para que el RSI de Wilder ya esté asentado; la última, aún abierta, se descarta). Ej.: 00:45 UTC → 141 velas; 12:00 → 721; máximo 1440. Después se sigue **vela a vela** por WebSocket (`<par>@kline_1m`, solo los pares con posición, ~2 MB/h por par) y el RSI y las bandas se actualizan cada minuto sin más REST. Si el WebSocket se corta y falta alguna vela, se recarga por REST.

**Velas de otros exchanges**: las velas se piden por REST a **Bybit → KuCoin (futuros) → Coinbase → MEXC (futuros)** en ese orden (el primero que tenga el par; `IND_EXCHANGES`) y **Binance solo como último recurso**, para que el único REST a Binance sea el del ratio. Cada exchange se pagina a su máximo (Bybit 1000, KuCoin 500, Coinbase 290, MEXC 1000 por petición) y después se sigue con una petición pequeña por minuto **al mismo exchange**: nunca se mezclan velas de mercados distintos (el volumen del VWAP no sería comparable). Los minutos sin operaciones que Coinbase no publica se rellenan planos y sin volumen. Un `1000PEPE` se busca también sin el prefijo (la escala del precio da igual: RSI y bandas se calculan con velas del mismo sitio). Si vienen de Binance, se piden en tramos de 499 con `startTime`/`endTime` (peso 2 cada uno; 1500 de golpe pesan 10) y siguen por su WebSocket `kline_1m`.

```bash
curl -X POST https://<bot>/api/indicators -H 'Content-Type: application/json' -d '{"engine": "ratio_tp", "enabled": true}'
curl -X POST https://<bot>/api/indicators -H 'Content-Type: application/json' -d '{"rsi_period": 14, "rsi_level": 13, "vwap_k": 2.5}'
```

### Demora mínima y precio de ejecución en papel

- **REST a Binance y bloqueos (418/429)**: el único REST a Binance es el del **ratio** (más `exchangeInfo` al arrancar). Va por la IP directa; si recibe un 418/429, esa ruta queda vetada lo que diga Binance y las peticiones pasan a las **proxies** (`REST_PROXY_URLS`, por defecto las 5 de Fixie), **rotando una distinta en cada petición** y saltando las vetadas, hasta que la IP directa se desbloquee. Con todas vetadas no sale nada (insistir alarga el veto). Además se lee el peso usado que Binance devuelve en `X-MBX-USED-WEIGHT-1M`: si una ruta pasa de `REST_WEIGHT_BUDGET` (80 % de 2400) descansa hasta el minuto siguiente, **antes** de que llegue el 429. Las proxies no se usan para nada más.
- **Demora mínima**: ninguna posición se cierra por TP/SL, ratio o RSI/VWAP antes de **2 s** desde que abrió (`MIN_HOLD_S`, editable en *Sistema → Ajustes generales*). Si la señal llega antes, se vuelve a mirar al cumplirse la demora. Los cierres manuales, «Cerrar todas» y el objetivo global no esperan.
- **Precios como el executor**: la entrada y la salida en papel se valoran con el **libro de 20 niveles**, tomado del **WebSocket** `<par>@depth20@100ms` (se conecta, toma una foto y se cierra; sin REST): una compra recorre los asks desde el mejor y una venta los bids, y se toma el precio medio ponderado de la cantidad; lo que pase de 20 niveles va al peor precio visto. Las órdenes al executor (aperturas, cierres y venganzas) **no esperan el libro**: salen al instante con el libro si hay una foto reciente, o si no con el precio en vivo (o el de la señal), y el TP/SL se calcula desde ese precio. El libro se pide en paralelo y, cuando llega, se corrige en la página el precio de entrada (o de salida) en papel; el TP/SL ya puesto no cambia. La venganza sale en el mismo instante que el cierre que la dispara, con la pérdida estimada a ese precio. **Excepción, el TP por precio**: en un mechazo el último precio puede tocar el TP sin que haya bids/asks a ese nivel, así que antes de enviar el cierre se valora la salida con el libro; si no llega al TP (igual o mejor) el cierre se **rechaza** y la posición sigue abierta hasta que el TP se cumpla de verdad (se registra en el log y en `tp_rejects`). El SL y los cierres por señal (ratio, RSI/VWAP, objetivo, manual) no se frenan. Se apaga con `TP_BOOK_CHECK=false`. Los tres bots de ratio comparten un mismo libro. Se apaga con `BOOK_FILL=false`.

### Trade de venganza (bot `ratio_rev`)

Cuando una operación de este bot cierra **en pérdida** por cualquier motivo (SL, TP por ratio, cierre manual…), abre otra en el mismo par para recuperarla:

- **Notional**: el de la operación cerrada **× `rev_mult`** (×1, ×2, ×3, ×4 o ×10 desde la web).
- **Dirección**: `opposite` (contraria a la perdedora, por defecto) o `same`.
- **TP**: `rev_recover_pct` % de la pérdida, en USD netos de fees (64 % por defecto).
- **SL**: `rev_sl_pct` % de (pérdida × `rev_mult`), en USD (150 % por defecto).
- Si la venganza pierde, **no se encadena otra**. Tampoco se dispara al alcanzar el objetivo global ni con «Cerrar todas».

Su salida y su venganza se ajustan en *Bots → Bot ratio + venganza*, o con:

```bash
curl -X POST https://<bot>/api/ratio -H 'Content-Type: application/json' \
     -d '{"engine": "ratio_rev", "tp_pct": 10, "sl_pct": -5, "ratio_exit": true,
          "revenge": true, "rev_mult": 10, "rev_recover_pct": 65, "rev_sl_pct": 100, "rev_dir": "opposite"}'
```

## Ancho de banda

El tráfico de red venía casi todo de streams de *todo el mercado* abiertos las 24 h y del panel pidiendo el estado completo cada 1,5 s. Ahora:

| Antes | Ahora |
| --- | --- |
| `!markPrice@arr@1s` + `!ticker@arr` en `WS.py` (~250 KB/s siempre) | Apagados por defecto (`WS_ALL_MARKET`, `WS_ALL_TICKER`). |
| `!miniTicker@arr` abierto siempre para el 24 h (~80 KB/s) | Exploración de 30 s por WebSocket en cada ciclo de 5 min (`TICKER_MODE=scan`, ~10 % del tráfico; `ws` = abierto siempre). |
| `bookTicker` de cada vigilado, sin darlo de baja nunca | Vigilados (24 h ≥ ±X %): `markPrice` (cada 3 s; `WATCH_PRICE_STREAM=markPrice@1s` para cada segundo). Con posición: además `aggTrade` (último precio operado, cada 100 ms como máximo). Todo se da de baja al salir de la lista o cerrar. Los bots de velas usan `bookTicker`. |
| REST sin comprimir (`exchangeInfo` ≈ 1,5 MB en cada arranque) | Solo queda REST para el ratio (no existe por WebSocket) y para los filtros de cantidad de `exchangeInfo` (al arrancar si no hay caché, y luego cada 10 días), todo con gzip. |
| Panel: estado completo de los 3 bots cada 1,5 s, sin comprimir | Cada 3 s, gzip, solo el detalle del bot que se ve, y pausa con la pestaña oculta. La página tiene ETag (recarga = 304). |
| Executor: `GET /api/state` cada 20 s por hilo | `HEAD` (sin cuerpo). |

El bloque *Red* de la web (y `GET /health`) muestra los bytes recibidos y enviados desde el arranque y el ritmo por hora.

### Ahorro de datos (botones en la web)

En *Bots → Bots de ratio · entrada común → Ahorro de datos* hay dos interruptores que se guardan y se aplican al momento:

- **Vigilados: precio en vivo** (`watch_live_price`). Encendido, cada vigilado recibe `markPrice` cada 3 s. Apagado, su 24 h se lee solo en la exploración de cada 5 min, y el precio para entrar se pide solo cuando el ratio da señal. Con muchos vigilados es lo que más tráfico ahorra.
- **Abiertas: precio rápido** (`fast_prices`). Encendido, usa `aggTrade` (~100 ms). Apagado, usa `markPrice@1s`. Con el precio a 1 s, el TP puesto en Binance (si la posición va sola) cubre los picos.

También con `POST /api/global` (`{"watch_live_price": false, "fast_prices": false}`).

## Panel web

Menú con seis secciones; cada una tiene paneles desplegables (se recuerda cuáles dejaste abiertos) y tema claro, oscuro o automático:

| Sección | Qué hay |
| --- | --- |
| **Resumen** | Cifras de la sesión, una tarjeta por bot (estado, parámetros, pausar), acciones generales (invertir, pausar el executor, poner a cero, cerrar todo) y las operaciones activas en tarjetas. |
| **Operaciones** | Tarjetas de las posiciones abiertas, filtrables por bot, con PnL, barra SL → TP, MFE/MAE y botón de cierre; vista en tabla y señales recientes. |
| **Bots** | Un panel por bot: entrada común de los de ratio, salida de cada uno, tarjeta de la venganza, umbral/TP/bloqueos de los de velas, encender las velas y el modo invertido. |
| **Mercado** | Vigilados de los bots de ratio y evaluaciones de velas. |
| **Historial** | Cierres y MFE/MAE por bot, CSV. |
| **Sistema** | Objetivo, límite de abiertas, fee, executor, tráfico de red y eventos. |

El panel solo pide al servidor los datos de la sección abierta (`/api/status?view=`).

## Bots de velas 1m

Se encienden y apagan en caliente desde *Bots → Encender velas* (o `POST /api/global` con `{"klines_enabled": true}`); `KLINE_BOTS_ENABLED` es solo el valor inicial. Las velas son el stream con más tráfico (~0,5 MB/s).

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

`direction` puede ser `LONG` o `SHORT`, `level` lleva el % del cuerpo de la vela (o del cambio del ratio) que disparó la señal y `strategy` dice qué bot la mandó (`ratio`, `ratio_tp`, `ratio_rev`, `close` o `live`). Los `trade_id` son únicos entre todos los bots.

El executor suma en **una** posición de Binance las de todos los bots en el mismo par y dirección (los tres bots de ratio abren con la misma señal). Por eso, mientras otro bot siga con ese par y dirección abierto, el `close` lleva `"quantity"` con la cantidad de esa operación y el executor cierra solo esa parte; el último en cerrar manda el cierre sin `quantity` y cierra todo lo que quede. La venganza en dirección contraria y los bots con posiciones opuestas en el mismo par necesitan la cuenta en **modo cobertura (Hedge Mode)**: en modo unidireccional Binance las netea.

### TP en Binance

Cada apertura con TP por precio lleva `"tp"` con el take profit de esa posición. El executor lo pone en Binance con **la cantidad de esa posición** y lo identifica por su `trade_id`, así que hay un TP por posición aunque varios bots compartan par y dirección (los tres de ratio abren juntos), y el TP salta aunque el bot o la red fallen. El bot *solo TP ratio* no lleva TP por precio. Si el TP cambia en caliente, se manda `open_tp` del `trade_id` con el nuevo nivel (o `close_tp` si se apagó). Se enciende y apaga en caliente desde la web (*Ahorro de datos* → «TP al executor», para todos los bots): al apagarlo se quita el TP en Binance de las abiertas y las aperturas nuevas van sin TP; al encenderlo se les pone. `SEND_TP` solo fija el valor inicial.

### Latencia

Las señales salen por conexiones HTTP persistentes (keep-alive): `EXECUTOR_WORKERS` hilos (4 por defecto) mantienen abierta su conexión con el executor y la refrescan con un `HEAD /api/state` cada `EXECUTOR_WARM_S` segundos (20) si no hay señales. Así cada señal paga solo el viaje de red, sin abrir TCP+TLS de nuevo. Cada par va siempre por el mismo hilo, así sus señales llegan en el orden en que se mandaron (un cierre parcial antes que el total, un cierre antes de la venganza en el mismo par) y los pares distintos salen en paralelo. La entrada usa el último precio de la vela. La web muestra los ms de decisión de cada bot y, para la última señal, los ms del HTTP al executor y el total desde que llegó el mensaje de Binance.

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
| `REST_PROXY_URLS` | 5 proxies Fixie | Proxies (separadas por comas) para el REST de Binance cuando la IP directa está vetada; rotan. `REST_PROXY_URL` añade una más. |
| `SYMBOL_REFRESH_HOURS` | `240` | Cada cuánto se renueva `exchangeInfo` (filtros de cantidad): 10 días. |
| `RATIO_TP_PCT` | `10` | Bots `ratio` y `ratio_rev`: take profit por precio, % a favor (0 = sin TP). |
| `RATIO_SL_PCT` | `4.8` | Bots `ratio` y `ratio_rev`: stop loss por precio, % en contra (0 = sin SL). |
| `RATIO_EXIT_BY_RATIO` | `true` | Cerrar también cuando el ratio recupera el % que cayó. |
| `REVENGE_ENABLED` | `true` | Bot `ratio_rev`: trade de venganza activo al arrancar. |
| `REVENGE_MULT` | `11` | Venganza: multiplicador del notional de la operación perdedora. |
| `REVENGE_RECOVER_PCT` | `64` | Venganza: % de la pérdida que busca recuperar (TP en USD). |
| `REVENGE_SL_PCT` | `150` | Venganza: SL en % de (pérdida × multiplicador). |
| `REVENGE_DIR` | `opposite` | Venganza: `opposite` (contraria) o `same`. |
| `KLINE_BOTS_ENABLED` | `false` | Activa los dos bots de velas 1m (abre el stream de velas). |
| `TICKER_MODE` | `scan` | 24 h por WebSocket: exploración cada 5 min (`scan`) o abierto siempre (`ws`). |
| `TICKER_SCAN_S` | `30` | Segundos máximos de cada exploración del mercado (la diaria los usa enteros). |
| `TICKER_SCAN_QUICK` | `true` | Corta las exploraciones de cada ciclo en cuanto llegó todo el mercado activo. |
| `SYMBOL_SCAN_HOURS` | `24` | Cada cuánto la exploración rehace la lista de símbolos. |
| `MIN_HOLD_S` | `2` | Segundos mínimos entre la apertura y un cierre por TP/SL, ratio o RSI/VWAP (editable en la web). |
| `BOOK_FILL` | `true` | Entrada y salida en papel con el libro de 20 niveles, como el executor. |
| `TP_BOOK_CHECK` | `true` | El TP por precio solo cierra si el libro (bids/asks) llega al TP; si no, rechaza el cierre y la deja abierta. |
| `BOOK_MAX_AGE_S` / `BOOK_TIMEOUT_S` | `1.5` / `2` | Cuánto vale una foto del libro y la espera máxima del WebSocket. |
| `IND_RSI_PERIOD` / `IND_RSI_LEVEL` / `IND_VWAP_K` | `14` / `13` / `2.5` | Valores iniciales del TP por RSI + VWAP (luego se editan en la web). |
| `RATIO_POLL_DELAY_S` | `180` | Segundos tras cada bloque de 5 min (:00/:05) antes de consultar el ratio. |
| `IND_EXCHANGES` | `bybit,kucoin,coinbase,mexc,binance` | Orden de los exchanges para las velas del RSI/VWAP. |
| `BINANCE_KLINES_CHUNK` | `499` | Velas por petición cuando hay que pedirlas a Binance (≤499 = peso 2). |
| `REST_WEIGHT_BUDGET` | `1920` | Peso por minuto (`X-MBX-USED-WEIGHT-1M`) a partir del cual una ruta de Binance descansa hasta el minuto siguiente. |
| `WATCH_LIVE_PRICE` | `true` | Precio en vivo `markPrice` (cada 3 s) para los vigilados sin posición (`false` = solo los que tienen posición). |
| `WS_SYMBOL_STREAM` | auto | Stream de precio por posición: `aggTrade` (solo ratio) o `bookTicker` (con velas). |
| `WS_ALL_MARKET` / `WS_ALL_TICKER` | `false` | Streams de todo el mercado en `WS.py` (mucho tráfico). |
| `STATUS_POLL_MS` | `3000` | Intervalo de consulta del panel. |
| `RECOVERY_FILE` | `/tmp/botkline_recovery.json` | Copia local de posiciones abiertas e historial. Upstash está desactivado por ahora: en Render free las posiciones abiertas se pierden al reiniciar. |

## Endpoints

| Ruta | Uso |
| --- | --- |
| `GET /` | Panel con los dos bots: posiciones, señales, cierres, eventos y link del executor. |
| `GET /api/status` | Estado en JSON. `?engine=ratio|ratio_tp|ratio_rev|close|live` manda el detalle (señales, cierres, MFE/MAE) solo de ese bot. |
| `GET/POST /api/executor` | Ver o cambiar el link del Executor (común). |
| `GET/POST /api/config` | Ver o cambiar el % de la señal y del TP de un bot (`engine`). |
| `POST /api/close/<trade_id>` | Cierre manual de una posición (`/api/close/<SÍMBOLO>?engine=live` cierra todas las del par en ese bot). |
| `GET /api/trades?engine=` | Historial de cierres de un bot, o de los dos. |
| `GET /api/trades.csv?engine=` | Historial con MFE/MAE en CSV. |
| `POST /api/block` | Bloquear o permitir LONG o SHORT en un bot. |
| `GET/POST /api/ratio` | Ver o cambiar los bots de ratio: vigilancia y rango (comunes), `tp_pct`, `sl_pct`, `ratio_exit`; con `"engine": "ratio_rev"`, la salida y la venganza de ese bot. |
| `GET/POST /api/global` | Modo invertido, objetivo en USD, fee y demora mínima `min_hold_s` (comunes). |
| `GET/POST /api/indicators` | TP por RSI + VWAP: `{"engine": "ratio", "enabled": true}` por bot; `rsi_period`, `rsi_level`, `vwap_k`, `use_rsi`, `use_vwap` comunes. |
| `POST /api/realized/reset` | Poner a cero el PnL realizado de un bot (`{"engine": "ratio"}`); el historial se conserva. |
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

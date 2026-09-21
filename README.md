# Tarea 3 — Beam avanzado

Proyecto base autocontenido para la asignatura **Streaming de datos y sus
aplicaciones**. La tarea consiste en completar un pipeline de pagos con tiempo
de evento, ventanas, estado por clave y una salida idempotente.

El repositorio es deliberadamente un esqueleto: `notebook.py` contiene la
consigna, contratos y funciones sin implementación. No incluye la solución.

## Objetivo

Producir totales confirmados por comercio y minuto:

- usando `event_time`, no el tiempo de llegada;
- tolerando hasta 120 segundos de atraso;
- descartando estados distintos de `CONFIRMED`;
- deduplicando `event_id` dentro de cada comercio;
- conservando metadatos de ventana y pane;
- materializando la salida mediante una clave idempotente.

## Ejecutar con Docker

Desde este directorio:

```bash
docker compose up --build notebook
```

Abrir <http://localhost:2718>. Docker inicia Marimo en modo editor porque la
tarea requiere completar las celdas de código. Los cambios en `notebook.py` se
guardan en el directorio local.

El editor usa `--no-token` para simplificar el trabajo en `localhost`; no debe
exponerse directamente a una red pública.

## Ejecutar con uv

```bash
uv sync --frozen
uv run marimo edit notebook.py
```

## Trabajar con tests

```bash
uv run pytest
```

Los tests se entregan deliberadamente en rojo: las funciones del notebook
lanzan `NotImplementedError`. El objetivo es implementar las celdas hasta
obtener una suite completamente verde.

Los tests cargan las funciones directamente desde `notebook.py`; no hay que
copiar la solución a otro módulo.

Para validar además estilo y estructura:

```bash
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

Dentro del contenedor también se puede ejecutar:

```bash
docker compose exec notebook uv run pytest
```

## Entrega

Entregar un repositorio propio que incluya:

- `notebook.py` con todas las funciones implementadas;
- evidencia de ejecución del pipeline;
- todas las pruebas provistas para desorden, duplicados, atraso y reintentos
  ejecutadas y aprobadas;
- un README breve con decisiones y trade-offs;
- instrucciones reproducibles con Docker o `uv`.

No modificar `data/payments.jsonl`; puede agregarse un conjunto de datos
adicional para las pruebas.


---

## Decisiones de Diseño y Resumen Técnico

### 1. Tiempo de Evento y Ventanas Fijas (Fixed Windows)
- Se utiliza `event_time` extraído con `parse_utc` como el timestamp de dominio para garantizar determinismo sin importar el orden de llegada.
- Se configuran ventanas fijas (`FixedWindows`) de 60 segundos con un `allowed_lateness` de 120 segundos.

### 2. Deduplicación con Estado y Expiración (Timers)
- Para la deduplicación por clave se utiliza `SetStateSpec` agrupado por `merchant_id`.
- Para evitar que el estado crezca indefinidamente (*memory leaks*), se implementa un `TimerSpec` configurado en el tiempo de evento (`TimeDomain.WATERMARK`) alineado al final de la ventana más la lateness permitida, limpiando el estado acumulado con `@on_timer`.

### 3. Manejo de Datos Tardíos y Triggers
- Se utiliza una política de triggers `AfterWatermark(early=..., late=AfterCount(1))` en modo acumulativo (`AccumulationMode.ACCUMULATING`).
- Esto permite emitir paneles intermedios y de revisión cuando llegan eventos fuera de orden o tardíos dentro de la ventana de tolerancia.

### 4. Idempotencia y Reintentos
- Para evitar duplicación de datos al escribir en sinks externos ante fallos de red o reintentos de procesamiento, se construye una clave idempotente determinista con el formato `merchant_id|window_start`.
- En un escenario real o simulado, esta clave permite operaciones de tipo `UPSERT`, garantizando que reescrituras de la misma ventana mantengan la consistencia y converjan a un único registro final.
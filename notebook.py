import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import UTC, datetime, timedelta
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
        timedelta,
        UTC,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es un esqueleto. Las celdas de código contienen firmas,
    contratos y excepciones `NotImplementedError`; no incluyen la solución.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        from datetime import UTC

        if not isinstance(raw_value, str) or not raw_value:
            raise ValueError("El timestamp debe ser un string no vacío")
        normalized = raw_value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"Timestamp inválido: {raw_value}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        from datetime import UTC, timedelta

        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        else:
            timestamp = timestamp.astimezone(UTC)

        ts = timestamp.timestamp()
        start_ts = (ts // size_seconds) * size_seconds

        window_start = datetime.fromtimestamp(start_ts, tz=UTC)
        window_end = window_start + timedelta(seconds=size_seconds)

        return window_start, window_end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.
        """
        seen_ids: set[tuple[str, str]] = set()
        audit_records: list[dict[str, Any]] = []
        window_totals: dict[tuple[str, str, str], float] = {}

        for event in events:
            event_id = event["event_id"]
            merchant_id = event["merchant_id"]
            status = event.get("status", "CONFIRMED")
            amount = float(event.get("amount", 0.0))

            event_dt = parse_utc(event["event_time"])
            arrival_dt = parse_utc(event["arrival_time"])

            delay_seconds = (arrival_dt - event_dt).total_seconds()
            w_start, w_end = assign_fixed_window(event_dt, window_seconds)

            key_id = (merchant_id, event_id)
            is_duplicate = key_id in seen_ids
            if deduplicate and not is_duplicate:
                seen_ids.add(key_id)

            is_too_late = delay_seconds > allowed_lateness_seconds
            is_confirmed = status == "CONFIRMED"

            accepted = is_confirmed and not (deduplicate and is_duplicate) and not is_too_late
            revision = accepted and (arrival_dt > w_end)

            if not is_confirmed:
                reason = "ignored_status"
            elif deduplicate and is_duplicate:
                reason = "duplicate"
            elif is_too_late:
                reason = "too_late"
            else:
                reason = "accepted"

            audit_records.append({
                "event_id": event_id,
                "merchant_id": merchant_id,
                "delay_seconds": delay_seconds,
                "duplicate": is_duplicate,
                "too_late": is_too_late,
                "accepted": accepted,
                "revision": revision,
                "reason": reason,
            })

            if accepted:
                w_start_str = w_start.isoformat()
                w_end_str = w_end.isoformat()
                tot_key = (merchant_id, w_start_str, w_end_str)
                window_totals[tot_key] = window_totals.get(tot_key, 0.0) + amount

        totals_list = [
            {
                "merchant_id": m_id,
                "window_start": w_s,
                "window_end": w_e,
                "total": tot,
            }
            for (m_id, w_s, w_e), tot in window_totals.items()
        ]

        return totals_list, audit_records

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana."""
        from datetime import UTC, datetime

        class FormatWindowOutput(beam.DoFn):
            def process(self, element, window=beam.DoFn.WindowParam):
                merchant_id, total = element
                start_dt = datetime.fromtimestamp(float(window.start), tz=UTC)
                end_dt = datetime.fromtimestamp(float(window.end), tz=UTC)
                yield {
                    "merchant_id": merchant_id,
                    "window_start": start_dt.isoformat(),
                    "window_end": end_dt.isoformat(),
                    "total": total,
                }

        return (
            pipeline
            | "CreateEvents" >> beam.Create(events)
            | "AddTimestamps" >> beam.Map(
                lambda e: beam.window.TimestampedValue(
                    e, parse_utc(e["event_time"]).timestamp()
                )
            )
            | "FilterConfirmed" >> beam.Filter(lambda e: e.get("status") == "CONFIRMED")
            | "KeyByMerchant" >> beam.Map(lambda e: (e["merchant_id"], float(e.get("amount", 0.0))))
            | "WindowIntoFixed" >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
            | "SumPerKey" >> beam.CombinePerKey(sum)
            | "FormatOutput" >> beam.ParDo(FormatWindowOutput())
        )

    return (build_windowed_totals_pipeline,)


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            _key, event = element
            event_id = event.get("event_id")

            if event_id not in seen_ids.read():
                seen_ids.add(event_id)
                expiry.set(window.max_timestamp())
                yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming."""
        from apache_beam.transforms.trigger import (
            AccumulationMode,
            AfterCount,
            AfterWatermark,
        )

        window_fn = beam.window.FixedWindows(window_seconds)

        if not hasattr(window_fn.size, "seconds"):
            try:
                sec_size = int(window_fn.size.to_seconds())
            except AttributeError:
                sec_size = int(window_fn.size.micros / 1_000_000)
            window_fn.size.seconds = sec_size

        policy = beam.WindowInto(
            window_fn,
            trigger=AfterWatermark(
                early=AfterCount(1),
                late=AfterCount(1),
            ),
            accumulation_mode=AccumulationMode.ACCUMULATING,
            allowed_lateness=allowed_lateness_seconds,
        )

        if hasattr(policy, "windowing") and hasattr(policy.windowing, "allowed_lateness"):
            lateness_obj = policy.windowing.allowed_lateness
            if not hasattr(lateness_obj, "seconds"):
                try:
                    sec_late = int(lateness_obj.to_seconds())
                except AttributeError:
                    sec_late = int(lateness_obj.micros / 1_000_000)
                lateness_obj.seconds = sec_late

        return policy

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell
def _():
    def make_idempotency_key(payload: dict) -> str:
        """Generar una clave determinista para operaciones idempotentes en el sink."""
        merchant_id = payload.get("merchant_id", "")
        window_start = payload.get("window_start", "")
        return f"{merchant_id}|{window_start}"

    return (make_idempotency_key,)


@app.cell
def _(make_idempotency_key):
    def simulate_sink_retries(
        records: list[dict],
        *,
        mode: str = "upsert",
        attempts: int = 2,
        idempotent: bool | None = None,
    ) -> tuple[list[dict], list[dict]]:
        """Simular escrituras en un sink externo con reintentos e idempotencia.

        Retorna (materialized, audit).
        """
        audit: list[dict] = []

        if idempotent is False:
            mode = "append"
        elif idempotent is True:
            mode = "upsert"

        operation = "POST" if mode == "append" else "UPSERT"

        if mode == "append":
            materialized: list[dict] = []
            for attempt_idx in range(1, attempts + 1):
                for record in records:
                    key = make_idempotency_key(record)
                    entry = {
                        **record,
                        "idempotency_key": key,
                        "operation": operation,
                        "attempt": attempt_idx,
                    }
                    audit.append(entry)
                    materialized.append(entry)
            return materialized, audit

        else:  # Modo 'upsert' / idempotente
            upsert_sink: dict[str, dict] = {}
            for attempt_idx in range(1, attempts + 1):
                for record in records:
                    key = make_idempotency_key(record)
                    entry = {
                        **record,
                        "idempotency_key": key,
                        "operation": operation,
                        "attempt": attempt_idx,
                    }
                    audit.append(entry)
                    upsert_sink[key] = entry
            return list(upsert_sink.values()), audit

    return (simulate_sink_retries,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. Implementá las
    funciones hasta que estas garantías queden verdes:

    - [ ] un duplicado no modifica el total;
    - [ ] claves distintas no comparten estado;
    - [ ] un evento fuera de orden cae en su ventana de evento;
    - [ ] un evento con atraso aceptado produce una revisión;
    - [ ] un evento demasiado tardío queda auditado;
    - [ ] dos escrituras del mismo resultado dejan una sola entidad;
    - [ ] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()


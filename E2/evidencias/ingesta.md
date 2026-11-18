# Triggers (eventos que disparan la ingesta y el procesamiento)

Generado: 2026-11-18 02:15 UTC

| Servicio | Nombre | Expresión | Estado | Destino |
|---|---|---|---|---|
| EventBridge Scheduler | IngestFunctionEstadoCada2Min | `rate(2 minutes)` | ENABLED | ecobici-cloud-IngestFunction-kQWNCMMscn6J `{"task": "status"}` |
| EventBridge Scheduler | IngestFunctionClimaCadaHora | `rate(1 hour)` | ENABLED | ecobici-cloud-IngestFunction-kQWNCMMscn6J `{"task": "weather"}` |
| Glue Trigger | ecobici-cloud-curate-diario | `cron(15 0 * * ? *)` | ACTIVATED | ecobici-cloud-curate |

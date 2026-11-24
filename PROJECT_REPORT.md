# Project Report: Real-Time Ingestion Pipeline with Schema Drift Handling

## Objective
Build a streaming data pipeline for IoT telemetry that continues processing when producers add optional fields or evolve payload versions.

## Design
Apache Kafka provides the durable event buffer. A Python consumer reads JSON events using a consumer group and processes them in micro-batches. A versioned JSON schema registry describes the supported contracts. The compatibility policy permits additive optional fields while preserving the original required fields.

## Schema drift strategy
The consumer compares each incoming payload's keys with the latest known schema. Unknown keys are logged, counted, and recorded in `schema_drift_events`; they are also preserved in the output `extra_fields` JSON column. This prevents a harmless producer-side addition from breaking the pipeline. Missing required fields, invalid numeric values, invalid timestamps, and out-of-range humidity or battery values are sent to `quarantine_events`.

## Transformation
The transformation layer standardizes timestamps to UTC, converts Fahrenheit to Celsius, casts numeric strings, normalizes device types, enriches device records with a site label, and carries optional firmware/battery/signal fields into the warehouse.

## Reliability and monitoring
Kafka auto-commit is disabled. Offsets are committed only after a micro-batch has been persisted. The warehouse uses `event_id` as a primary key and `INSERT OR IGNORE` for idempotency. Runtime counters and the latest quality error are exposed through a lightweight HTTP endpoint at `/metrics`.

## Output
Clean records are stored in `data/warehouse.db` in the `telemetry` table. Invalid messages are stored in `quarantine_events`; schema changes are stored in `schema_drift_events`.

## Validation
Run `python -m unittest discover -s tests -v` to verify type conversion, enrichment, required-field validation, schema drift detection, and additive compatibility.

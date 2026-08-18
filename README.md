# SWIM TFDM Consumer

This service consumes TFDM and TFMS messages from a SWIM Kafka broker, stores them in MongoDB, and exposes a small web UI and REST API to inspect flights and their latest TFMS route data.

## What it does

- **TFDM consumer** (`app/consumer.py`): reads `faa-tfdm-raw`, parses flight records, and stores them in the `flights` collection.
- **TFMS consumer** (`app/tfms_consumer.py`): reads `faa-tfms-raw`, parses per-flight TFMS messages, stores them in `tfms_messages`, and maintains the latest planned route in `flight_routes`.
- **Web UI** (`app/templates/`): lists active flights with a TFMS message count and links to per-flight TFMS message lists and detail pages.
- **REST API** (`app/api.py`): exposes JSON endpoints for flight snapshots and routes.

## Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `MONGODB_URL` | MongoDB connection string | `mongodb://localhost:27017` |
| `MONGODB_DB` | Database name | `swim_tfdm_consumer` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap list | `localhost:9092` |
| `KAFKA_TOPIC` | TFDM Kafka topic | `faa-tfdm-raw` |
| `KAFKA_TFMS_TOPIC` | TFMS Kafka topic | `faa-tfms-raw` |
| `KAFKA_GROUP_ID` | TFDM consumer group | `swim-tfdm-consumer` |
| `KAFKA_TFMS_GROUP_ID` | TFMS consumer group | `swim-tfms-consumer` |
| `KAFKA_AUTO_OFFSET_RESET` | Where to start when no offset exists | `latest` |
| `MONGODB_COLLECTION` | TFDM collection name | `flights` |
| `MONGODB_TFMS_COLLECTION` | TFMS collection name | `tfms_messages` |

## MongoDB collections

- `flights` — current TFDM flight snapshots, enriched with a `tfmsSummary` of useful TFMS data.
- `tfms_messages` — all parsed TFMS messages with a link to a TFDM flight when one can be matched.
- `flight_routes` — the latest known planned route per `flight_number`/`departure`/`arrival`, updated as new `FlightRoute`, `FlightSectors`, or `flightPlanAmendmentInformation` messages arrive.

## API endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/flights/{flight_number}` | Current TFDM flight snapshot, including a `tfmsSummary` of the latest useful TFMS data |
| GET | `/flights/{flight_number}/tfms` | HTML page listing TFMS messages for the flight |
| GET | `/flights/{flight_number}/route` | Latest planned route for the flight (JSON) |
| GET | `/tfms/{tfms_id}` | HTML detail page with raw TFMS message JSON |
| GET | `/` | HTML flight list |

## TFMS summary in flight records

When a TFMS message is linked to a TFDM flight, the `flights` record is updated with a `tfmsSummary` containing the latest useful information, such as:

- `first_igtd` — the first observed initial gate/ground time of departure; retained permanently.
- `latest_igtd`, `latest_eta`, `latest_etd` — the most recent TFMS times.
- `latest_flight_status`, `latest_aircraft_model` — from `FlightTimes`.
- `latest_position` — from `trackInformation`.
- `latest_route_text` — from `flightPlanAmendmentInformation`.
- `tfm_id`, `gufi`, `tfms_message_count`.

The first `igtd` is only set once and is not overwritten by later messages.

## Airport code normalization

US ICAO airport codes that start with `K` are normalized to the IATA/3-letter form:

- `KSFO` → `SFO`
- `KIAD` → `IAD`
- `KMIA` → `MIA`

Non-US codes (`CYEG`, `EGLL`, etc.) are left unchanged.

## Local development

```bash
uv sync
uv run python -m compileall app
uv run uvicorn app.main:app --reload
```

## Docker

```bash
docker build -t swim-tfdm-consumer:latest .
```

## Deployment

Pushes to `main` trigger the GitHub Actions workflow at `.github/workflows/deploy.yml`. The workflow builds a Docker image, pushes it to GHCR, and applies the Kustomize manifests in `k8s/overlays/prod` to a self-hosted k3s runner.

## Route data

The latest planned route is maintained in `flight_routes` and is updated on the fly as the following TFMS message types are consumed:

- `FlightRoute` — complete route with fixes, waypoints, DP, and STAR.
- `FlightSectors` — predicted trajectory broken down by ATC sector.
- `flightPlanAmendmentInformation` — route amendments in legacy string format.

The `/flights/{flight_number}/route` endpoint returns the most recently updated route for that flight number.

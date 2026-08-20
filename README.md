# SWIM TFDM Consumer

This service consumes TFDM, TFMS, TBFM, SFDPS, and STDDS messages from a SWIM Kafka broker, stores them in MongoDB, and exposes a small web UI and REST API to inspect flights and their latest data from all feeds. It also supports flight subscriptions with webhook delivery.

## What it does

- **TFDM consumer** (`app/consumer.py`): reads `faa-tfdm-raw`, parses flight records, and stores them in the `flights` collection.
- **TFMS consumer** (`app/tfms_consumer.py`): reads `faa-tfms-raw`, parses per-flight TFMS messages, stores them in `tfms_messages`, and maintains the latest planned route in `flight_routes`.
- **TBFM consumer** (`app/tbfm_consumer.py`): reads `faa-tbfm-raw`, parses per-flight TBFM XML metering messages, and stores them in `tbfm_messages`.
- **SFDPS consumer** (`app/sfdps_consumer.py`): reads `faa-sfdps-raw`, parses SFDPS JSON flight records, and stores them in `sfdps_messages`.
- **STDDS consumer** (`app/stdds_consumer.py`): reads `faa-stdds-raw`, parses STDDS JSON terminal tracks/flight plans, and stores them in `stdds_messages`.
- **Web UI** (`app/templates/`): lists active flights with message counts and links to per-flight message lists and detail pages for all feeds.
- **REST API** (`app/api.py`): exposes JSON endpoints for flight snapshots and routes.
- **Auth & clients** (`app/auth.py`, `app/security.py`, `app/admin.py`): client credentials, JWT tokens, and an admin portal for managing clients and subscriptions.
- **Subscriptions & webhooks** (`app/subscriptions.py`, `app/dispatcher.py`): clients can subscribe to a flight number and receive webhook calls whenever the flight snapshot changes.

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
| `RUN_MODE` | Process mode: `api` (web/API + consumers) or `dispatcher` (webhook dispatcher) | `api` |
| `JWT_SECRET` | Secret used to sign JWT access tokens | `change-me` |
| `ADMIN_SESSION_SECRET` | Secret used to sign admin portal session cookies | `change-me-admin-session` |
| `JWT_EXPIRY_HOURS` | JWT token lifetime in hours | `24` |
| `INACTIVITY_TIMEOUT_MIN` | Minutes of no SWIM messages before a subscription is removed | `120` |
| `PREFLIGHT_TIMEOUT_MIN` | Minutes before the first SWIM message before a subscription is removed | `1440` |
| `INACTIVITY_SCAN_INTERVAL_MIN` | Minutes between inactivity scans | `15` |
| `WEBHOOK_TIMEOUT_SECONDS` | Per-attempt webhook HTTP timeout | `10` |
| `WEBHOOK_RETRIES` | Number of webhook retry attempts | `5` |
| `WEBHOOK_RETRY_BASE_SECONDS` | Base delay for exponential retries | `5` |
| `WEBHOOK_THROTTLE_SECONDS` | Minimum seconds between webhooks for the same flight | `1` |

## MongoDB collections

- `flights` — current TFDM flight snapshots, enriched with `tfmsSummary`, `tbfmSummary`, `sfdpsSummary`, and `stddsSummary` blocks.
- `tfms_messages` — all parsed TFMS messages with a link to a TFDM flight when one can be matched.
- `tbfm_messages` — all parsed TBFM XML metering messages with a link to a TFDM flight when one can be matched.
- `sfdps_messages` — all parsed SFDPS JSON flight messages with a link to a TFDM flight when one can be matched.
- `stdds_messages` — all parsed STDDS JSON terminal track/flight plan records with a link to a TFDM flight when one can be matched.
- `flight_routes` — the latest known planned route per flight. When a TFMS route message can be matched to a `flights` doc, the `flight_routes` `_id` is the linked `flights._id`; otherwise it falls back to `flight_number`/`departure`/`arrival`.
- `clients` — API client credentials created via the admin portal.
- `subscriptions` — active/failing flight subscriptions and delivery attempt state.
- `flight_webhooks` — last-sent snapshot, send timestamp, and throttle state per flight.
- `admin` — admin user record for the portal.

## API endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/flights/{flight_number}` | Current TFDM flight snapshot, including `tfmsSummary`, `tbfmSummary`, `sfdpsSummary`, and `stddsSummary` |
| POST | `/flights/batch` | Look up a batch of flight numbers and get a current TFDM snapshot for each; request body is `["AA123", "UA456"]` and the response maps each flight number to its snapshot (or `null` if not found) |
| GET | `/flights/{flight_number}/tfms` | HTML page listing TFMS messages for the flight |
| GET | `/flights/{flight_number}/tbfm` | HTML page listing TBFM messages for the flight |
| GET | `/flights/{flight_number}/sfdps` | HTML page listing SFDPS messages for the flight |
| GET | `/flights/{flight_number}/stdds` | HTML page listing STDDS messages for the flight |
| GET | `/flights/{flight_number}/route` | Latest planned route for the flight (JSON) |
| GET | `/tfms/{tfms_id}` | HTML detail page with raw TFMS message JSON |
| GET | `/tbfm/{tbfm_id}` | HTML detail page with raw TBFM message XML |
| GET | `/sfdps/{sfdps_id}` | HTML detail page with raw SFDPS message JSON |
| GET | `/stdds/{stdds_id}` | HTML detail page with raw STDDS message JSON |
| GET | `/` | HTML flight list |
| POST | `/api/v1/auth/token` | Exchange client credentials (HTTP Basic) for a JWT |
| GET | `/api/v1/subscriptions` | List the authenticated client's active/failing subscriptions |
| GET | `/api/v1/subscriptions/{subscription_id}` | Get a single subscription |
| DELETE | `/api/v1/subscriptions/{subscription_id}` | Unsubscribe |
| GET | `/admin` | Admin portal (session-based) |

All endpoints except `/api/v1/auth/token` and `/admin` require a valid `Authorization: Bearer <token>` header.

## Flight API fields

`/flights/{flight_number}` now returns a clean TFDM snapshot with:

- Top-level identifiers and status: `tfdmId`, `tfmId`, `flightPlanIdentifier`, `flightNumber`, `airline`, `aircraftIdentification`, `flightState`, etc.
- `departure` block: `airport`, `offBlockTime`, `runway`, `estimatedRunwayDepartureTime`, `earliestRunwayDepartureTime`, `estimatedTaxiOutTime`, `predictedDelay`, `currentDelay`, `predictedSpot`, `fix`.
- `arrival` block: `airport`, `fix`, `estimatedArrivalTime`, `actualArrivalTime`, `runway`, `estimatedTaxiInTime`, `elapsedTaxiInTime`, `predictedSpot`, `actualSpot`, `movementAreaActualExitTime`.
- `tfmsSummary` block: the latest useful TFMS data matched to the flight.
- `tbfmSummary` block: the latest useful TBFM metering data matched to the flight, including meter fix, ETA at the meter fix, runway ETA, estimated departure time, runway assignment, miles-in-trail, and the originating TMA facility.
- `sfdpsSummary` block: the latest useful SFDPS data matched to the flight, including FDPS status, departure/arrival airports, actual/estimated runway times, latest lat/lon, altitude, speed, and controlling unit/sector.
- `stddsSummary` block: the latest useful STDDS data matched to the flight, including terminal source, track number, MRT time, latest lat/lon/altitude, beacon code, aircraft type, runway, and entry/exit fixes.

## TFDM update behavior

TFDM `FlightAdd` / `FlightUpdate` messages are merged into the existing `flights` record by `tfdm_id`, `tfm_id`, or `flight_plan_identifier` so that partial updates do not erase nested fields like `departure` / `arrival` details.

## TFMS summary in flight records

When a TFMS message is linked to a TFDM flight, the `flights` record is updated with a `tfmsSummary` containing the latest useful information, such as:

- `first_igtd` — the first observed initial gate/ground time of departure; retained permanently.
- `latest_igtd`, `latest_eta`, `latest_etd` — the most recent TFMS times.
- `latest_flight_status`, `latest_aircraft_model` — from `FlightTimes`.
- `latest_position` — from `trackInformation`.
- `latest_route_text` — from `flightPlanAmendmentInformation`.
- `tfm_id`, `gufi`, `tfms_message_count`.

The first `igtd` is only set once and is not overwritten by later messages.

## TBFM summary in flight records

When a TBFM message is linked to a TFDM flight, the `flights` record is updated with a `tbfmSummary` containing:

- `tbfm_message_count` — count of linked TBFM messages.
- `latest_env_srce`, `latest_tma_id`, `latest_air_type` — originating TMA facility and message type.
- `latest_meter_fix`, `latest_meter_fix_eta` — the metering fix and its latest ETA.
- `latest_runway_eta` — latest runway arrival ETA.
- `latest_etd` — latest estimated departure time.
- `latest_tbfm_runway` — runway designator from TBFM.
- `latest_miles_in_trail` — any miles-in-trail / metering restriction text.

## SFDPS summary in flight records

When an SFDPS message is linked to a TFDM flight, the `flights` record is updated with an `sfdpsSummary` containing:

- `sfdps_message_count` — count of linked SFDPS messages.
- `latest_source_time_stamp` — latest message timestamp.
- `latest_fdps_flight_status` — `ACTIVE`, `SCHEDULED`, etc.
- `latest_departure_airport`, `latest_arrival_airport`.
- `latest_actual_departure_time`, `latest_estimated_arrival_time`.
- `latest_position_lat`, `latest_position_lon`, `latest_altitude`, `latest_speed`.
- `latest_controlling_unit`, `latest_sector`, `latest_centre`, `latest_source`, `latest_system`.
- `gufi`, `flight_number`.

## STDDS summary in flight records

When an STDDS track record is linked to a TFDM flight, the `flights` record is updated with an `stddsSummary` containing:

- `stdds_message_count` — count of linked STDDS records.
- `latest_src` — terminal source (e.g. `PCT`, `ROA`).
- `latest_track_num`, `latest_mrt_time`, `latest_track_status`.
- `latest_lat`, `latest_lon`, `latest_altitude`.
- `latest_reported_beacon_code`, `latest_assigned_beacon_code`.
- `latest_ac_type`, `latest_runway`, `latest_entry_fix`, `latest_exit_fix`.
- `gufi`, `flight_number`, `departure_airport`, `arrival_airport`.

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

## Subscriptions and webhooks

An admin creates client credentials through `/admin`. A client authenticates with `POST /api/v1/auth/token` (HTTP Basic), then creates subscriptions with `POST /api/v1/subscriptions`.

When a SWIM message causes the flight snapshot to change, the dispatcher sends a `FLIGHT_UPDATED` webhook to the subscription's `webhook_url`:

```json
{
  "subscription_id": "...",
  "event_type": "FLIGHT_UPDATED",
  "flight": { ... same as /flights/{flight_number} ... },
  "previous_flight": { ... or null ... }
}
```

The diff ignores `_id`, `createdAt`, `updatedAt`, and `status`. A `status` of `deleted` always triggers a final `FLIGHT_UPDATED` before the subscription is removed.

Webhooks are throttled to one per flight per second. Failed deliveries are retried up to 5 times with exponential backoff. A subscription is removed after 10 consecutive delivery failures, 2 hours with no SWIM messages, or when the client calls `DELETE /api/v1/subscriptions/{subscription_id}`.

A separate `swim-tfdm-consumer-dispatcher` deployment (one replica, `RUN_MODE=dispatcher`) watches the MongoDB `flights` collection for changes. If MongoDB change streams are unavailable, it falls back to polling.

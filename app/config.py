from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    mongodb_url: str = "mongodb://10.0.0.16:27017,10.1.1.27:27017/?replicaSet=rs0"
    mongodb_db: str = "swim_tfdm_consumer"
    mongodb_collection: str = "flights"
    mongodb_tfms_collection: str = "tfms_messages"
    mongodb_tbfm_collection: str = "tbfm_messages"
    mongodb_sfdps_collection: str = "sfdps_messages"
    mongodb_stdds_collection: str = "stdds_messages"

    kafka_bootstrap_servers: str = "10.0.0.94:9092"
    kafka_topic: str = "faa-tfdm-raw"
    kafka_group_id: str = "swim-tfdm-consumer"
    kafka_tfms_topic: str = "faa-tfms-raw"
    kafka_tfms_group_id: str = "swim-tfms-consumer"
    kafka_tbfm_topic: str = "faa-tbfm-raw"
    kafka_tbfm_group_id: str = "swim-tbfm-consumer-v3"
    kafka_sfdps_topic: str = "faa-sfdps-raw"
    kafka_sfdps_group_id: str = "swim-sfdps-consumer-v3"
    kafka_stdds_topic: str = "faa-stdds-raw"
    kafka_stdds_group_id: str = "swim-stdds-consumer-v2"
    kafka_auto_offset_reset: str = "latest"

    app_host: str = "0.0.0.0"
    app_port: int = 8000
    run_mode: str = "api"  # "api" or "dispatcher"

    jwt_secret: str = "change-me-please-replace-this-in-production-with-a-secure-secret"
    jwt_algorithm: str = "HS256"
    jwt_expiry_hours: int = 24

    admin_session_secret: str = "change-me-admin-session-please-replace-in-production"
    admin_session_max_age: int = 3600  # 1 hour

    inactivity_timeout_min: int = 120
    preflight_timeout_min: int = 1440  # 24 hours
    inactivity_scan_interval_min: int = 15

    webhook_timeout_seconds: float = 10.0
    webhook_retries: int = 5
    webhook_retry_base_seconds: int = 5
    webhook_throttle_seconds: float = 1.0


settings = Settings()

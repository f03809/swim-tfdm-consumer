from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    mongodb_url: str = "mongodb://10.0.0.16:27017"
    mongodb_db: str = "swim_tfdm_consumer"
    mongodb_collection: str = "flights"
    mongodb_tfms_collection: str = "tfms_messages"
    mongodb_tbfm_collection: str = "tbfm_messages"

    kafka_bootstrap_servers: str = "10.0.0.94:9092"
    kafka_topic: str = "faa-tfdm-raw"
    kafka_group_id: str = "swim-tfdm-consumer"
    kafka_tfms_topic: str = "faa-tfms-raw"
    kafka_tfms_group_id: str = "swim-tfms-consumer"
    kafka_tbfm_topic: str = "faa-tbfm-raw"
    kafka_tbfm_group_id: str = "swim-tbfm-consumer"
    kafka_auto_offset_reset: str = "latest"

    app_host: str = "0.0.0.0"
    app_port: int = 8000


settings = Settings()

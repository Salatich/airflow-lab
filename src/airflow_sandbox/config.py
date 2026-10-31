"""Application configuration using Pydantic Settings."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration for the Airflow Sandbox environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Airflow
    airflow_image_name: str = Field(default="apache/airflow:2.10.5", alias="AIRFLOW_IMAGE_NAME")
    airflow_uid: int = Field(default=50000, alias="AIRFLOW_UID")
    airflow_gid: int = Field(default=0, alias="AIRFLOW_GID")
    airflow_webserver_port: int = Field(default=8080)
    airflow_admin_user: str = Field(default="airflow", alias="AIRFLOW_WWW_USER_USERNAME")
    airflow_admin_password: str = Field(default="airflow", alias="AIRFLOW_WWW_USER_PASSWORD")

    # PostgreSQL
    postgres_user: str = Field(default="airflow", alias="POSTGRES_USER")
    postgres_password: str = Field(default="airflow", alias="POSTGRES_PASSWORD")
    postgres_db: str = Field(default="airflow", alias="POSTGRES_DB")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    sql_alchemy_conn: str = Field(
        default="postgresql+psycopg2://airflow:airflow@localhost:5432/airflow",
        alias="AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    )

    # MinIO / S3
    minio_endpoint: str = Field(default="http://localhost:9000", alias="AWS_ENDPOINT_URL")
    minio_access_key: str = Field(default="minioadmin", alias="AWS_ACCESS_KEY_ID")
    minio_secret_key: str = Field(default="minioadmin", alias="AWS_SECRET_ACCESS_KEY")
    minio_bucket_name: str = Field(default="airflow-xcom-bucket", alias="MINIO_BUCKET_NAME")
    minio_region: str = Field(default="us-east-1", alias="AWS_DEFAULT_REGION")

    # XCom
    xcom_size_threshold_bytes: int = Field(default=1024, alias="XCOM_SIZE_THRESHOLD_BYTES")
    xcom_s3_prefix: str = Field(default="xcom/", alias="XCOM_S3_KEY_PREFIX")


settings = Settings()

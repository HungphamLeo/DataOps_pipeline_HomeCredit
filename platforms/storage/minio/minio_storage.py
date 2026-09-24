"""
MinIO Storage Backend
=====================
Concrete implementation of IObjectStorage for MinIO / S3-compatible stores.

Responsibilities (SRP):
  - Upload / download bytes (parquet files, JSON, etc.)
  - Ensure bucket exists
  - List objects under a prefix
  - Write a Polars DataFrame as Parquet directly to MinIO

NOT responsible for:
  - Data transformation (that belongs to processing layer)
  - Connection pooling of DB (separate concern)
  - Diagnostic reporting (use probe scripts separately)
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import List, Optional

from platforms.storage.base_storage import IObjectStorage
from log.config.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

class MinioStorageBackend(IObjectStorage):
    """
    MinIO / S3-compatible object storage backend.

    Depends on `minio` package (already in requirements.txt).
    Credentials are injected via constructor — never read from env here.
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Parameters
        ----------
        endpoint   : HOST:PORT — e.g. "localhost:9000".  Do NOT include http://.
        access_key : MinIO / S3 access key.
        secret_key : MinIO / S3 secret key.
        secure     : Use TLS (False for local dev).
        """
        self.logger = logger or logger_manager.get_logger(__name__)
        try:
            from minio import Minio
            self._client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
            )
            self.logger.info("minio_client_initialized endpoint=%s secure=%s", endpoint, secure)
        except Exception as e:
            self.logger.exception(
                "minio_client_initialization_failed endpoint=%s error=%s", endpoint, e
            )
            raise

    # ------------------------------------------------------------------
    # IObjectStorage implementation
    # ------------------------------------------------------------------

    def ensure_bucket(self, bucket: str) -> None:
        """Create bucket if it does not exist."""
        try:
            if not self._client.bucket_exists(bucket):
                self._client.make_bucket(bucket)
                self.logger.info("minio_bucket_created bucket=%s", bucket)
            else:
                self.logger.debug("minio_bucket_exists bucket=%s", bucket)
        except Exception as e:
            self.logger.exception("minio_bucket_check_failed bucket=%s error=%s", bucket, e)
            raise

    def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Upload raw bytes to MinIO."""
        try:
            self.ensure_bucket(bucket)
            buf = io.BytesIO(data)
            self._client.put_object(
                bucket_name=bucket,
                object_name=key,
                data=buf,
                length=len(data),
                content_type=content_type,
            )
            self.logger.info(
                "minio_upload_completed bucket=%s key=%s bytes=%d",
                bucket, key, len(data),
            )
        except Exception as e:
            self.logger.exception("minio_upload_failed bucket=%s key=%s error=%s", bucket, key, e)
            raise

    def download_bytes(self, bucket: str, key: str) -> bytes:
        """Download object content as bytes."""
        try:
            response = self._client.get_object(bucket, key)
            try:
                data = response.read()
                self.logger.info(
                    "minio_download_completed bucket=%s key=%s bytes=%d",
                    bucket, key, len(data),
                )
                return data
            finally:
                response.close()
                response.release_conn()
        except Exception as e:
            self.logger.exception("minio_download_failed bucket=%s key=%s error=%s", bucket, key, e)
            raise

    def list_objects(self, bucket: str, prefix: str) -> List[str]:
        """Return list of object keys under *prefix* (recursive)."""
        try:
            objects = self._client.list_objects(bucket, prefix=prefix, recursive=True)
            keys = [obj.object_name for obj in objects]
            self.logger.info(
                "minio_list_completed bucket=%s prefix=%s objects=%d",
                bucket, prefix, len(keys),
            )
            return keys
        except Exception as e:
            self.logger.exception("minio_list_failed bucket=%s prefix=%s error=%s", bucket, prefix, e)
            raise

    def object_exists(self, bucket: str, key: str) -> bool:
        """Return True if the object exists in the bucket."""
        from minio.error import S3Error

        try:
            self._client.stat_object(bucket, key)
            self.logger.debug("minio_object_exists bucket=%s key=%s exists=true", bucket, key)
            return True
        except S3Error:
            self.logger.debug("minio_object_exists bucket=%s key=%s exists=false", bucket, key)
            return False
        except Exception as e:
            self.logger.exception("minio_object_exists_failed bucket=%s key=%s error=%s", bucket, key, e)
            raise

    # ------------------------------------------------------------------
    # Polars / Parquet helpers
    # ------------------------------------------------------------------

    def write_dataframe_parquet(
        self,
        df: "polars.DataFrame",  # type: ignore[name-defined]
        bucket: str,
        key: str,
    ) -> str:
        """
        Serialise a Polars DataFrame to Parquet and upload to MinIO.

        Returns the full object path: ``{bucket}/{key}``
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        buf = io.BytesIO()
        pq.write_table(df.to_arrow(), buf)
        buf.seek(0)
        raw = buf.read()
        self.upload_bytes(bucket, key, raw, content_type="application/parquet")
        path = f"{bucket}/{key}"
        self.logger.info("[MinIO] Wrote parquet %s (%d bytes)", path, len(raw))
        return path

    def read_dataframe_parquet(
        self,
        bucket: str,
        key: str,
    ) -> "polars.DataFrame":  # type: ignore[name-defined]
        """Download a Parquet file from MinIO and return a Polars DataFrame."""
        import polars as pl

        raw = self.download_bytes(bucket, key)
        return pl.read_parquet(io.BytesIO(raw))

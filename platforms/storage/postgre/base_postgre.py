"""PostgreSQL storage backend with pooled connections and structured logging."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import pool as pg_pool

from log.config.logger_setup import logger_manager
from platforms.storage.base_storage import IRelationalStorage, StorageBackend


class PostgreSQLWriter(IRelationalStorage):
    """Thread-safe PostgreSQL writer backed by a connection pool."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        pool_min: int = 1,
        pool_max: int = 5,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = username
        self.password = password
        self.logger = logger or logger_manager.get_logger(__name__)
        self._pool = pg_pool.ThreadedConnectionPool(
            pool_min,
            pool_max,
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
        )
        self.logger.info(
            "postgres_pool_initialized host=%s port=%s database=%s pool_min=%s pool_max=%s",
            host,
            port,
            database,
            pool_min,
            pool_max,
        )

    @contextmanager
    def _conn(self):
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def close(self) -> None:
        self._pool.closeall()
        self.logger.info("postgres_pool_closed database=%s", self.database)

    def execute(self, sql: str, params: Optional[tuple] = None) -> Dict[str, Any]:
        try:
            with self._conn() as conn, conn.cursor() as cursor:
                cursor.execute(sql, params)
            self.logger.debug("postgres_execute_completed")
            return {"ok": True}
        except Exception as exc:
            self.logger.error("postgres_execute_failed error=%s", exc, exc_info=True)
            return {"ok": False, "error": str(exc)}

    def query(self, sql: str, params: Optional[tuple] = None) -> Dict[str, Any]:
        try:
            with self._conn() as conn, conn.cursor() as cursor:
                cursor.execute(sql, params)
                columns = [item[0] for item in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            self.logger.debug("postgres_query_completed rows=%d", len(rows))
            return {"ok": True, "results": rows}
        except Exception as exc:
            self.logger.error("postgres_query_failed error=%s", exc, exc_info=True)
            return {"ok": False, "error": str(exc), "results": []}

    def insert(self, target: str, data: Any) -> Dict[str, Any]:
        if not data:
            return {"ok": True, "inserted_count": 0}
        records = data if isinstance(data, list) else [data]
        keys = list(records[0].keys())
        columns = ", ".join(f'"{key}"' for key in keys)
        placeholders = ", ".join(f"%({key})s" for key in keys)
        sql = f'INSERT INTO {target} ({columns}) VALUES ({placeholders})'
        try:
            with self._conn() as conn, conn.cursor() as cursor:
                cursor.executemany(sql, records)
            self.logger.info("postgres_insert_completed table=%s rows=%d", target, len(records))
            return {"ok": True, "inserted_count": len(records)}
        except Exception as exc:
            self.logger.error("postgres_insert_failed table=%s error=%s", target, exc, exc_info=True)
            return {"ok": False, "inserted_count": 0, "error": str(exc)}

    def upsert(
        self,
        target: str,
        data: List[Dict[str, Any]],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        if not data:
            return {"ok": True, "upserted_count": 0}
        keys = list(data[0].keys())
        columns = ", ".join(f'"{key}"' for key in keys)
        placeholders = ", ".join(f"%({key})s" for key in keys)
        conflicts = ", ".join(f'"{key}"' for key in conflict_columns)
        updates = update_columns or [key for key in keys if key not in conflict_columns]
        action = "DO NOTHING" if not updates else "DO UPDATE SET " + ", ".join(
            f'"{key}" = EXCLUDED."{key}"' for key in updates
        )
        sql = (
            f'INSERT INTO {target} ({columns}) VALUES ({placeholders}) '
            f'ON CONFLICT ({conflicts}) {action}'
        )
        try:
            with self._conn() as conn, conn.cursor() as cursor:
                cursor.executemany(sql, data)
            self.logger.info("postgres_upsert_completed table=%s rows=%d", target, len(data))
            return {"ok": True, "upserted_count": len(data)}
        except Exception as exc:
            self.logger.error("postgres_upsert_failed table=%s error=%s", target, exc, exc_info=True)
            return {"ok": False, "upserted_count": 0, "error": str(exc)}

    def bulk_insert(self, table: str, data: Any) -> Dict[str, Any]:
        if hasattr(data, "to_dict"):
            data = data.to_dict("records")
        return self.insert(table, data)

    def ensure_schema(self, schema: str) -> Dict[str, Any]:
        return self.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    def execute_script(self, sql_script: str) -> Dict[str, Any]:
        errors = []
        for statement in (item.strip() for item in sql_script.split(";")):
            if statement:
                result = self.execute(statement)
                if not result["ok"]:
                    errors.append(result["error"])
        return {"ok": not errors, "errors": errors}


class PostgreSQLStorageBackend(StorageBackend):
    """Compatibility adapter exposing PostgreSQLWriter through StorageBackend."""

    def __init__(
        self,
        pipeline_logger: Optional[logging.Logger],
        postgres_writer: PostgreSQLWriter,
    ) -> None:
        self.pg = postgres_writer
        self.logger = pipeline_logger or logger_manager.get_logger(__name__)

    def save(self, dataset_name: str, data: Any, fmt: Optional[str] = None) -> Dict[str, Any]:
        try:
            result = self.pg.insert(dataset_name, data)
            return {"ok": True, **result}
        except Exception as exc:
            self.logger.exception("postgres_storage_save_failed table=%s", dataset_name)
            return {"ok": False, "error": str(exc)}

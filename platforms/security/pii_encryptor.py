"""
PII Encryption Engine — Vectorized AES-256-GCM via PySpark Pandas UDF.
Hỗ trợ Crypto-Shredding và bảo mật định danh người dùng.
"""
from __future__ import annotations

import base64
import os
import pandas as pd
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

# Master Key 256-bit (32 bytes) — Production lấy từ HashiCorp Vault / AWS KMS
_MASTER_KEY_HEX = os.getenv("PII_MASTER_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
_MASTER_KEY = bytes.fromhex(_MASTER_KEY_HEX)


class PIIEncryptor:
    """Xử lý mã hóa và giải mã AES-256-GCM."""

    def __init__(self, key: bytes = _MASTER_KEY):
        self.aesgcm = AESGCM(key)

    def encrypt_val(self, val: str | None) -> str | None:
        if val is None or str(val).strip() == "":
            return None
        # Nonce 12-byte chuẩn GCM
        nonce = os.urandom(12)
        ct = self.aesgcm.encrypt(nonce, str(val).encode("utf-8"), None)
        # Ghép nonce + ciphertext và encode Base64
        return base64.b64encode(nonce + ct).decode("utf-8")

    def decrypt_val(self, token: str | None) -> str | None:
        if token is None or str(token).strip() == "":
            return None
        try:
            raw = base64.b64decode(token.encode("utf-8"))
            nonce, ct = raw[:12], raw[12:]
            return self.aesgcm.decrypt(nonce, ct, None).decode("utf-8")
        except Exception:
            # Khi Key đã bị xoá (Crypto-Shredded) hoặc token bị hỏng
            return "[SHREDDED_DATA]"


_default_encryptor = PIIEncryptor()


@pandas_udf(StringType())
def encrypt_pii_vectorized(series: pd.Series) -> pd.Series:
    """Spark Vectorized UDF: Mã hóa cả series dữ liệu tốc độ cao qua Apache Arrow."""
    return series.apply(_default_encryptor.encrypt_val)


@pandas_udf(StringType())
def decrypt_pii_vectorized(series: pd.Series) -> pd.Series:
    """Spark Vectorized UDF: Giải mã series dữ liệu."""
    return series.apply(_default_encryptor.decrypt_val)
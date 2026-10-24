import pytest
from platforms.security.pii_encryptor import PIIEncryptor

def test_encrypt_decrypt_roundtrip():
    encryptor = PIIEncryptor()
    original_value = "150000.0"
    
    encrypted = encryptor.encrypt_val(original_value)
    assert encrypted != original_value
    assert isinstance(encrypted, str)
    
    decrypted = encryptor.decrypt_val(encrypted)
    assert decrypted == original_value

def test_crypto_shredding_behavior():
    # Khách hàng có Key A
    key_a = b"12345678901234567890123456789012"
    encryptor_a = PIIEncryptor(key=key_a)
    encrypted_data = encryptor_a.encrypt_val("sensitive_passport_id")

    # Giả lập Key A bị hủy (KMS xóa key, dùng Key khác để thử giải mã)
    key_b = b"99999999999999999999999999999999"
    encryptor_b = PIIEncryptor(key=key_b)
    
    # Giải mã thất bại -> Trả về token đã bị shred
    result = encryptor_b.decrypt_val(encrypted_data)
    assert result == "[SHREDDED_DATA]"
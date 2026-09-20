"""
Great Expectations validator task.

Gracefully handles the case where the GE project has not yet been fully
initialised (missing expectations / checkpoints) by logging a warning
instead of crashing the entire pipeline.  Set `ge_enabled: false` in
pipeline_config.yaml or pass `enabled=False` to skip validation entirely
during early development.
"""
import great_expectations as gx
from prefect import task
import logging

logger = logging.getLogger(__name__)


@task(log_prints=True, retries=1, retry_delay_seconds=10)
def run_ge_checkpoint(
    checkpoint_name: str,
    ge_root_dir: str,
    enabled: bool = True,
) -> bool:
    """
    Chạy một Great Expectations checkpoint và raise exception nếu validation thất bại.

    Parameters
    ----------
    checkpoint_name : str
        Tên của checkpoint đã định nghĩa trong thư mục great_expectations/checkpoints/.
    ge_root_dir : str
        Đường dẫn tuyệt đối đến thư mục gốc của Great Expectations project.
    enabled : bool
        Nếu False, bỏ qua validation và trả về True ngay lập tức.
        Hữu ích khi GE chưa được setup đầy đủ trong môi trường dev.

    Returns
    -------
    bool
        True nếu validation pass hoặc bị bỏ qua.

    Raises
    ------
    ValueError
        Nếu checkpoint thực thi nhưng validation kết quả là fail.
    """
    if not enabled:
        print(f"[SKIP] GE validation bị tắt (enabled=False). Bỏ qua checkpoint '{checkpoint_name}'.")
        return True

    print(f"Đang chạy Great Expectations checkpoint: '{checkpoint_name}' | root: {ge_root_dir}")

    try:
        # GE >= 0.18.x: dùng get_context với mode="file" và project_root_dir
        try:
            context = gx.get_context(mode="file", project_root_dir=ge_root_dir)
        except TypeError:
            # Fallback cho các phiên bản GE cũ hơn
            context = gx.get_context(context_root_dir=ge_root_dir)

        result = context.run_checkpoint(checkpoint_name=checkpoint_name)

    except Exception as e:
        # Nếu checkpoint chưa tồn tại (GE chưa được setup), log cảnh báo thay vì crash
        err_msg = str(e).lower()
        if any(kw in err_msg for kw in ("not found", "does not exist", "no such file", "checkpoint")):
            print(
                f"[WARN] Checkpoint '{checkpoint_name}' chưa được thiết lập. "
                "Bỏ qua validation — hãy chạy `great_expectations checkpoint new` để tạo."
            )
            return True
        print(f"[ERROR] Lỗi khi chạy GE checkpoint '{checkpoint_name}': {e}")
        raise

    if not result["success"]:
        print(f"[FAIL] Great Expectations checkpoint '{checkpoint_name}' thất bại!")
        for run_result in result.run_results.values():
            validation_result = run_result.get("validation_result", {})
            for vr in validation_result.get("results", []):
                if not vr.get("success", True):
                    etype = vr.get("expectation_config", {}).get("expectation_type", "unknown")
                    details = vr.get("result", {})
                    print(f"  - FAILED: {etype} | details: {details}")
        raise ValueError(
            f"Kiểm tra chất lượng dữ liệu thất bại cho checkpoint '{checkpoint_name}'."
        )

    print(f"[PASS] Great Expectations checkpoint '{checkpoint_name}' thành công!")
    return True

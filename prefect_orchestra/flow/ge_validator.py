import great_expectations as gx
from prefect import task


@task(log_prints=True, retries=1, retry_delay_seconds=10)
def run_ge_checkpoint(checkpoint_name: str, ge_root_dir: str):
    """
    Chạy một Great Expectations checkpoint và raise exception nếu validation thất bại.

    Tương thích với GE >= 1.x (gx.get_context với context_root_dir).
    """
    print(f"Đang chạy Great Expectations checkpoint: '{checkpoint_name}'")

    try:
        context = gx.get_context(context_root_dir=ge_root_dir)
        result = context.run_checkpoint(checkpoint_name=checkpoint_name)
    except Exception as e:
        print(f"Lỗi khi chạy GE checkpoint '{checkpoint_name}': {e}")
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

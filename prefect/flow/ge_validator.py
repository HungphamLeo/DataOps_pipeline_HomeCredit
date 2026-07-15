import great_expectations as ge
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from prefect import task


@task(log_prints=True, retries=1)
def run_ge_checkpoint(checkpoint_name: str, ge_root_dir: str):
    """
    Chạy một Great Expectations checkpoint và raise exception nếu thất bại.
    """
    print(f"Đang chạy Great Expectations checkpoint: {checkpoint_name}")
    
    try:
        context = ge.get_context(context_root_dir=ge_root_dir)
        result: CheckpointResult = context.run_checkpoint(checkpoint_name=checkpoint_name)
    except Exception as e:
        print(f"Lỗi khi chạy GE checkpoint '{checkpoint_name}': {e}")
        raise

    if not result.success:
        print(f"Great Expectations checkpoint '{checkpoint_name}' thất bại!")
        for run_result in result.run_results.values():
            for validation_result in run_result["validation_result"]["results"]:
                if not validation_result["success"]:
                    print(f"- Expectation Failed: {validation_result['expectation_config']['expectation_type']}")
                    print(f"  - Details: {validation_result.get('result', {})}")
        raise ValueError(f"Kiểm tra chất lượng dữ liệu thất bại cho checkpoint '{checkpoint_name}'.")

    print(f"Great Expectations checkpoint '{checkpoint_name}' đã thành công!")
    return True
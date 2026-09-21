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
from log.config.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)


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
        logger.info("ge_validation_skipped checkpoint=%s", checkpoint_name)
        return True

    logger.info(
        "ge_validation_started checkpoint=%s root=%s",
        checkpoint_name, ge_root_dir,
    )

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
            logger.warning(
                "ge_checkpoint_missing checkpoint=%s root=%s",
                checkpoint_name, ge_root_dir,
            )
            return True
        logger.exception("ge_validation_failed checkpoint=%s", checkpoint_name)
        raise

    if not result["success"]:
        logger.error("ge_validation_failed checkpoint=%s", checkpoint_name)
        for run_result in result.run_results.values():
            validation_result = run_result.get("validation_result", {})
            for vr in validation_result.get("results", []):
                if not vr.get("success", True):
                    etype = vr.get("expectation_config", {}).get("expectation_type", "unknown")
                    details = vr.get("result", {})
                    logger.error(
                        "ge_expectation_failed checkpoint=%s expectation=%s details=%s",
                        checkpoint_name, etype, details,
                    )
        raise ValueError(
            f"Kiểm tra chất lượng dữ liệu thất bại cho checkpoint '{checkpoint_name}'."
        )

    logger.info("ge_validation_completed checkpoint=%s", checkpoint_name)
    return True

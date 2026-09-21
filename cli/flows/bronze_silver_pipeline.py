"""Backward-compatible entrypoint for the simplified Bronze-to-Staging flow."""

from cli.flows.serving.bronze.bronze_flow import bronze_ingest_flow
from cli.flows.serving.staging.staging_flow import staging_transform_flow


def bronze_to_silver_flow() -> list[str]:
    """Run the active metadata-driven pipeline.

    The old implementation contained a hard-coded source list and business
    transformations. Keeping this small wrapper avoids breaking old deploy
    references while ensuring there is only one active implementation.
    """
    bronze_ingest_flow()
    return staging_transform_flow()


if __name__ == "__main__":
    bronze_to_silver_flow()

from typing import Any, Dict


def get_provider_info() -> Dict[str, Any]:
    return {
        "package-name": "airflow-provider-flyte",
        "name": "Flyte Airflow Provider",
        "description": "A Flyte provider for Apache Airflow.",
        "hook-class-names": ["flyte_provider.hooks.flyte.FlyteHook"],
        "extra-links": ["flyte_provider.operators.flyte.RegistryLink"],
        "versions": ["0.0.1"],
    }

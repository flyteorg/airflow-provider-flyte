from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

"""Perform the package airflow-provider-flyte setup."""
setup(
    name="airflow-provider-flyte",
    version="0.0.1",
    description="Flyte Airflow Provider",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=flyte_provider.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=[
        "flyte_provider",
        "flyte_provider.hooks",
        "flyte_provider.sensors",
        "flyte_provider.operators",
    ],
    install_requires=["apache-airflow>=2.0", "flytekit>=1.0.0"],
    setup_requires=["setuptools", "wheel"],
    author="Samhita Alla",
    author_email="samhita@union.ai",
    url="https://flyte.org/",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires="~=3.7",
)

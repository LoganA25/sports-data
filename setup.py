import glob
from setuptools import find_packages, setup

setup(
    name="sports",
    packages=find_packages(exclude=[""]),
    # package data paths are relative to the package key
    package_data={
        "sports": ["../" + path for path in glob.glob("dbt_project/**/*", recursive=True)]
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "dagster-dbt",
        "pandas",
        "numpy",
        "scipy",
        "dbt-core",
        "dbt-duckdb",
        "dagster-duckdb",
        "dagster-duckdb-pandas",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)

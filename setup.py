from setuptools import setup, find_packages
from pathlib import Path

setup(
    name="event-driven-cpssw",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src", include=["glue_etl", "glue_etl.*"]),
    install_requires=[
        "boto3>=1.34.0",
        "pyspark>=3.3.0",
        "awswrangler>=3.0.0"
    ],
    author="Dipak Vaidya",
    author_email="your.email@example.com",
    description="AWS Glue ETL pipeline for Sales Order",
    url="https://github.com/03dipak/glue_etl_pipeline",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    include_package_data=True,
    zip_safe=False,
)
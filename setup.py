#!/usr/bin/env python

from setuptools import setup

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dask-deltatable",
    version="0.3rc",
    description="Dask + Delta Table ",
    maintainer="rajagurunath",
    maintainer_email="gurunathrajagopal@gmail.com",
    license="BSD-3-Clause",
    packages=["dask_deltatable"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    extras_require={
        "dev": ["pytest", "requests", "pytest-cov>=2.10.1"],
        "s3": ["s3fs", "boto3"],
    },
    package_data={"dask_deltatable": ["*.pyi" "__init__.pyi", "core.pyi"]},
    include_package_data=True,
    zip_safe=False,
)

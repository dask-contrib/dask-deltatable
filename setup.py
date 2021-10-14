#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dask-deltatable",
    version="0.2",
    description="Dask + Delta Table ",
    maintainer="rajagurunath",
    maintainer_email="gurunathrajagopal@gmail.com",
    license="MIT",
    packages=["dask_deltatable"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    extras_require={"dev": ["pytest", "requests", "pytest-cov>=2.10.1"]},
    include_package_data=True,
    zip_safe=False,
)

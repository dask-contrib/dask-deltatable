#!/usr/bin/env python

from __future__ import annotations

from setuptools import setup

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dask-deltatable",
    version="0.3",
    description="Dask + Delta Table ",
    maintainer="rajagurunath",
    maintainer_email="gurunathrajagopal@gmail.com",
    license="BSD-3-Clause",
    packages=["dask_deltatable"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.9",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    extras_require={
        "dev": ["pytest", "requests", "pytest-cov>=2.10.1"],
        "s3": ["s3fs", "boto3"],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Database",
        "Topic :: Scientific/Engineering",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    include_package_data=True,
    zip_safe=False,
)

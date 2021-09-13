#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dask-deltatable",
    version="2021.7.3",
    description="Dask + Delta Table ",
    license="BSD",
    packages=["dask_deltatable"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    extras_require={"dev": ["pytest","requests"]},
    include_package_data=True,
    zip_safe=False,
)

from setuptools import setup, find_packages

from __version__ import version

with open("../README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="bigeye_airflow",
    version=version,
    author="Bigeye",
    author_email="support@bigeye.com",
    description="Airflow operators to be used with Bigeye",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/torodata/toro-airflow",
    packages=find_packages(exclude=['tests', 'astro']),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)

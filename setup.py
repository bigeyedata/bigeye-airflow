import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bigeye-airflow",
    version="0.0.7",
    author="Bigeye",
    author_email="support@bigeye.com",
    description="Airflow operators to be used with Bigeye",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/torodata/toro-airflow",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

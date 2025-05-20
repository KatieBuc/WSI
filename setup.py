from setuptools import setup, find_packages

setup(
    name="wsi",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "numpy",
        "pandas",
        "matplotlib",
    ],
    author="Katie Buchhorn",
    description="Python package for parsing and formulating the Womens Safety Index",
)

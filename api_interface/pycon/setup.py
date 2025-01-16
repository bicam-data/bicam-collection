from setuptools import setup, find_packages
setup(
    name="pycon",
    version="0.1.0",
    author="Ryan Delano",
    description="A Python library for accessing congressional data",
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        "aiohttp",
        "asyncpg",
        "python-dotenv",
        "pyyaml",
        "ujson",
    ]
)
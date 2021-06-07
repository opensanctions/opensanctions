from setuptools import setup, find_packages


setup(
    name="opensanctions",
    version="3.0.0",
    url="https://github.com/alephdata/opensanctions",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples", "test"]),
    namespace_packages=[],
    package_data={"opensanctions": ["metadata/*.yml", "metadata/*.yaml"]},
    zip_safe=False,
    install_requires=[
        "followthemoney >= 1.21.2",
        "sqlalchemy",
        "alembic",
        "followthemoney-store[postgresql] >= 3.0.1, < 4.0.0",
        "requests[security] >= 2.25.0, < 3.0.0",
        "requests_cache >= 0.6.3, < 0.7.0",
        "alephclient >= 2.1.3",
        "structlog",
        "colorama",
        "xlrd",
        "lxml",
    ],
    entry_points={
        "console_scripts": [
            "opensanctions = opensanctions.cli:cli",
            "osanc = opensanctions.cli:cli",
        ],
    },
)

from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="opensanctions",
    version="3.1.1",
    url="https://opensanctions.org",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="OpenSanctions",
    author_email="info@opensanctions.org",
    packages=find_packages(exclude=["ez_setup", "examples", "test"]),
    namespace_packages=[],
    package_data={
        "opensanctions": [
            "**/*.yml",
            "**/*.yaml",
            "**/*.ini",
            "**/*.mako",
            "**/*.ijson",
            "migrate/versions/*.py",
        ]
    },
    zip_safe=False,
    install_requires=[
        "followthemoney >= 1.21.2",
        "nomenklatura >= 0.0.1",
        "pantomime",
        "sqlalchemy",
        "alembic",
        "certifi",
        "addressformatting",
        "prefixdate",
        "psycopg2-binary",
        "requests[security] >= 2.25.0, < 3.0.0",
        "requests_cache >= 0.8.1, < 0.10.0",
        "datapatch",
        "structlog",
        "colorama",
        "xlrd",
        "lxml",
    ],
    extras_require={
        "dev": [
            "sphinx",
            "bump2version",
            "wheel>=0.29.0",
            "twine",
            "black",
            "flake8>=2.6.0",
            "sphinx-rtd-theme",
        ],
    },
    entry_points={
        "console_scripts": [
            "opensanctions = opensanctions.cli:cli",
            "osanc = opensanctions.cli:cli",
        ],
    },
)

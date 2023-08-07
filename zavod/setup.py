from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="zavod",
    version="0.8.0",
    description="Data factory for followthemoney data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="data mapping identity followthemoney etl parsing",
    author="OpenSanctions",
    author_email="friedrich@opensanctions.org",
    url="https://github.com/opensanctions/opensanctions",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    namespace_packages=[],
    include_package_data=True,
    package_data={"": ["zavod/data/*", "zavod/py.typed"]},
    zip_safe=False,
    install_requires=[
        "followthemoney == 3.5.1",
        "nomenklatura == 3.3.7",
        "datapatch == 1.0.2",
        "addressformatting == 1.3.2",
        "certifi",
        "colorama",
        "google-cloud-storage",
        "lxml == 4.9.3",
        "lxml-stubs == 0.4.0",
        "openpyxl == 3.1.2",
        "orjson == 3.9.3",
        "pantomime == 0.6.1",
        "plyvel == 1.5.0",
        "prefixdate",
        "psycopg2-binary",
        "pyicu < 2.12.0",
        "requests[security]",
        "sqlalchemy[mypy]",
        "structlog",
        "xlrd == 2.0.1",
    ],
    tests_require=[],
    entry_points={
        "console_scripts": [
            "zavod = zavod.cli:cli",
        ],
    },
    extras_require={
        "dev": [
            "wheel>=0.29.0",
            "twine",
            "mypy",
            "flake8>=2.6.0",
            "pytest",
            "pytest-cov",
            "lxml-stubs",
            "coverage>=4.1",
            "requests-mock",
            "types-setuptools",
            "types-requests",
            "types-google-cloud-ndb",
        ],
        "docs": [
            "pillow",
            "cairosvg",
            "mkdocs",
            "mkdocstrings[python]",
            "mkdocs-material",
        ],
    },
)

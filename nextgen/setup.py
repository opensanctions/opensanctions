from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="zavod",
    version="0.7.4",
    description="Data factory for followthemoney data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="data mapping identity followthemoney etl parsing",
    author="Friedrich Lindenberg",
    author_email="friedrich@opensanctions.org",
    url="https://github.com/opensanctions/zavod",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    namespace_packages=[],
    include_package_data=True,
    package_data={"": ["zavod/data/*", "zavod/py.typed"]},
    zip_safe=False,
    install_requires=[
        "followthemoney >= 3.2.0, < 4.0.0",
        "nomenklatura >= 3.2.0, < 4.0.0",
        "addressformatting >= 1.3.0, < 2.0.0",
        "datapatch >= 0.2.1",
        "click >= 8.0.0, < 8.2.0",
        "requests",
        "structlog",
        "lxml",
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
            "types-setuptools",
            "types-requests",
        ]
    },
)

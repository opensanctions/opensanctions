from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="opensanctions",
    version="3.2.0",
    url="https://opensanctions.org",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="OpenSanctions",
    author_email="info@opensanctions.org",
    packages=find_packages(exclude=["ez_setup", "zavod", "examples", "test"]),
    namespace_packages=[],
    package_data={
        "opensanctions": [
            "*.yml",
            "*.yaml",
            "*.ini",
            "*.mako",
            "*.ijson",
        ]
    },
    zip_safe=False,
    install_requires=[
        "zavod >= 0.8.0",
    ],
    extras_require={
        "dev": ["bump2version", "wheel>=0.29.0", "twine"],
    },
    entry_points={
        "console_scripts": [
            "opensanctions = opensanctions.cli:cli",
            "osanc = opensanctions.cli:cli",
        ],
    },
)

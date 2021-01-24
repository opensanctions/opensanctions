from setuptools import setup, find_packages


setup(
    name="opensanctions",
    version="2.00",
    author="Organized Crime and Corruption Reporting Project",
    author_email="data@occrp.org",
    url="https://github.com/alephdata/opensanctions",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples", "test"]),
    namespace_packages=[],
    package_data={"opensanctions": ["config/*.yml"]},
    zip_safe=False,
    install_requires=[
        "followthemoney >= 1.21.2",
        "followthemoney-store[postgresql] >= 2.1.6",
        "memorious >= 1.8.3",
        "alephclient >= 2.1.3",
        "xlrd",
    ],
    entry_points={
        "console_scripts": [
            "opensanctions = opensanctions.cli:cli",
            "osanc = opensanctions.cli:cli",
        ],
    },
)

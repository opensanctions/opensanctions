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
        "memorious >= 1.2.3",
        "alephclient >= 1.2.1",
        "xlrd",
    ],
    entry_points={"memorious.plugins": ["opensanctions = opensanctions:init"],},
)

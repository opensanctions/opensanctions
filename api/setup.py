from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="osapi",
    version="3.2.0",
    url="https://opensanctions.org/docs/api/",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="OpenSanctions",
    author_email="info@opensanctions.org",
    packages=find_packages(exclude=["ez_setup", "examples", "test"]),
    namespace_packages=[],
    zip_safe=False,
    install_requires=[
        "opensanctions",
        "elasticsearch[async]",
        "fastapi",
        "uvicorn[standard]",
        "python-multipart",
        "email-validator",
    ],
)

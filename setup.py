from setuptools import setup, find_packages

setup(
    name='pepparser',
    version='0.0.1',
    description="",
    long_description="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    keywords='pep poi persons popolo database',
    author='Friedrich Lindenberg',
    author_email='friedrich@pudo.org',
    url='http://pudo.org',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=[],
    include_package_data=True,
    zip_safe=False,
    test_suite='nose.collector',
    install_requires=[
        "unicodecsv",
        "click",
        "requests",
        "lxml",
        "csvkit",
        "six"
    ],
    entry_points={
        'console_scripts': [
            'pep = pepparser.cli:cli'
        ]
    },
    tests_require=[]
)

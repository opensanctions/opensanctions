from setuptools import setup, find_packages


setup(
    name='opensanctions',
    version='1.99',
    author='Organized Crime and Corruption Reporting Project',
    author_email='data@occrp.org',
    url='https://github.com/alephdata/opensanctions',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'test']),
    namespace_packages=[],
    package_data={
        'opensanctions': ['config/*.yml']
    },
    zip_safe=False,
    install_requires=[
        'followthemoney >= 1.9.2',
        'balkhash[sql] >= 0.3.0',
        'memorious >= 0.8',
        'countrynames',
        'xlrd',
    ],
    entry_points={
        'memorious.plugins': [
            'opensanctions = opensanctions:init'
        ],
        'console_scripts': [
            'osanc-dump = opensanctions:cli',
        ]
    }
)

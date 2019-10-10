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
        'followthemoney >= 1.21.2',
        'balkhash[sql] >= 1.0.2',
        'memorious >= 1.2.3',
        'alephclient >= 1.2.1',
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

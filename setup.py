from setuptools import setup, find_packages


setup(
    name='opensanctions',
    version='0.1',
    author='Journalism Development Network',
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
        'memorious >= 0.4',
        'unicodecsv',
        'xlrd',
        'attrs',
        'pandas',
    ],
    entry_points={
        'memorious.plugins': [
            'opensanctions = opensanctions:init'
        ]
    }
)

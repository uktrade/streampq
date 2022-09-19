import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='streampq',
    version='0.0.48',
    author='Department for International Trade',
    author_email='sre@digital.trade.gov.uk',
    description='PostgreSQL adapter to stream results of multi-statement queries without a server-side cursor',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/uktrade/streampq',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ],
    python_requires='>=3.8.0',
    py_modules=[
        'streampq',
    ],
)

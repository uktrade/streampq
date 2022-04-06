import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='streampq',
    version='0.0.0',
    author='Department for International Trade',
    author_email='sre@digital.trade.gov.uk',
    description='Stream results of multi statement PostgreSQL queries from Python',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/uktrade/streampq',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ],
    python_requires='>=3.6.0',
    py_modules=[
        'streampq',
    ],
)

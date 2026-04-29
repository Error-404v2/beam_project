import setuptools

setuptools.setup(
    name='beam-profiling-pipeline',
    version='0.1',
    install_requires=[
        'apache-beam[gcp]==2.72.0',
    ],
    packages=setuptools.find_packages(),
    py_modules=['main', 'pipeline', 'config', 'utils', 'schemas', 'transforms'],
)

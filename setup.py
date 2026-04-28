import setuptools

setuptools.setup(
    name='beam-profiling-pipeline',
    version='0.1',
    install_requires=[
        'apache-beam[gcp]==2.72.0',
        'pandas==3.0.2',
        'matplotlib==3.10.9',
        'seaborn==0.13.2',
        'networkx==3.6.1',
    ],
    packages=setuptools.find_packages(),
    py_modules=['main', 'pipeline', 'config', 'utils', 'schemas', 'transforms'],
)

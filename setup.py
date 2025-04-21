from setuptools import setup, find_packages

setup(
    name='ifood case',
    version='0.1.0',
    description='projeto ifood teste',
    author='Yuan',
    url='https://github.com/zng489/ifood_case',
    packages=find_packages(),
    install_requires=[
        'apache-airflow==2.10.5',
        'boto3==1.37.36',
        'botocore==1.37.36',
        'duckdb==1.2.2',
        'Flask_AppBuilder==4.5.2',
        'moto==4.2.12',
        'pyspark==3.5.1',  # Compatível com Python 3.12
        'python-dotenv==1.1.0',
        'requests==2.32.3',
        'selenium==4.31.0',
    ],
    python_requires='>=3.12',
)

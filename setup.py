from setuptools import setup, find_packages

setup(
    name="opensearch-ppl-loadtest",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "opensearch-py>=2.0.0"
    ],
    python_requires=">=3.7",
    author="LoadTest Framework",
    description="OpenSearch PPL Load Testing Framework"
)
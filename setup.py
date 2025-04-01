"""
Script d'installation pour le package sqlite3-to-oracle.
"""

from setuptools import setup, find_packages
import os

# Lire le contenu du README
here = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = "Outil de conversion de bases de données SQLite vers Oracle"

# Version du package
version = '1.0.0'

setup(
    name="sqlite3-to-oracle",
    version=version,
    description="Outil de conversion de bases de données SQLite vers Oracle",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="MATILDA Team",
    author_email="info@matilda.com",
    url="https://github.com/matilda-team/sqlite3-to-oracle",
    packages=find_packages(),
    install_requires=[
        "oracledb>=1.0.0",
        "python-dotenv>=0.15.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.10.0",
            "black>=20.8b1",
            "isort>=5.0.0",
            "mypy>=0.800",
        ],
        "ui": [
            "rich>=10.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "sqlite3-to-oracle=sqlite3_to_oracle.cli:main",
            "reload-missing-tables=reload_missing_tables:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: SQL",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Utilities",
    ],
    python_requires=">=3.7",
    keywords="sqlite, oracle, database, conversion, migration",
)

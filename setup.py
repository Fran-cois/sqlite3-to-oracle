from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Lire les dépendances depuis requirements.txt
with open("requirements.txt", "r", encoding="utf-8") as req_file:
    install_requires = [line.strip() for line in req_file if line.strip()]

setup(
    name="sqlite3-to-oracle",
    version="0.1.0",
    author="MATILDA",
    author_email="votre@email.com",
    description="Convertisseur de base de données SQLite vers Oracle",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/votre-repo/sqlite3-to-oracle",
    packages=find_packages(include=['sqlite3_to_oracle', 'sqlite3_to_oracle.*']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=install_requires,
    entry_points={
        "console_scripts": [
            "sqlite3-to-oracle=sqlite3_to_oracle.cli:main",
        ],
    },
)

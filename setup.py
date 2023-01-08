from setuptools import setup, find_packages

setup(
    name="files_kraken",
    version="1.0",
    description="Files monitoring tool",
    long_description="A package that allows to monitor file system changes and store some info in DBbased on these changes.",
    author="Mikhail Ushakov",
    author_email="etozhe.musha@gmail.com",
    url="https://github.com/MrDunn0/files-kraken",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "better-abc==0.0.3",
        "pyfakefs==5.0.0",
        "pytest==7.2.0",
        "tinydb==4.7.0",
        "tinydb-serialization==2.1.0"
    ]
)

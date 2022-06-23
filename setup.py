import os

from setuptools import find_packages, setup

ROOT_PATH = os.path.abspath(os.path.dirname(__file__))
VERSION = "0.1.0"
long_description = """
An implementation of `Raft` consensus algorithm written in Python 3.
"""


def get_requirements(env: str = None):
    requirements = "requirements"
    if env is not None:
        requirements = f"{requirements}-{env}"
    with open(f"{requirements}.txt".format(env)) as fp:
        return [x.strip() for x in fp.read().split("\n") if not x.startswith("#")]


install_requires = get_requirements()
dev_requires = get_requirements("dev")


setup(
    name="raft",
    version=VERSION,
    long_description=long_description,
    author="rapsealk",
    package_dir={"": "raft"},
    packages=find_packages("raft"),
    python_requires=">=3.10",
    install_requires=install_requires,
    extras_require={"dev": dev_requires},
    zip_safe=False,
    include_package_data=True
)

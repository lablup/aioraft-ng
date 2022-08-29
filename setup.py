import os

from setuptools import setup

ROOT_PATH = os.path.abspath(os.path.dirname(__file__))
VERSION = "0.1.0"
description = (
    "An implementation of `Raft` consensus algorithm written in asyncio-based Python 3."
)


def get_requirements(env: str = None):
    requirements = "requirements"
    if env is not None:
        requirements = f"{requirements}-{env}"
    with open(f"{requirements}.txt".format(env)) as fp:
        return [x.strip() for x in fp.read().split("\n") if not x.startswith("#")]


install_requires = get_requirements()
dev_requires = get_requirements("dev")


setup(
    name="aioraft",
    version=VERSION,
    description=description,
    author="Lablup Inc.",
    maintainer="rapsealk",
    maintainer_email="jskang@lablup.com",
    url="https://github.com/lablup/aioraft",
    license="Apache-2.0",
    package_dir={"aioraft": "aioraft"},
    python_requires=">=3.10",
    install_requires=install_requires,
    extras_require={"dev": dev_requires},
    zip_safe=False,
    include_package_data=True,
)

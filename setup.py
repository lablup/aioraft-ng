import os
from pathlib import Path

from setuptools import setup

ROOT_PATH = os.path.abspath(os.path.dirname(__file__))
VERSION = (Path(__file__).parent / "aioraft" / "VERSION").read_text().strip()
description = (
    "An implementation of `Raft` consensus algorithm written in asyncio-based Python 3."
)


def get_requirements(env: str = "base"):
    requirements = os.path.join("requirements", env)
    with open(f"{requirements}.txt".format(env)) as fp:
        return [x.strip() for x in fp.read().split("\n") if not x.startswith("#")]


install_requires = get_requirements()


setup(
    name="aioraft-ng",
    version=VERSION,
    description=description,
    author="Lablup Inc.",
    maintainer="rapsealk",
    maintainer_email="jskang@lablup.com",
    url="https://github.com/lablup/aioraft-ng",
    license="Apache License 2.0",
    package_dir={"aioraft": "aioraft"},
    package_data={"": ["VERSION"]},
    python_requires=">=3.10",
    install_requires=install_requires,
    extras_require={},
    zip_safe=False,
    include_package_data=True,
)

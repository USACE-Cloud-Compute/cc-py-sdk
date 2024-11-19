# !/usr/bin/env python

from setuptools import setup

setup(
    package_dir={"": "src"},
    name="cc",
    packages=["cc"],
    version="0.1.0",
    description="Cloud Compute Python SDK",
    author="USACE",
    license="MIT",
    author_email="randal.s.goss@usace.army.mil",
    url="https://github.com/usace/cc-py-sdk",
    keywords=[
        "template",
        "package",
    ],
    python_requires=">=3.10",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development",
    ],
)

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="greeclimate-davo22",
    python_requires=">=3.8",
    install_requires=requirements,
    author="Clifford Roche",
    author_email="",
    description="Fork of greeclimate adding Gree Cloud (MQTT) support, for cloud-only devices",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/davo22/greeclimate",
    packages=setuptools.find_packages(exclude=["tests"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
)

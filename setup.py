from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="oep-client",
    version="0.17.1",
    description="client side tool for openenergy platform",
    long_description_content_type="text/markdown",
    long_description=long_description,
    packages=["oep_client"],
    author="Christian Winger",
    author_email="c.winger@oeko.de",
    url="https://github.com/wingechr/oep-client",
    install_requires=[
        "requests",
        "pandas",
        "click",
        # "sqlalchemy",
        # "oedialect",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    entry_points={"console_scripts": ["oep-client = oep_client.__main__:main"]},
    package_data={},
)

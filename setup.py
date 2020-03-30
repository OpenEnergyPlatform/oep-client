from setuptools import setup

setup(
    name="oep-client",
    version="0.0.0",
    description="client side tool for openenergy platform",
    packages=["oep_client"],
    author="Christian Winger",
    author_email="c.winger@oeko.de",
    url="http://toolsfrek1.oeko.local/gitlab/c.winger/oekolib",
    install_requires=["omi"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            # 'CMD = package.module:function'
        ]
    },
    package_data={
        # 'package.module: [file_patterns]'
    },
)

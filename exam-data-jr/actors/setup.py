import setuptools

setuptools.setup(
    name="actors",
    packages=setuptools.find_packages(exclude=["actors_tests"]),
    install_requires=[
        "dagster==0.14.3",
        "dagit==0.14.3",
        "pytest",
    ],
)

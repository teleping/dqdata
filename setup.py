import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dqdata",
    version="0.2.01",
    author="teleping",
    author_email="teleping@163.com",
    description="Data sdk for DuCheng quant database.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=" https://pypi.org/project/dqdata/",
    project_urls={
        "Bug Tracker": "https://pypi.org/project/dqdata/",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=["numpy", "pandas", "urllib3", "sqlalchemy"],
)
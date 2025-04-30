import setuptools
import os.path

base_dir = os.path.dirname(__file__)

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

about = {}
with open(os.path.join(base_dir, "bdqc_taxa", "src", "__about__.py")) as f:
    exec(f.read(), about)


setuptools.setup(
    name=about["__title__"],
    version=about["__version__"],
    description=about["__summary__"],
    url=about["__uri__"],
    author=about["__author__"],
    author_email=about["__email__"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"bdqc_taxa": "bdqc_taxa/src"},
    packages=["bdqc_taxa"],
    include_package_data=True,
    package_data={
        'bdqc_taxa': ['../custom_sources.sqlite', 'custom_sources.sqlite']
    },
    python_requires=">=3.6",
    extras_require={
        'dev': [
            'pandas',
            'numpy'
        ],
    },
)
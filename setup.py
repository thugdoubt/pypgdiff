from setuptools import setup
import codecs

with codecs.open("README.rst", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name = "pyPGdiff",
    version = "0.1.3",
    description = "Generate on-the-fly migrations between PostgreSQL schemas",
    long_description = long_description,

    url = "https://github.com/thugdoubt/pypgdiff",

    author = "Nick Purvis",
    author_email = "nep@noisetu.be",

    license = "GPLv2",

    classifiers = [
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: SQL",
    ],

    keywords = "sql postgresql migration",

    packages = [
        "pypgdiff",
    ],

    install_requires = [
        "psycopg2",
    ],

    entry_points = {
        "console_scripts": [
            "pypgdiff=pypgdiff:main",
        ],
    },
)

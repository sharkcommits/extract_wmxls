from setuptools import find_packages, setup

setup(
    name = 'extractipedia',
    version = '0.0.2',
    description = 'A simple tool that extracts plain text Wikipedia Pages to SQLite database.',
    packages = find_packages(),
    long_description = open('README.md').read(),
    long_description_content_type = 'text/markdown',
    url = 'https://github.com/sharkcommits/extractipedia',
    author = 'sharkcommits',
    author_email = 'sharkcommits@protonmail.com',
    license = 'GPLv3',
    classifiers = [
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent"
    ],
    extras_require = {
        'dev': ['pytest>=7.0', 'twine>=4.0.2']    
    },
    python_requires = '>=3.10'
) 
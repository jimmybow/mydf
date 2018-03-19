from setuptools import setup, find_packages

setup(
    name = 'mydf',
    version = '0.0.11',
    author = 'Jimmybow',
    author_email = 'jimmybow@hotmail.com.tw',
    keywords = 'pandas dplyr dfply',
    packages = find_packages(),
    package_dir={'mydf':'mydf'},
    install_requires=['numpy', 'pandas'],
    description = 'An improved version for dfply.',
    license = 'GNU General Public License v3.0',
    url = 'https://github.com/jimmybow/mydf'
)

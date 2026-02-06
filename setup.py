from setuptools import setup, find_packages
from typing import List

HYPHEN = '-e .'
def get_requirements(file_path:str)->List[str]:
    requirements = []
    with open(file_path) as f:
        requirements = f.readlines()
        requirements = [req.replace('\n', '') for req in requirements]
    
        if HYPHEN in requirements:
            requirements.remove(HYPHEN)

    return requirements


setup(
    name='lagos-rent-predictor',
    version='0.1.0',
    author='Husain Ridwan',
    author_email='h.ridwan707@gmail.com',
    packages=find_packages(),
    install_requires=get_requirements('requirements.txt')
)
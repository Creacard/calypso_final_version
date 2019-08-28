from setuptools import setup, find_packages

with open("requierements.txt") as f:
    requierements = f.read()

setup(
  name='Creacard_Utils',
  version = '0.0.1',
  requierements= requierements,
  packages=find_packages(),
  url='',
  license='',
  author='Justin Valet',
  description='This packages wraps several tools',
  install_requires=[
    "pandas"
  ]
)

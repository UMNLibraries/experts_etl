from setuptools import setup, find_packages

setup(
  name='experts_etl',
  version='0.0.0',
  description='Moves data from UMN to Pure (Experts@Minnesota), and vice versa.',
  url='https://github.com/UMNLibraries/experts_etl',
  author='David Naughton',
  author_email='nihiliad@gmail.com',
  packages=find_packages(exclude=['tests','docs'])
)

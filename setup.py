# -*- coding: utf-8 -*-
from distutils.core import setup

packages = \
['experts_etl',
 'experts_etl.edw_to_pure',
 'experts_etl.extractor_loaders',
 'experts_etl.oit_to_edw',
 'experts_etl.transformer_loaders']

package_data = \
{'': ['*'], 'experts_etl': ['templates/*']}

install_requires = \
['dotenv_switch',
 'experts_dw',
 'jinja2>=2.10.1',
 'pandas',
 'pureapi',
 'python-daemon',
 'python-json-logger',
 'schedule']

setup_kwargs = {
    'name': 'experts-etl',
    'version': '5.6.1',
    'description': 'Moves data from UMN to Pure (Experts@Minnesota), and vice versa.',
    'long_description': '# Experts@Minnesota ETL\n\nMoves data from UMN to Pure (Experts@Minnesota), and vice versa.\n\n## Usage\n\nPlease see `tests/test_*.py` for now. We freely admit this section of the documentation requires\nmuch improvement.\n\n## Pure API Versions\n\nSuccessfully tested against Pure API versions 5.10.x - 5.12.x.\n\n## Requirements and Recommendations\n\n### Python Versions\n\nexperts_etl requires Python >= 3.\n\n### Environment Variables\n\nTo connect to the Pure server, including when running `tests/test_client.py`, the\n`PURE_API_URL` and `PURE_API_KEY` environment variables must be set. To connect to\nthe Experts@Minnesota Data Warehouse and the UMN OIT Data Warehouse, the environment\nvariables `EXPERTS_DB_USER`, `EXPERTS_DB_PASS`, and `EXPERTS_DB_SERVICE_NAME`\nmust be set.\n\nOne option is to set these environment variables in a `.env` file. See `env.dist` for an example.\n\n`EXPERTS_DB_SERVICE_NAME` must identify an Oracle tnsnames connection string.\n\n```\n# Example env\nEXPERTS_DB_SERVICE_NAME=hoteltst.oit\n\n# $ORACLE_HOME/network/admin/tnsnames.ora\nhoteltst.oit =\n  (DESCRIPTION =\n    (ADDRESS = (PROTOCOL = TCP)(Host = oracle-instance-domain.umn.edu)(Port = 1521))\n    (CONNECT_DATA =\n      (SERVICE_NAME = hoteltst.oit)\n    )\n  )\n```\n\n### pyenv, venv, and poetry \n\nTo install and manage Python versions we use [pyenv](https://github.com/pyenv/pyenv), and to manage\ndependencies we use [poetry](https://poetry.eustace.io/). While alternative tools will work, we document\nhere only the tools we use. We will document the use of other tools if demand arises.\n\nOne way to set up all these tools to work together, for a new project, is to follow the workflow below.\nNote that we prefer to put virtual environments inside the project directory. Note also that we use the\nbuilt-in `venv` module to create virtual environments, and we name their directories `.venv`, because\nthat is what `poetry` does and expects.\n\n* Install pyenv.\n* `pyenv install $python_version`\n* `mkdir $project_dir; cd $project_dir`\n* Create a `.python-version` file, containing `$python_version`. \n* `pip install poetry`\n* `poetry config settings.virtualenvs.in-project true`\n* `python -m venv ./.venv/`\n* `source ./.venv/bin/activate`\n \nNow running commands like `poetry install` or `poetry update` should install packages into the virtual\nenvironment in `./.venv`. Don\'t forget to `deactivate` the virtual environment when finished using it.\nIf the project virtual environment is not activated, `poetry run` and `poetry shell` will activate it.\nWhen using `poetry shell`, exit the shell to deactivate the virtual environment. \n\n## Installing\n\nAdd to `pyproject.toml`:\n\n```\nexperts_etl = {git = "git://github.com/UMNLibraries/experts_etl.git"}\n```\n\nTo specify a version, include the `tag` parameter:\n\n```\nexperts_etl = {git = "git://github.com/UMNLibraries/experts_etl.git", tag = "1.0.0"}\n```\n\nTo install, run `poetry install`.\n\n## Testing\n\nRun the following, either as arguments\nto `poetry run`, or after running `poetry shell`:\n\n```\npytest tests/test_affiliate_job.py\npytest tests/test_employee_job.py\n```\n\nOr to run all tests: `pytest`\n\nNote that many of these tests are integration tests that execute queries against UMN databases,\nso the environment variables described in\n[Requirements and Recommendations](#requirements-and-recommendations)\nmust be set in order to run those tests.\n\n## Contributing\n\n### Include an updated `setup.py`.\n\nPython package managers, including poetry, will be unable to install a VCS-based package without a \n`setup.py` file in the project root. To generate `setup.py`:\n\n```\npoetry build\ntar -zxf dist/experts_etl-0.0.0.tar.gz experts_etl-0.0.0/setup.py --strip-components 1\n```\n\n### Please commit `pyproject.lock`.\n\nBecause experts_etl is an application, please commit `pyproject.lock` so that we can reproduce builds\nwith exactly the same set of packages.\n',
    'author': 'David Naughton',
    'author_email': 'naughton@umn.edu',
    'url': None,
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.6.3,<4.0.0',
}


setup(**setup_kwargs)

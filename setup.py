# -*- coding: utf-8 -*-
from distutils.core import setup

packages = \
['experts_etl',
 'experts_etl.edw_to_pure',
 'experts_etl.extractor_loaders',
 'experts_etl.oit_to_edw',
 'experts_etl.transformer_loaders']

package_data = \
{'': ['*'], 'experts_etl': ['pure_to_edw/*', 'templates/*']}

install_requires = \
['dotenv_switch',
 'experts_dw',
 'jinja2>=2.10.1',
 'pandas',
 'pureapi',
 'python-daemon',
 'python-json-logger',
 'python-ldap>=3.3,<4.0',
 'schedule']

setup_kwargs = {
    'name': 'experts-etl',
    'version': '5.9.2',
    'description': 'Moves data from UMN to Pure (Experts@Minnesota), and vice versa.',
    'long_description': "# Experts@Minnesota ETL\n\nMoves data from UMN to Pure (Experts@Minnesota), and vice versa.\n\n## Overview\n\nThis is an Extract-Transform-Load (ETL) system that integrates other major\nsystems both inside and outside UMN. The most important of these are the\nOIT Legacy Data Warehouse and Pure systems hosted by Elsevier.\n\n### Installing and Running Remotely\n\nTo provision remote environments, and to install and deploy Experts ETL for\nrunning in those environemtns, see\n[experts-ansible on UMN GitHub](https://github.umn.edu/Libraries/experts-ansible).\n\n### Installing and Running Locally\n\nWhile it should be possible to run the Experts ETL system on a local\ndevelopment machine, we advise against it, due to its reliance on\nintegration with external systems, whose data it writes as well as reads.\nInstead most of this document is about running unit and integration tests\nin a local development environment.\n\n## Installing\n\n### Prerequisites\n\n#### Python\n\nExperts ETL requires a relatively recent version of Python 3. See the\n[pyproject.toml](pyproject.toml) project config file for supported versions.\n\n#### Oracle\n\nBoth the OIT Legacy Data Warehouse and the Experts Data Warehouse are Oracle\ndatabases. See [experts-dw on GitHub](https://github.com/UMNLibraries/experts-dw)\nfor supported versions of the required Oracle InstanctClient library.\n\n#### LDAP\n\nExperts ETL uses LDAP to search for some student researcher information. See the\n[python-ldap build prerequisites](https://www.python-ldap.org/en/python-ldap-3.3.0/installing.html#build-prerequisites)\nfor the required system libraries to install in your local environment.\n\n### pyenv, venv, and poetry\n\nTo install and manage Python versions we use\n[pyenv](https://github.com/pyenv/pyenv), and to manage dependencies we use\n[poetry](https://poetry.eustace.io/).\n\nOne way to set up all these tools to work together, for a new project, is to\nfollow the workflow below. Note that we prefer to put virtual environments\ninside the project directory. Note also that we use the built-in `venv` module\nto create virtual environments, and we name their directories `.venv`, because\nthat is what `poetry` does and expects.\n\n* Install pyenv.\n* `pyenv install $python_version`\n* `mkdir $project_dir; cd $project_dir`\n* Create a `.python-version` file, containing `$python_version`.\n* `pip install poetry`\n* `poetry config settings.virtualenvs.in-project true`\n* `python -m venv ./.venv/`\n* `source ./.venv/bin/activate`\n\nNow running commands like `poetry install` or `poetry update` should install\npackages into the virtual environment in `./.venv`.\n\nSo to install the python package dependencies for Experts ETL, run `poetry install`.\n\nDon't forget to `deactivate` the virtual environment when finished using it. If\nthe project virtual environment is not activated, `poetry run` and `poetry\nshell` will activate it.  When using `poetry shell`, exit the shell to\ndeactivate the virtual environment.\n\n## Testing\n\n### Environment Variables\n\nExperts ETL connects to several external services, for which it requires configuration\nvia environment variables:\n\n* Pure web services API\n  * `PURE_API_DOMAIN`\n  * `PURE_API_VERSION`\n  * `PURE_API_KEY`\n* Experts@Minnesota Data Warehouse and the UMN OIT Data Warehouse\n  * `EXPERTS_DB_USER`\n  * `EXPERTS_DB_PASS`\n  * `EXPERTS_DB_HOSTNAME`\n  * `EXPERTS_DB_SERVICE_NAME`\n* UMN LDAP\n  * `UMN_LDAP_DOMAIN`\n  * `UMN_LDAP_PORT`\n\nSome tests are integration tests that connect to these external services, so\nthese variables must be set for testing. One option is to set these\nenvironment variables in a `.env` file. See `env.dist` for an example.\n\n### Running the Tests\n\nRun the following, either as arguments\nto `poetry run`, or after running `poetry shell`:\n\n```\npytest tests/test_affiliate_job.py\npytest tests/test_employee_job.py\n...\n```\n\nOr to run all tests: `pytest`\n\n## Contributing\n\n### Include an updated `setup.py`.\n\nPython package managers, including poetry, will be unable to install a VCS-based\npackage without a `setup.py` file in the project root. To generate `setup.py`:\n\n```\npoetry build\ntar -zxf dist/experts_etl-0.0.0.tar.gz experts_etl-0.0.0/setup.py --strip-components 1\n```\n\n### Please commit `pyproject.lock`.\n\nBecause Experts ETL is an application, please commit `pyproject.lock` so that we\ncan reproduce builds with exactly the same set of packages.\n",
    'author': 'David Naughton',
    'author_email': 'naughton@umn.edu',
    'url': None,
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.6.3,<4.0.0',
}


setup(**setup_kwargs)

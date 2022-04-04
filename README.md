# Experts@Minnesota ETL

Moves data from UMN to Pure (Experts@Minnesota), and vice versa.

## Overview

This is an Extract-Transform-Load (ETL) system that integrates other major
systems both inside and outside UMN. The most important of these are the
OIT Legacy Data Warehouse and Pure systems hosted by Elsevier.

### Installing and Running Remotely

To provision remote environments, to install and deploy Experts ETL for
running in those environemtns, and for general documentation about our
deployment of Experts ETL in those remote environments, see 
[experts-ansible on UMN GitHub](https://github.umn.edu/Libraries/experts-ansible).

### Installing and Running Locally

While it should be possible to run the Experts ETL system on a local
development machine, we advise against it, due to its reliance on
integration with external systems, whose data it writes as well as reads.
Instead most of this document is about running unit and integration tests
in a local development environment.

## Installing

### Prerequisites

#### Python

Experts ETL requires a relatively recent version of Python 3. See the
[pyproject.toml](pyproject.toml) project config file for supported versions.

#### Oracle

Both the OIT Legacy Data Warehouse and the Experts Data Warehouse are Oracle
databases. See [experts\_dw on GitHub](https://github.com/UMNLibraries/experts_dw)
for supported versions of the required Oracle InstanctClient library.

#### LDAP

Experts ETL uses LDAP to search for some student researcher information. See the
[python-ldap build prerequisites](https://www.python-ldap.org/en/python-ldap-3.3.0/installing.html#build-prerequisites)
for the required system libraries to install in your local environment.

### pyenv, venv, and poetry

To install and manage Python versions we use
[pyenv](https://github.com/pyenv/pyenv), and to manage dependencies we use
[poetry](https://poetry.eustace.io/).

One way to set up all these tools to work together, for a new project, is to
follow the workflow below. Note that we prefer to put virtual environments
inside the project directory. Note also that we use the built-in `venv` module
to create virtual environments, and we name their directories `.venv`, because
that is what `poetry` does and expects.

* Install pyenv.
* `pyenv install $python_version`
* `mkdir $project_dir; cd $project_dir`
* Create a `.python-version` file, containing `$python_version`.
* `pip install poetry`
* `poetry config virtualenvs.in-project true`
* `python -m venv ./.venv/`
* `source ./.venv/bin/activate`

Now running commands like `poetry install` or `poetry update` should install
packages into the virtual environment in `./.venv`.

So to install the python package dependencies for Experts ETL, run `poetry install`.

Don't forget to `deactivate` the virtual environment when finished using it. If
the project virtual environment is not activated, `poetry run` and `poetry
shell` will activate it.  When using `poetry shell`, exit the shell to
deactivate the virtual environment.

## Testing

### Environment Variables

Experts ETL connects to several external services, for which it requires configuration
via environment variables:

* Pure web services API
  * `PURE_API_DOMAIN`
  * `PURE_API_VERSION`
  * `PURE_API_KEY`
* Experts@Minnesota Data Warehouse and the UMN OIT Data Warehouse
  * `EXPERTS_DB_USER`
  * `EXPERTS_DB_PASS`
  * `EXPERTS_DB_HOSTNAME`
  * `EXPERTS_DB_PORT`
  * `EXPERTS_DB_SERVICE_NAME`

Some tests are integration tests that connect to these external services, so
these variables must be set for testing. One option is to set these
environment variables in a `.env` file. See `env.dist` for an example.

### Running the Tests

Run the following, either as arguments
to `poetry run`, or after running `poetry shell`:

```
pytest tests/test_affiliate_job.py
pytest tests/test_employee_job.py
...
```

Or to run all tests: `pytest`

## Contributing

### Entry point: `runner.py`

Automated, scheduled ETL processes start by executing [runner.py](runner.py).
To follow the flow of execution from the beginning, start with that file.

### Separate project: `pureapi`

Experts ETL gets data from Pure via its web services API, for which UMN
Libraries has a separate project,
[pureapi](https://github.com/UMNLibraries/pureapi),
which is a dependency for this project, Experts ETL. Any contributions which
use the Pure API should be made there.

### Include an updated `setup.py`.

Python package managers, including poetry, will be unable to install a VCS-based
package without a `setup.py` file in the project root. To generate `setup.py`:

```
poetry build
tar -zxf dist/experts_etl-0.0.0.tar.gz experts_etl-0.0.0/setup.py --strip-components 1
```

### Please commit `pyproject.lock`.

Because Experts ETL is an application, please commit `pyproject.lock` so that we
can reproduce builds with exactly the same set of packages.

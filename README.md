# Sage: a SPARQL query engine for public Linked Data providers
[![Build Status](https://travis-ci.com/sage-org/sage-engine.svg?branch=master)](https://travis-ci.com/sage-org/sage-engine) [![PyPI version](https://badge.fury.io/py/sage-engine.svg)](https://badge.fury.io/py/sage-engine) [![Docs](https://img.shields.io/badge/docs-passing-brightgreen)](https://sage-org.github.io/sage-engine/)

[Read the online documentation](https://sage-org.github.io/sage-engine/)

SaGe is a SPARQL query engine for public Linked Data providers that implements *Web preemption*. The SPARQL engine includes a smart Sage client
and a Sage SPARQL query server hosting RDF datasets (hosted using [HDT](http://www.rdfhdt.org/)).
This repository contains the **Python implementation of the SaGe SPARQL query server**.

SPARQL queries are suspended by the web server after a fixed quantum of time and resumed upon client request. Using Web preemption, Sage ensures stable response times for query execution and completeness of results under high load.

The complete approach and experimental results are available in a Research paper accepted at The Web Conference 2019, [available here](https://hal.archives-ouvertes.fr/hal-02017155/document). *Thomas Minier, Hala Skaf-Molli and Pascal Molli. "SaGe: Web Preemption for Public SPARQL Query services" in Proceedings of the 2019 World Wide Web Conference (WWW'19), San Francisco, USA, May 13-17, 2019*.

We appreciate your feedback/comments/questions to be sent to our [mailing list](mailto:sage@univ-nantes.fr) or [our issue tracker on github](https://github.com/sage-org/sage-engine/issues).

# Table of contents

* [Installation](#installation)
* [Getting started](#getting-started)
  * [Server configuration](#server-configuration)
  * [Starting the server](#starting-the-server)
* [Sage Docker image](#sage-docker-image)
* [Command line utilities](#command-line-utilities)
* [Documentation](#documentation)

# Installation

Requirements:
* Python 3.7 (*or higher*)
* [pip](https://pip.pypa.io/en/stable/)
* [Virtualenv](https://pypi.org/project/virtualenv)
* **gcc/clang** with **c++11 support**
* **Python Development headers**
> You should have the `Python.h` header available on your system.   
> For example, for Python 3.6, install the `python3.6-dev` package on Debian/Ubuntu systems.

## Manual Installation

```bash
# Download the project and move to the extended-property-paths branch
git clone https://github.com/sage-org/sage-engine
cd sage-engine
git checkout extended-property-paths
# Create a virtual environment to isolate SaGe dependencies
virtualenv --python=/usr/bin/python3 ppaths
# Activate the virtual environment
source ppaths/bin/activate
# Install SaGe dependencies
pip install -r requirements.txt
pip install -e .[hdt,postgres]
```
The various SaGe backends are installed as extras dependencies, using the `-e` flag.

To make the installation of SaGe easier, SaGe is installed in a virtual environment.

```bash
# To activate the SaGe (ppaths) environment
source ppaths/bin/activate
# To deactivate the SaGe environment
deactivate
```

# Getting started

## Server configuration

A Sage server is configured using a configuration file in [YAML syntax](http://yaml.org/).
You will find below a minimal working example of such configuration file.
A full example is available [in the `config_examples/` directory](https://github.com/sage-org/sage-engine/blob/master/config_examples/example.yaml)

```yaml
name: SaGe Test server
maintainer: Chuck Norris
quota: 75
max_depth: 5
max_results: 10000
max_control_tuples: 10000
graphs:
-
  name: dbpedia
  uri: http://example.org/dbpedia
  description: DBPedia
  backend: hdt-file
  file: datasets/dbpedia.2016.hdt
```

The `quota` and `max_results` fields are used to set the maximum time quantum and the maximum number of results allowed per request, respectively.

The `max_depth` and `max_control_tuples` fields are used to set the maximum depth and the maximum number of control tuples allowed per request, respectively. These fields are used for property paths queries that contain transitive closure path expressions.

Each entry in the `graphs` field declare a RDF dataset with a name, description, backend and options specific to this backend.
The `hdt-file` backend allow a SaGe server to load RDF datasets from [HDT files](http://www.rdfhdt.org/). Sage uses [pyHDT](https://github.com/Callidon/pyHDT) to load and query HDT files.
The `postgres` backend allow a SaGe server to manager RDF datasets stored into [PostgreSQL](https://www.postgresql.org/). SaGe uses [psycopg2](https://pypi.org/project/psycopg2/) to interact with PostgreSQL.

## Starting the server

The `sage` executable, installed alongside the Sage server, allows to easily start a Sage server from a configuration file using [Gunicorn](http://gunicorn.org/), a Python WSGI HTTP Server.

```bash
# Do not forget to activate the SaGe environment
source ppaths/bin/activate
# Launch the Sage server with 4 workers on port 8000
sage my_config.yaml -w 4 -p 8000
```

The full usage of the `sage` executable is detailed below:
```
Usage: sage [OPTIONS] CONFIG

  Launch the Sage server using the CONFIG configuration file

Options:
  -p, --port INTEGER              The port to bind  [default: 8000]
  -w, --workers INTEGER           The number of server workers  [default: 4]
  --log-level [debug|info|warning|error]
                                  The granularity of log outputs  [default:
                                  info]
  --help                          Show this message and exit.
```

# Documentation

To generate the documentation, navigate in the `docs` directory and generate the documentation

```bash
cd docs/
make html
open build/html/index.html
```

Copyright 2017-2019 - [GDD Team](https://sites.google.com/site/gddlina/), [LS2N](https://www.ls2n.fr/?lang=en), [University of Nantes](http://www.univ-nantes.fr/)
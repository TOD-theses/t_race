# T-Race

[![codecov](https://codecov.io/gh/TOD-theses/t_race/branch/main/graph/badge.svg?token=t_race_token_here)](https://codecov.io/gh/TOD-theses/t_race)
[![CI](https://github.com/TOD-theses/t_race/actions/workflows/main.yml/badge.svg)](https://github.com/TOD-theses/t_race/actions/workflows/main.yml)

See https://tod-theses.github.io/t_race/.

## Install

In a virtual environment, run:

```
pip install git+https://github.com/TOD-theses/t_race
```

For a docker version, see below. To install from source code with development requirements, use `make install` in a virtual environment.

## Requirements

The TOD candidate mining requires access to a postgres database. We use the postgres docker image for this:
- `docker run --rm --name miner_postgres -p 5432:5432 -e POSTGRES_PASSWORD=password postgres`

All steps require an archive node that supports the debug namespace (e.g. `debug_traceCall`). The attack analysis also requires custom JS tracing with this namespace (which apparently not all provider support out of the box). There is an incompatibility between Erigon and Reth. It is configured per default for Erigon, but can be changed to Reth in the source code by replacing "old Erigon" with "reth".

We suggest using a local RPC cache (e.g. https://github.com/fuzzland/cached-eth-rpc/), in particular for block tracing results.

We developed the tool with Python 3.10.12.

## Usage

After installing it, you can run it as following:

```bash
$ t_race --help
$ t_race run --help
$ t_race mine --help
$ t_race check --help
$ t_race stats --help
```

Refer to [t_race_results](https://github.com/TOD-theses/t_race_results) for the commands used in the thesis.

### Configuration

The tool includes global options, such as the `--provider`, and command specific options, such as `--window-size` for the `t_race run` command.

For global options, place them directly after `t_race`, e.g. `t_race --provider http://example.org run`. For command specific options, place them at the end of the command.

## Docker

First create the image with `make docker`.

Then you can run it via `docker run t_race:latest --help`.

For instance:

```bash
docker run -v "$PWD/out":/app/out --network="host" t_race mine --blocks 19895500-19895510 --window-size 25 --duplicates-limit 10
```

This will share the local `out` directory with the docker container and allow docker to access the localhost (for the archive node provider and postgresql server).
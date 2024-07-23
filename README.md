# T-Race

[![codecov](https://codecov.io/gh/TOD-theses/t_race/branch/main/graph/badge.svg?token=t_race_token_here)](https://codecov.io/gh/TOD-theses/t_race)
[![CI](https://github.com/TOD-theses/t_race/actions/workflows/main.yml/badge.svg)](https://github.com/TOD-theses/t_race/actions/workflows/main.yml)

See https://tod-theses.github.io/t_race/.

## Install

In a virtual environment, run:

```
pip install git+https://github.com/TOD-theses/t_race
```

For a docker version, see below. To install the local version with development requirements, use `make install`.

## Usage

After installing it, you can run it as following:

```bash
$ t_race --help
$ t_race run --help
```

### Configuration

The tool includes global options, such as the `--provider`, and command specific options, such as `--window-size` for the `t_race run` command.

For global options, place them directly after `t_race`, e.g. `t_race --provider http://example.org run`. For command specific options, place them at the end.

### Run command

The `t_race run` command will execute all components of T-RACE.

## Docker

First create the image with `make docker`.

Then you can run it via `docker run t_race:latest --help`.

For instance:

```bash
docker run -v "$PWD/out":/app/out --network="host" t_race mine --blocks 19895500-19895510 --window-size 25 --duplicates-limit 10
```

This will share the local `out` directory with the docker container and allow docker to access the localhost (for the archive node provider and postgresql server).
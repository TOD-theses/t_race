# T-Race

[![codecov](https://codecov.io/gh/TOD-theses/t_race/branch/main/graph/badge.svg?token=t_race_token_here)](https://codecov.io/gh/TOD-theses/t_race)
[![CI](https://github.com/TOD-theses/t_race/actions/workflows/main.yml/badge.svg)](https://github.com/TOD-theses/t_race/actions/workflows/main.yml)

See https://tod-theses.github.io/t_race/.

## Install

See [CONTRIBUTING.md](CONTRIBUTING.md) for installation instructions.

## Usage

After installing it, you can run it as following:

```bash
$ python -m t_race --help
#or
$ t_race --help
```

## Docker

First create the image with `make docker`.

Then you can run it via `docker run t_race:latest --help`.

For instance:

```bash
docker run -v "$PWD/out":/app/out --network="host" t_race mine --blocks 19895500-19895510 --window-size 25 --duplicates-limit 10
```

This will share the local `out` directory with the docker container and allow docker to access the localhost (for the archive node provider and postgresql server).
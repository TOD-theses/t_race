"""CLI interface for t_race project."""

from argparse import ArgumentParser


def main():
    parser = ArgumentParser(description="Find and analyze TOD transactions in Ethereum")

    args = parser.parse_args()
    print(args)

#!/usr/bin/env python

# Third party modules
import click

# First party modules
import data_finder.spider


# @click.group()
@click.version_option(version=data_finder.__version__)
def entry_point():
    """Find data."""
    initial_url = "http://martin-thoma.de/"
    data_finder.spider.main(initial_url)

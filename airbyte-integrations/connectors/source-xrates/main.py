#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_xrates import SourceXrates

if __name__ == "__main__":
    source = SourceXrates()
    launch(source, sys.argv[1:])

# This file is part of ci_middleware.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

import argparse
from abc import ABC, abstractmethod
from collections.abc import Sequence

from ._constants import MISC_INPUT_RUN
from .mock_dataset_maker import MockDatasetMaker
from .repo_data import RepoData


class Tool(ABC):
    def __init__(self, parser: argparse.ArgumentParser):
        parser.set_defaults(subcommand=self)

    @abstractmethod
    def __call__(self, args: argparse.Namespace) -> None:
        raise NotImplementedError()


class MakeBaseRepo(Tool):
    def __init__(self, parser: argparse.ArgumentParser) -> None:
        super().__init__(parser)
        parser.add_argument("root", help="Root path for the base data repository.")
        parser.add_argument(
            "--clobber",
            action="store_true",
            help=(
                "Delete any existing repo and recreate it.  "
                "Default is to add dimension records into an existing repo."
            ),
        )

    def __call__(self, args: argparse.Namespace) -> None:
        RepoData.prep(args.root, clobber=args.clobber)


class PrepForPipeline(Tool):
    def __init__(self, parser: argparse.ArgumentParser):
        super().__init__(parser)
        parser.add_argument("root", help="Root path for the data repository to modify.")
        parser.add_argument(
            "pipeline", help="URI for the original pipeline whose mocked inputs should be added."
        )
        parser.add_argument(
            "--run", default=MISC_INPUT_RUN, help="RUN collection name to write mocked inputs to."
        )

    def __call__(self, args: argparse.Namespace) -> None:
        MockDatasetMaker.prep(args.root, args.pipeline, args.run)


class Display(Tool):
    def __init__(self, parser: argparse.ArgumentParser):
        super().__init__(parser)
        parser.add_argument("root", help="Root path for the data repository to query for spatial data IDs.")
        parser.add_argument(
            "--no-browser",
            action="store_false",
            help="Do not open the plot in a browser window.",
            dest="browser",
        )
        parser.add_argument(
            "--filename",
            help="Name of an HTML file to save the plot to.",
            default=None,
        )

    def __call__(self, args: argparse.Namespace) -> None:
        import bokeh.io

        from lsst.daf.butler import Butler

        from .display import DimensionDisplay

        butler = Butler.from_config(args.root)
        display = DimensionDisplay()
        display.add_repo(butler)
        fig = display.draw()
        if args.filename is not None:
            bokeh.io.output_file(args.filename, title=args.root)
        if args.browser:
            bokeh.io.show(fig)
        else:
            bokeh.io.save(fig)


def main(argv: Sequence[str]) -> None:
    parser = argparse.ArgumentParser(description="Command-line interface for ci_middleware test utilities.")
    subparsers = parser.add_subparsers()
    MakeBaseRepo(
        subparsers.add_parser(
            "make-base-repo", help="Build a base data repository with dimension records and skyMap only."
        )
    )
    PrepForPipeline(
        subparsers.add_parser(
            "prep-for-pipeline",
            help="Add mocked input datasets and formatter configuration to a data repository.",
        )
    )
    Display(
        subparsers.add_parser("display", help="Display of all spatial data ID regions in a data repository.")
    )
    args = parser.parse_args(argv)
    args.subcommand(args)


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])

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

__all__ = (
    "INSTRUMENT",
    "DETECTORS",
    "BANDS",
    "SKYMAP",
    "UNMOCKED_DATASET_TYPES",
    "DEFAULTS_COLLECTION",
    "MISC_INPUT_RUN",
    "PIPELINE_FORMATTERS_CONFIG_DIR",
    "INPUT_FORMATTERS_CONFIG_DIR",
)

INSTRUMENT = "HSC"
DETECTORS = (57, 58, 49, 50, 41, 42)
BANDS = ("r", "i")
SKYMAP = "ci_mw"
UNMOCKED_DATASET_TYPES = ("skyMap",)
DEFAULTS_COLLECTION = "HSC/defaults"
MISC_INPUT_RUN = "HSC/misc"
PIPELINE_FORMATTERS_CONFIG_DIR = "pipeline_formatters"
INPUT_FORMATTERS_CONFIG_DIR = "input_formatters"

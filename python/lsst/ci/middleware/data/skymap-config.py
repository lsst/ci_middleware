# This file is part of ci_middleware
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

# type: ignore

assert config.__class__.__name__ == "DiscreteSkyMapConfig"

config.projection = "TAN"
config.tractOverlap = 0.25 / 60  # Overlap between tracts (degrees)
config.pixelScale = 0.168
config.raList = [149.7, 148.9, 149.7, 148.9]
config.decList = [1.1, 1.1, 1.9, 1.9]
config.radiusList = [0.4, 0.4, 0.4, 0.4]
config.tractBuilder["cells"].cellInnerDimensions = [200, 150]
config.tractBuilder["cells"].numCellsPerPatchInner = 30
config.pixelScale = 0.168
config.tractBuilder = "cells"

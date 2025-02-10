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

__all__ = ("DimensionDisplay",)

# This module imports bokeh (which is considered an optional dependency) at
# module scope. it should only be imported with ImportError guards or at
# function scope.

import dataclasses
from collections import defaultdict
from collections.abc import Iterable
from typing import Any, cast

import bokeh.plotting
import numpy as np

from lsst.afw.geom import makeCdMatrix, makeSkyWcs
from lsst.daf.butler import Butler, DataCoordinate, DimensionRecord, DimensionUniverse, SkyPixDimension
from lsst.geom import Point2D, SpherePoint, degrees
from lsst.sphgeom import Box

BASE_STYLE = {"fill_alpha": 0.5}


STYLE_DEFAULTS: dict[str, dict[str, Any]] = dict(
    htm7={"line_color": "black", "fill_color": "white", "fill_alpha": 0.0, **BASE_STYLE, "level": "underlay"},
    patch={"line_color": "blue", "fill_color": "blue", **BASE_STYLE, "level": "overlay"},
    visit_detector_region={"line_color": "red", "fill_color": "red", **BASE_STYLE, "level": "glyph"},
)


@dataclasses.dataclass
class DataCoordinateRegion:
    """Struct that holds a spatial dimension record and the indices of its
    regions' vertices in the parent array of all vertices.

    This approach of putting all vertices into a single array allows us to
    fully vectorize transforming points from sky coordinates to the plotting
    projection, which is very important when using AST (as our SkyWcs objects
    do under the hood).
    """

    record: DimensionRecord
    vertex_slice: slice


class DimensionDisplay:
    """A Bokeh-based display tool for spatial data IDs.

    Parameters
    ----------
    center : `lsst.geom.SpherePoint`, optional
        Center point for the gnomonic projection.  If not provided, the
        center of the lat/long bounding box of all regions is used.
    **styles : `dict`
        Additional keyword arguments are dictionaries of Bokeh plot styles for
        a particular dimension element.
    """

    def __init__(self, center: SpherePoint | None = None, **styles: dict):
        self._center = center
        self._styles = STYLE_DEFAULTS.copy()
        for k, v in styles.items():
            self._styles.setdefault(k, BASE_STYLE.copy()).update(v)
        self._bbox = Box()
        self._sphere_vertices: list[SpherePoint] = []
        self._regions: defaultdict[str, dict[DataCoordinate, DataCoordinateRegion]] = defaultdict(dict)

    def add_record(self, record: DimensionRecord, update_bbox: bool = True) -> None:
        """Add a single spatial dimension record to the display.

        Parameters
        ----------
        record : `lsst.daf.butler.DimensionRecord`
            Spatial dimension record.  Must have a ``region`` attribute that is
            a `lsst.sphgeom.ConvexPolygon`.  Records with the same dimension
            element and data ID as one that has already been added will be
            ignored.
        update_bbox : `bool`, optional
            If `True` (default) update the internal bounding box used to set
            plot limits and (possibly) the center of the projection to include
            the bounding box of this record's region.

        Notes
        -----
        This method does not actually do any plotting; it just updates internal
        state that determines what will be plotted when `draw` is called.
        """
        if record.dataId in self._regions[record.definition.name]:
            return
        vertices = [SpherePoint(v) for v in record.region.getVertices()]
        vertex_begin = len(self._sphere_vertices)
        self._sphere_vertices.extend(vertices)
        vertex_end = len(self._sphere_vertices)
        if update_bbox:
            self._bbox.expandTo(record.region.getBoundingBox())
        data_id_region = DataCoordinateRegion(record, slice(vertex_begin, vertex_end))
        self._regions[record.definition.name][record.dataId] = data_id_region

    def add_records(
        self,
        records: Iterable[DimensionRecord],
        update_bbox: bool = True,
    ) -> None:
        """Add multiple spatial dimension records to the display.

        Parameters
        ----------
        records: `~collections.abc.Iterable` [ \
                `lsst.daf.butler.DimensionRecord` ]
            Spatial dimension records.  Must have a ``region`` attributes that
            are `lsst.sphgeom.ConvexPolygon` objects.  Records with the same
            dimension element and data IDs are automatically deduplicated.
        update_bbox : `bool`, optional
            If `True` (default) update the internal bounding box used to set
            plot limits and (possibly) the center of the projection to include
            the bounding boxes of these records' regions.

        Notes
        -----
        This method does not actually do any plotting; it just updates internal
        state that determines what will be plotted when `draw` is called.
        """
        for record in records:
            self.add_record(record, update_bbox=update_bbox)

    def add_repo(self, butler: Butler) -> None:
        """Add all non-skypix spatial dimension records in a repository.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            Butler to query for dimension records.

        Notes
        -----
        This method should not be used on data repositories with full-sky
        skymaps or a lot of visits.  It is intended for inspecting small test
        datasets.
        """
        for element in ["visit", "tract", "patch", "visit_detector_region"]:
            self.add_records(butler.registry.queryDimensionRecords(element))
        self.add_skypix_grid(butler.dimensions, butler.dimensions.commonSkyPix.name)

    def add_skypix_grid(self, universe: DimensionUniverse, element: str | None = None) -> None:
        """Add a grid for a skypix dimension that covers the current bounding
        box of all reagions added so far.

        Parameters
        ----------
        universe : `lsst.daf.butler.DimensionUniverse`
            Object that defines all dimensions.
        element : `str`, optional
            Name of the skypix system, e.g. ``healpix11``.  Default is
            ``universe.commonSkyPix.name``.

        Notes
        -----
        This method does not actually do any plotting; it just updates internal
        state that determines what will be plotted when `draw` is called.  It
        should typically be called just before `draw`, since its behavior
        depends on what else has already been added to the display, though
        repeated calls can be used to safely extend the grid after the bounding
        box has changed, without any ill effects other than some wasted
        calculations.
        """
        if element is None:
            element = universe.commonSkyPix.name
        pixelization = cast(SkyPixDimension, universe[element]).pixelization
        for begin, end in pixelization.envelope(self._bbox):
            for id in range(begin, end):
                self.add_record(
                    universe[element].RecordClass(id=id, region=pixelization.pixel(id)), update_bbox=False
                )

    def draw(self) -> bokeh.plotting.figure:
        """Create a Bokeh figure object from the records that have been added.

        Returns
        -------
        fig : `bokeh.figure.Figure`
            Bokeh figure.  Will need to have `bokeh.plotting.show` or some
            other output function called on it in order to do anything.
        """
        if self._center is None:
            center = SpherePoint(self._bbox.getCenter())
        else:
            center = self._center
        wcs = makeSkyWcs(Point2D(0.0, 0.0), center, makeCdMatrix(1.0 * degrees))
        figure = bokeh.plotting.figure(tooltips=[("data ID", "@data_id")], sizing_mode="stretch_both")
        proj_vertices = np.array(wcs.skyToPixel(self._sphere_vertices), dtype=float)
        for element, regions_by_data_id in self._regions.items():
            if element not in self._styles:
                continue
            x = []
            y = []
            data_id_strs = []
            for region in regions_by_data_id.values():
                region_xy = proj_vertices[region.vertex_slice].transpose().copy()
                x.append(region_xy[0])
                y.append(region_xy[1])
                data_id_strs.append(str(region.record.dataId))
            data = bokeh.models.ColumnDataSource(
                data={
                    "x": x,
                    "y": y,
                    "data_id": data_id_strs,
                }
            )
            figure.patches("x", "y", source=data, **self._styles[element])
        return figure

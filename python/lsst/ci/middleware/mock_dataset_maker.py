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

__all__ = ("MockDatasetMaker", "UNMOCKED_DATASET_TYPES")

from typing import Any, ClassVar, cast

from lsst.daf.butler import Butler, DataCoordinate, DatasetRef, DatasetType, DimensionGraph, SkyPixDimension
from lsst.pipe.base import Pipeline, PipelineGraph
from lsst.pipe.base.tests.mocks import MockDataset, is_mock_name, mock_pipeline_graph
from lsst.resources import ResourcePathExpression
from lsst.sphgeom import Box, ConvexPolygon

from ._constants import MISC_INPUT_RUN, UNMOCKED_DATASET_TYPES


class MockDatasetMaker:
    """A helper class that generates mock datasets given their dataset types.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Writeable butler client.
    """

    def __init__(self, butler: Butler):
        self.butler = butler
        self.bounded_dimensions = butler.dimensions.extract(self._BOUNDED_DIMENSIONS)
        self.cached_data_ids: dict[DimensionGraph, frozenset[DataCoordinate]] = {}
        self._spatial_bounds = None

    _BOUNDED_DIMENSIONS: ClassVar[tuple[str, ...]] = (
        "exposure",
        "visit",
        "detector",
        "tract",
        "patch",
        "band",
    )

    @classmethod
    def prep(cls, root: str, uri: ResourcePathExpression, run: str = MISC_INPUT_RUN) -> None:
        """Add mock input datasets for a pipeline and set up butler formatter
        configuration for all dataset types in that pipeline.

        Parameters
        ----------
        root : `str`
            Butler repository root.
        uri : convertible to `lsst.resources.ResourcePath`'
            URI to the pipeline file.
        run : `str`, optional
            RUN collection that mock datasets should be written to.

        Notes
        -----
        This both registers dataset types and adds mock datasets.  If an
        overall input dataset type of the pipeline is already registered, its
        datasets are assumed to already be present and will not be mocked.
        """
        original = Pipeline.from_uri(uri).to_graph()
        mocked = mock_pipeline_graph(original, unmocked_dataset_types=UNMOCKED_DATASET_TYPES)
        butler = Butler.from_config(root, writeable=True, run=run)
        maker = cls(butler)
        maker.make_inputs(mocked, run)

    def make_inputs(self, graph: PipelineGraph, run: str = MISC_INPUT_RUN) -> None:
        """Add mock input datasets for a pipeline.

        Parameters
        ----------
        pipeline_graph : `lsst.pipe.base.PipelineGraph`
            Already-mocked pipeline graph whose inputs should be created.
        run : `str`, optional
            RUN collection that mock datasets should be written to.

        Notes
        -----
        This both registers dataset types and adds mock datasets.  If an
        overall input dataset type of the pipeline is already registered, its
        datasets are assumed to already be present and will not be mocked.
        """
        graph.resolve(self.butler.registry)
        for _, dataset_type_node in graph.iter_overall_inputs():
            assert dataset_type_node is not None, "Guaranteed by 'resolve' call above."
            self.make_datasets(dataset_type_node.dataset_type, run)

    def make_datasets(self, dataset_type: DatasetType, run: str = MISC_INPUT_RUN) -> None:
        """Add mock datasets of the given type.

        Parameters
        ----------
        dataset_type : `lsst.daf.butler.DatasetType`
            Dataset type to register and create mock datasets for.
        run : `str`, optional
            RUN collection that mock datasets should be written to.

        Notes
        -----
        This both registers the dataset type and adds mock datasets for it.  If
        the dataset typeis already registered, its datasets are assumed to
        already be present and will not be added.
        """
        if not is_mock_name(dataset_type.name):
            return
        if not self.butler.registry.registerDatasetType(dataset_type):
            return
        if (data_ids := self.cached_data_ids.get(dataset_type.dimensions)) is None:
            remaining_skypix_dimensions, dimensions = self._split_dimensions(dataset_type.dimensions)
            data_ids = self._get_bounded_data_ids(dimensions)
            while remaining_skypix_dimensions:
                skypix_dimension = remaining_skypix_dimensions.pop()
                pixelization = skypix_dimension.pixelization
                next_data_ids = set()
                next_dimensions = self.butler.dimensions.extract(list(dimensions.names) + [skypix_dimension])
                for data_id in data_ids:
                    for begin, end in pixelization.envelope(
                        data_id.region if dimensions.spatial else self.spatial_bounds
                    ):
                        for index in range(begin, end):
                            kwargs: dict[str, Any] = {skypix_dimension.name: index}
                            next_data_ids.add(
                                self.butler.registry.expandDataId(data_id, graph=next_dimensions, **kwargs)
                            )
                data_ids = frozenset(next_data_ids)
                dimensions = next_dimensions
                self.cached_data_ids[dimensions] = data_ids
        for data_id in data_ids:
            ref = DatasetRef(dataset_type, data_id, run=run)
            self.butler.put(
                MockDataset(
                    dataset_id=ref.id,
                    dataset_type=dataset_type.to_simple(),
                    data_id=data_id.full.byName(),
                    run=run,
                ),
                ref,
            )

    @property
    def spatial_bounds(self) -> Box:
        """Latitude-longitude bounding box over which mock datasets with
        spatial data IDs should be added.
        """
        if self._spatial_bounds is None:
            spatial_bounds = Box()
            for data_id in self._get_bounded_data_ids(self.butler.dimensions["tract"].graph):
                spatial_bounds.expandTo(cast(ConvexPolygon, data_id.region).getBoundingBox())
            for data_id in self._get_bounded_data_ids(self.butler.dimensions["visit"].graph):
                spatial_bounds.expandTo(cast(ConvexPolygon, data_id.region).getBoundingBox())
            self._spatial_bounds = spatial_bounds
        return self._spatial_bounds

    def _get_bounded_data_ids(self, dimensions: DimensionGraph) -> frozenset[DataCoordinate]:
        """Return data IDs bounded by the content of the data repository, and
        cache them.

        Parameters
        ----------
        dimensions : `lsst.daf.butler.DimensionGraph`
            Dimensions of returned data IDs.

        Returns
        -------
        data_ids : `frozenset` [ `lsst.daf.butler.DataCoordinate` ]
            Fully-expanded data IDs.
        """
        if (data_ids := self.cached_data_ids.get(dimensions)) is None:
            data_ids = frozenset(self.butler.registry.queryDataIds(dimensions).expanded())
            self.cached_data_ids[dimensions] = data_ids
        return data_ids

    def _split_dimensions(self, dimensions: DimensionGraph) -> tuple[set[SkyPixDimension], DimensionGraph]:
        """Split dimensions into skypix dimensions and the rest.

        Parameters
        ----------
        dimensions : `lsst.daf.butler.DimensionGraph`
            Dimensions to split.

        Returns
        -------
        skypix : `set` [ `lsst.daf.butler.SkyPixDimension` ]
            SkyPix dimensions, which can only be bounded by the spatial
            extent of other data IDs.
        bounded : `lsst.daf.butler.DimensionGraph`
            All other dimensions, which can be bounded by querying for the
            data IDs in the repository.
        """
        skypix_dimensions = set()
        bounded_dimensions = set()
        for dimension in dimensions:
            if isinstance(dimension, SkyPixDimension):
                skypix_dimensions.add(dimension)
            elif dimension not in self.bounded_dimensions:
                raise NotImplementedError(
                    f"Cannot make mock dataset with unbounded dimension {dimension.name!r}"
                )
            else:
                bounded_dimensions.add(dimension)
        return skypix_dimensions, self.butler.dimensions.extract(bounded_dimensions)

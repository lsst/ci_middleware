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

import itertools
import os
from collections.abc import Sequence
from typing import Any, ClassVar, cast

from lsst.daf.butler import Butler, DataCoordinate, DatasetRef, DatasetType, DimensionGraph, SkyPixDimension
from lsst.pipe.base import Pipeline, PipelineDatasetTypes, TaskDef
from lsst.pipe.base.tests.mocks import MockDataset, MockStorageClass, is_mock_name, mock_task_defs
from lsst.resources import ResourcePathExpression
from lsst.sphgeom import Box, ConvexPolygon

from ._constants import (
    INPUT_FORMATTERS_CONFIG_DIR,
    MISC_INPUT_RUN,
    PIPELINE_FORMATTERS_CONFIG_DIR,
    UNMOCKED_DATASET_TYPES,
)


class MockDatasetMaker:
    """A helper class that generates mock datasets given their dataset types.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Writeable butler client.
    """

    def __init__(self, butler: Butler):
        self.butler = butler
        self.bounded_dimensions = butler.registry.dimensions.extract(self._BOUNDED_DIMENSIONS)
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
        original = Pipeline.from_uri(uri).toExpandedPipeline()
        mocked = mock_task_defs(original, unmocked_dataset_types=UNMOCKED_DATASET_TYPES)
        pipeline_config_dir = os.path.join(root, PIPELINE_FORMATTERS_CONFIG_DIR)
        MockStorageClass.make_formatter_config_dir(pipeline_config_dir)
        search_path = [pipeline_config_dir]
        input_config_dir = os.path.join(root, INPUT_FORMATTERS_CONFIG_DIR)
        if os.path.exists(input_config_dir):
            search_path.append(input_config_dir)
        butler = Butler(
            root,
            writeable=True,
            run=run,
            searchPaths=search_path,
        )
        maker = cls(butler)
        maker.make_inputs(mocked, run)

    def make_inputs(self, task_defs: Sequence[TaskDef], run: str = MISC_INPUT_RUN) -> PipelineDatasetTypes:
        """Add mock input datasets for a pipeline.

        Parameters
        ----------
        task_defs : `collections.abc.Sequence` [ `lsst.pipe.base.TaskDef` ]
            Expanded, already-mocked pipeline whose inputs should be created.
        run : `str`, optional
            RUN collection that mock datasets should be written to.

        Notes
        -----
        This both registers dataset types and adds mock datasets.  If an
        overall input dataset type of the pipeline is already registered, its
        datasets are assumed to already be present and will not be mocked.
        """
        dataset_types = PipelineDatasetTypes.fromPipeline(task_defs, registry=self.butler.registry)
        for dataset_type in itertools.chain(dataset_types.inputs, dataset_types.prerequisites):
            self.make_datasets(dataset_type, run)
        return dataset_types

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
                next_dimensions = self.butler.registry.dimensions.extract(
                    list(dimensions.names) + [skypix_dimension]
                )
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
            self.butler.put(MockDataset(ref=ref.to_simple()), ref)

    @property
    def spatial_bounds(self) -> Box:
        """Latitude-longitude bounding box over which mock datasets with
        spatial data IDs should be added.
        """
        if self._spatial_bounds is None:
            spatial_bounds = Box()
            for data_id in self._get_bounded_data_ids(self.butler.registry.dimensions["tract"].graph):
                spatial_bounds.expandTo(cast(ConvexPolygon, data_id.region).getBoundingBox())
            for data_id in self._get_bounded_data_ids(self.butler.registry.dimensions["visit"].graph):
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
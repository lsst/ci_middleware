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

__all__ = ("InstrumentRecords", "ObservationRecords", "make_skymap_instance", "RepoData")

import dataclasses
import itertools
import json
import os.path
import shutil
from collections import defaultdict
from collections.abc import Iterable
from typing import Any, cast

import pydantic
from lsst.daf.butler import (
    Butler,
    CollectionType,
    DatasetType,
    DimensionRecord,
    DimensionUniverse,
    SerializedDatasetType,
    SerializedDimensionRecord,
)
from lsst.pipe.base.tests.mocks import MockStorageClass, get_original_name
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.skymap import BaseSkyMap, DiscreteSkyMap
from lsst.sphgeom import ConvexPolygon

from ._constants import (
    BANDS,
    DEFAULTS_COLLECTION,
    DETECTORS,
    INPUT_FORMATTERS_CONFIG_DIR,
    INSTRUMENT,
    MISC_INPUT_RUN,
    SKYMAP,
)
from .mock_dataset_maker import MockDatasetMaker

FOCUS_HTM7_ID = 231866

INSTRUMENT_RECORDS_FILENAME = os.path.join("data", "instrument-records.json")
OBSERVATION_RECORDS_FILENAME = os.path.join("data", "observation-records.json")
INPUT_DATASET_TYPES_FILENAME = os.path.join("data", "input-dataset-types.json")
SKYMAP_CONFIG_FILENAME = os.path.join("data", "skymap-config.py")
INPUT_DATASET_TYPES_FILENAME = os.path.join("data", "input-dataset-types.json")

CALIBS_COLLECTION = "HSC/calibs"


@dataclasses.dataclass
class InstrumentRecords:
    """Struct that manages test-data dimension records typically inserted by
    instrument registration.
    """

    instrument: DimensionRecord
    physical_filter: list[DimensionRecord] = dataclasses.field(default_factory=list)
    detector: list[DimensionRecord] = dataclasses.field(default_factory=list)

    @classmethod
    def from_parent_repo(
        cls,
        butler: Butler,
        instrument: str = INSTRUMENT,
        detectors: tuple[int, ...] = DETECTORS,
        bands: tuple[str, ...] = BANDS,
    ) -> InstrumentRecords:
        """Load records from an external data repository that is being used to
        bootstrap or replace this CI package's embedded data.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            Butler client for the external data repository.
        instrument : `str`, optional
            Short (data ID) name of the instrument.
        detectors : `tuple` [ `int`, ... ], optional
            Detector IDs to include.  This is expected to be a small subset of
            the real instrument's detectors; we'll pretend the instrument only
            has these detectors in the test repository to keep data volume
            small, but otherwise use its real obs_ package and dimension
            records.
        bands : `tuple` [ `str`, ... ], optional
            Band names to include.  Like ``detectors``, this is expected to
            be a small subset of the real suite of bands.

        Returns
        -------
        records : `InstrumentRecords`
            Struct containing instrument records.
        """
        (instrument_record,) = butler.registry.queryDimensionRecords("instrument", instrument=instrument)
        result = cls(instrument_record)
        result.physical_filter.extend(
            butler.registry.queryDimensionRecords(
                "physical_filter",
                instrument=instrument,
                where="band IN (bands)",
                bind={"bands": bands},
            )
        )
        result.detector.extend(
            butler.registry.queryDimensionRecords(
                "detector",
                instrument=instrument,
                where="detector IN (detectors)",
                bind={"detectors": detectors},
            )
        )
        return result

    def write(
        self, filename: str = os.path.join(os.path.dirname(__file__), INSTRUMENT_RECORDS_FILENAME)
    ) -> None:
        """Write the records to a JSON file.

        Parameters
        ----------
        filename : `str`, optional
            Filename to write to.  Defaults to the location in this package
            where it should be committed to version control when boostrapping
            or replacing this CI package's embedded data.
        """
        with open(filename, "w") as stream:
            json.dump(
                {
                    "instrument": self.instrument.to_simple().dict(),
                    "physical_filter": [r.to_simple().dict() for r in self.physical_filter],
                    "detector": [r.to_simple().dict() for r in self.detector],
                },
                stream,
                indent=2,
            )
            # json.dump doesn't add newlines at ends of files, which runs afoul
            # of our linting.
            stream.write("\n")

    @classmethod
    def read(
        cls,
        universe: DimensionUniverse,
        uri: ResourcePathExpression = f"resource://lsst.ci.middleware/{INSTRUMENT_RECORDS_FILENAME}",
    ) -> InstrumentRecords:
        """Read the records from a JSON file.

        Parameters
        ----------
        universe : `lsst.daf.butler.DimensionUniverse`
            Definition of the dimension data model.
        uri : convertible to `lsst.resources.ResourcePath`, optional
            File to load.  Defaults to the location in this package where it
            should have been committed to version control

        Returns
        -------
        records : `InstrumentRecords`
            Loaded records struct.
        """
        uri = ResourcePath(uri)
        with uri.open() as stream:
            data = json.load(stream)
        result = cls(
            instrument=DimensionRecord.from_simple(
                SerializedDimensionRecord(**data["instrument"]), universe=universe
            )
        )
        result.physical_filter.extend(
            DimensionRecord.from_simple(SerializedDimensionRecord(**item), universe=universe)
            for item in data["physical_filter"]
        )
        result.detector.extend(
            DimensionRecord.from_simple(SerializedDimensionRecord(**item), universe=universe)
            for item in data["detector"]
        )
        return result


@dataclasses.dataclass
class ObservationRecords:
    """Struct that manages test-data dimension records typically inserted by
    raw ingest and visit definition.

    This class currently assumes all visit definitions are one-to-one and that
    visit and exposure IDs are interchangeable.
    """

    exposure: list[DimensionRecord] = dataclasses.field(default_factory=list)
    visit: list[DimensionRecord] = dataclasses.field(default_factory=list)
    visit_detector_region: list[DimensionRecord] = dataclasses.field(default_factory=list)
    visit_system: list[DimensionRecord] = dataclasses.field(default_factory=list)
    visit_system_membership: list[DimensionRecord] = dataclasses.field(default_factory=list)
    visit_definition: list[DimensionRecord] = dataclasses.field(default_factory=list)

    @classmethod
    def from_parent_repo_constraints(
        cls,
        butler: Butler,
        instrument: str = INSTRUMENT,
        detectors: tuple[int, ...] = DETECTORS,
        bands: tuple[str, ...] = BANDS,
        **kwargs: Any,
    ) -> ObservationRecords:
        """Load records from an external data repository using a simple query
        over visits.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            Butler client for the external data repository.
        instrument : `str`, optional
            Short (data ID) name of the instrument.
        detectors : `tuple` [ `int`, ... ], optional
            Detector IDs to include.  This is expected to be a small subset of
            the real instrument's detectors; we'll pretend the instrument only
            has these detectors in the test repository to keep data volume
            small, but otherwise use its real obs_ package and dimension
            records.
        bands : `tuple` [ `str`, ... ], optional
            Band names to include.  Like ``detectors``, this is expected to
            be a small subset of the real suite of bands.
        **kwargs
            Additional keyword arguments are data ID key-value pairs that
            constrain the set of visits.  When not provided, defaults to
            an `htm7` spatial constraint.

        Returns
        -------
        records : `ObservationRecords`
            Struct containing observation records.
        """
        if not kwargs:
            kwargs["htm7"] = FOCUS_HTM7_ID
        visit_ids = list(
            {
                cast(int, data_id["visit"])
                for data_id in butler.registry.queryDataIds(
                    ["visit", "detector"],
                    instrument=instrument,
                    where="band IN (bands) AND detector IN (detectors)",
                    bind={"detectors": detectors, "bands": bands},
                    **kwargs,
                )
            }
        )
        return cls.from_parent_repo_visits(butler, visit_ids, instrument, detectors)

    @classmethod
    def from_parent_repo_visits(
        cls,
        butler: Butler,
        visit_ids: list[int],
        instrument: str = INSTRUMENT,
        detectors: tuple[int, ...] = DETECTORS,
    ) -> ObservationRecords:
        """Load records from an external data repository using a query with
        explicit visit IDs.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            Butler client for the external data repository.
        visit_ids : `list` [ `int` ]
            Integer visit IDs to include.
        instrument : `str`, optional
            Short (data ID) name of the instrument.
        detectors : `tuple` [ `int`, ... ], optional
            Detector IDs to include.  This is expected to be a small subset of
            the real instrument's detectors; we'll pretend the instrument only
            has these detectors in the test repository to keep data volume
            small, but otherwise use its real obs_ package and dimension
            records.
        bands : `tuple` [ `str`, ... ], optional
            Band names to include.  Like ``detectors``, this is expected to
            be a small subset of the real suite of bands.

        Returns
        -------
        records : `ObservationRecords`
            Struct containing observation records.
        """
        result = cls()
        result._fill_visit_detector_region(butler, visit_ids, instrument, detectors)
        result._fill_visit_definition_elements(butler, visit_ids, instrument)
        result._fill_visit(butler, visit_ids, instrument)
        return result

    def find_removal_candidate(self, **kwargs: Any) -> tuple[int, tuple]:
        """Find the best visit to remove while preserving overall heterogeneity
        of the mock dataset.

        Parameters
        ----------
        **kwargs
            Keyword arguments may either be:

            - The name of an exposure dimension record field and the value that
              field must have in order to consider that exposure for removal.
            - An arbitrary name and a callable that extracts some hashable
              quantity from an exposure dimension record.  Records will be
              binned by these quantities, and a record from the largest bin
              will be removed.

        Returns
        -------
        visit_id : `int`
            Exposure/visit ID to consider removing.
        bin_key : `tuple`
            Tuple of extracted values that defined the bin with the most
            records.
        """
        fixed = {}
        extractors = {}
        for k, v in kwargs.items():
            if callable(v):
                extractors[k] = v
            else:
                fixed[k] = v
        binned: defaultdict[tuple, list[int]] = defaultdict(list)
        for record in self.exposure:
            if any(getattr(record, k) != v for k, v in fixed.items()):
                continue
            bin_key = []
            for k, extract in extractors.items():
                value = extract(record)
                bin_key.append(value)
            binned[tuple(bin_key)].append(record.id)
        biggest_bin_key = max(binned.keys(), key=lambda k: len(binned[k]))
        visit = binned[biggest_bin_key].pop()
        return visit, biggest_bin_key

    def remove_visit(self, id: int) -> None:
        """Remove a visit from all records."""
        self.exposure = [r for r in self.exposure if r.id != id]
        self.visit = [r for r in self.visit if r.id != id]
        self.visit_detector_region = [r for r in self.visit_detector_region if r.visit != id]
        self.visit_system_membership = [r for r in self.visit_system_membership if r.visit != id]
        self.visit_definition = [r for r in self.visit_definition if r.visit != id]

    def write(
        self, filename: str = os.path.join(os.path.dirname(__file__), OBSERVATION_RECORDS_FILENAME)
    ) -> None:
        """Write the records to a JSON file.

        Parameters
        ----------
        filename : `str`, optional
            Filename to write to.  Defaults to the location in this package
            where it should be committed to version control when boostrapping
            or replacing this CI package's embedded data.
        """
        with open(filename, "w") as stream:
            json.dump(
                {
                    f.name: [r.to_simple().dict() for r in getattr(self, f.name)]
                    for f in dataclasses.fields(self)
                },
                stream,
                indent=2,
            )
            # json.dump doesn't add newlines at ends of files, which runs afoul
            # of our linting.
            stream.write("\n")

    @classmethod
    def read(
        cls,
        universe: DimensionUniverse,
        uri: ResourcePathExpression = f"resource://lsst.ci.middleware/{OBSERVATION_RECORDS_FILENAME}",
    ) -> ObservationRecords:
        """Read the records from a JSON file.

        Parameters
        ----------
        universe : `lsst.daf.butler.DimensionUniverse`
            Definition of the dimension data model.
        uri : convertible to `lsst.resources.ResourcePath`, optional
            File to load.  Defaults to the location in this package where it
            should have been committed to version control

        Returns
        -------
        records : `ObservationRecords`
            Loaded records struct.
        """
        uri = ResourcePath(uri)
        with uri.open() as stream:
            data = json.load(stream)
        result = cls()
        for f in dataclasses.fields(result):
            getattr(result, f.name).extend(
                DimensionRecord.from_simple(SerializedDimensionRecord(**item), universe=universe)
                for item in data[f.name]
            )
        return result

    def _fill_visit_detector_region(
        self, butler: Butler, visit_ids: list[int], instrument: str, detectors: tuple[int, ...]
    ) -> None:
        """Fill in visit_detector_region dimension records.

        This is an implementation detail of `from_parent_repo_visits` split up
        for readability.  See that for parameter descriptions.
        """
        self.visit_detector_region.extend(
            butler.registry.queryDimensionRecords(
                "visit_detector_region",
                instrument=instrument,
                where="visit IN (visit_ids) AND detector IN (detectors)",
                bind={"visit_ids": visit_ids, "detectors": detectors},
            )
        )

    def _fill_visit_definition_elements(self, butler: Butler, visit_ids: list[int], instrument: str) -> None:
        """Fill in dimension records for exposure, visit_system, and the join
        tables that relate those to visit.

        This is an implementation detail of `from_parent_repo_visits` split up
        for readability.  See that for parameter descriptions.
        """
        data_ids = butler.registry.queryDataIds(
            ["exposure", "visit", "visit_system"],
            instrument=instrument,
            where="visit IN (visit_ids)",
            bind={"visit_ids": visit_ids},
        )
        for element_name in ["exposure", "visit_definition", "visit_system", "visit_system_membership"]:
            getattr(self, element_name).extend(
                d.records[element_name]
                for d in data_ids.subset(
                    butler.registry.dimensions[element_name].graph, unique=True
                ).expanded()
            )

    def _fill_visit(self, butler: Butler, visit_ids: list[int], instrument: str) -> None:
        """Fill in dimension records for visit itself.

        This is an implementation detail of `from_parent_repo_visits` split up
        for readability.  See that for parameter descriptions.

        This is the only place where we actually have to modify dimension
        record values (as opposed to just filtering them): we shrink the visit
        regions to correspond to just the detectors we're keeping.
        """
        visit_vector_vertices = defaultdict(list)
        for vdr_record in self.visit_detector_region:
            visit_vector_vertices[vdr_record.visit].extend(vdr_record.region.getVertices())
        for visit_record in butler.registry.queryDimensionRecords(
            "visit",
            instrument=instrument,
            where="visit IN (visit_ids)",
            bind={"visit_ids": visit_ids},
        ):
            d = visit_record.toDict()
            d["region"] = ConvexPolygon(visit_vector_vertices[visit_record.id])
            self.visit.append(type(visit_record)(**d))


def make_skymap_instance(
    uri: ResourcePathExpression = f"resource://lsst.ci.middleware/{SKYMAP_CONFIG_FILENAME}",
) -> DiscreteSkyMap:
    """Make a skymap object for the test repository.

    Parameters
    ----------
    uri : convertible to `lsst.resources.ResourcePath`, optional
        URI to the skymap config file.  Defaults to the location in this
        package where it should have been committed to version control.

    Returns
    -------
    skymap : `lsst.skymap.DiscreteSkyMap`
        Skymap object.
    """
    config = DiscreteSkyMap.ConfigClass()
    uri = ResourcePath(uri)
    config.loadFromString(uri.read())
    return DiscreteSkyMap(config)


class InputDatasetTypes(pydantic.BaseModel):
    """Datasets types used as overall inputs by most mocked pipelines.

    This is not expected to be exhaustive for all pipelines; it's a common
    subset that we want to include in the base repo we'll copy from every time
    we make a per-pipeline repo to test with.

    This class also groups these dataset types by the collection names their
    datasets should be inserted into.
    """

    __root__: dict[str, list[SerializedDatasetType]]

    @property
    def runs(self) -> Iterable[str]:
        """The RUN collections datasets should be written to."""
        return self.__root__.keys()

    def make_formatter_config_dir(self, root: str) -> None:
        """Make a butler config directory (suitable for the ``searchPaths``
        argument to butler construction) with formatter configuration for
        these dataset types.

        Parameters
        ----------
        root : `str`
            Directory that configuration files should be written to.
        """
        for serialized_dataset_type in itertools.chain.from_iterable(self.__root__.values()):
            assert serialized_dataset_type.storageClass is not None
            MockStorageClass.get_or_register_mock(get_original_name(serialized_dataset_type.storageClass))
        MockStorageClass.make_formatter_config_dir(root)

    @classmethod
    def read(
        cls,
        uri: ResourcePathExpression = f"resource://lsst.ci.middleware/{INPUT_DATASET_TYPES_FILENAME}",
    ) -> InputDatasetTypes:
        """Read the dataset types from a JSON file.

        Parameters
        ----------
        uri : convertible to `lsst.resources.ResourcePath`, optional
            File to load.  Defaults to the location in this package where it
            should have been committed to version control

        Returns
        -------
        dataset_types : `InputDatasetTypes`
            Loaded dataset types struct.
        """
        uri = ResourcePath(uri)
        with uri.open() as stream:
            data = json.load(stream)
        return cls.parse_obj(data)

    def resolve(self, universe: DimensionUniverse) -> dict[str, list[DatasetType]]:
        """Return dataset type objects with resolved dimensions.

        Parameters
        ----------
        universe : `lsst.daf.butler.DimensionUniverse`
            Definition of the dimension data model.

        Returns
        -------
        resolved : `dict` [ `str`, `list` [ `lsst.daf.butler.DatasetType` ] ]
            Mapping from RUN collection name to the dataset types whose
            datasets should be inserted into that collection.
        """
        return {
            run: [DatasetType.from_simple(s, universe=universe) for s in serialized_dataset_types]
            for run, serialized_dataset_types in self.__root__.items()
        }


class RepoData:
    """A high-level class for preparing test data repositories.

    Parameters
    ----------
    root : `str`
        Repository root directory.
    clobber : `bool`, optional
        If `True` and ``root`` exists, recursively delete it and make a new
        one.  Otherwise assume any existing path is an already-valid repo that
        may still need to be populated in one or more respects.
    """

    def __init__(self, root: str, clobber: bool = False):
        self.root = ResourcePath(root, forceDirectory=True)
        self.dataset_types = InputDatasetTypes.read()
        if self.root.exists():
            if clobber:
                shutil.rmtree(root, ignore_errors=False)
            else:
                return
        self.formatter_config_dir = os.path.join(root, INPUT_FORMATTERS_CONFIG_DIR)
        self.dataset_types.make_formatter_config_dir(self.formatter_config_dir)
        Butler.makeRepo(self.root)

    @classmethod
    def prep(cls, root: str, clobber: bool = False) -> Butler:
        """Fully prepare a data repository, running all regular methods of
        this class.

        Parameters
        ----------
        root : `str`
            Repository root directory.
        clobber : `bool`, optional
            If `True` and ``root`` exists, recursively delete it and make a new
            one.  Otherwise assume any existing path is an already-valid repo
            that will still need to be populated; this allows for external
            creation of repos with non-default configuration.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            Butler client for the new repository.
        """
        helper = cls(root, clobber=clobber)
        butler = Butler(helper.root, writeable=True, searchPaths=[helper.formatter_config_dir])
        helper.register_instrument(butler)
        helper.insert_observations(butler)
        helper.register_skymap(butler)
        helper.mock_input_datasets(butler)
        helper.make_defaults_collection(butler)
        return butler

    def register_instrument(self, butler: Butler) -> None:
        """Add all instrument-managed records to the repository."""
        instrument_records = InstrumentRecords.read(butler.registry.dimensions)
        butler.registry.insertDimensionData("instrument", instrument_records.instrument)
        butler.registry.insertDimensionData("physical_filter", *instrument_records.physical_filter)
        butler.registry.insertDimensionData("detector", *instrument_records.detector)

    def insert_observations(self, butler: Butler) -> None:
        """Add all observation records to the repository."""
        observation_records = ObservationRecords.read(butler.registry.dimensions)
        for field in dataclasses.fields(observation_records):
            butler.registry.insertDimensionData(field.name, *getattr(observation_records, field.name))

    def register_skymap(self, butler: Butler) -> None:
        """Add all skymap, tract, and patch records to the repository."""
        skymap_instance = make_skymap_instance()
        skymap_instance.register(SKYMAP, butler)

    def mock_input_datasets(self, butler: Butler) -> None:
        """Add mock input datasets that will be used by most pipelines."""
        mock_maker = MockDatasetMaker(butler)
        for run, dataset_types in self.dataset_types.resolve(butler.registry.dimensions).items():
            butler.registry.registerCollection(run, CollectionType.RUN)
            for dataset_type in dataset_types:
                mock_maker.make_datasets(dataset_type, run)

    def make_defaults_collection(self, butler: Butler) -> None:
        """Create default input collections for pipeline graph-building and
        execution.
        """
        defaults = []
        calibs = []
        for run in self.dataset_types.runs:
            if run.startswith(CALIBS_COLLECTION):
                calibs.append(run)
            else:
                defaults.append(run)
        butler.registry.registerCollection(CALIBS_COLLECTION, CollectionType.CHAINED)
        butler.registry.setCollectionChain(CALIBS_COLLECTION, calibs)
        defaults.append(CALIBS_COLLECTION)
        defaults.append(BaseSkyMap.SKYMAP_RUN_COLLECTION_NAME)
        butler.registry.registerCollection(MISC_INPUT_RUN, CollectionType.RUN)
        defaults.append(MISC_INPUT_RUN)
        butler.registry.registerCollection(DEFAULTS_COLLECTION, CollectionType.CHAINED)
        butler.registry.setCollectionChain(DEFAULTS_COLLECTION, defaults)

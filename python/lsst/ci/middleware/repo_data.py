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

__all__ = ("InstrumentRecords", "ObservationRecords")

import dataclasses
import json
import os.path
from collections import defaultdict
from typing import Any, cast

from lsst.daf.butler import Butler, DimensionRecord, DimensionUniverse, SerializedDimensionRecord
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.sphgeom import ConvexPolygon

from ._constants import BANDS, DETECTORS, INSTRUMENT

FOCUS_HTM7_ID = 231866

INSTRUMENT_RECORDS_FILENAME = os.path.join("data", "instrument-records.json")
OBSERVATION_RECORDS_FILENAME = os.path.join("data", "observation-records.json")


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

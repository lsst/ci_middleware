# This file is part of pipe_base.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ("OutputRepoTests",)

import tarfile
import unittest
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from lsst.daf.base import PropertySet
from lsst.daf.butler import Butler
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir
from lsst.pipe.base import QuantumGraph, TaskMetadata
from lsst.pipe.base.tests.mocks import MockDataset, MockDatasetQuantum, get_mock_name

PRODUCT_DIR = Path(__file__).parent.parent.parent.parent.parent.absolute()
TEST_DIR = PRODUCT_DIR.joinpath("tests")
DATA_DIR = PRODUCT_DIR.joinpath("data")


class OutputRepoTests:
    """A unit test helper for checking the outputs of pipeline runs.

    Parameters
    ----------
    name : `str`
        Name of the pipeline, as used in the subdirectory of the ``data``
        directory that holds the data repository ``tar`` file to extract
        and the suffix of the output collection.
    variant : `str`
        Which execution variant to extract a data repository for; either
        "direct" or "qbb".
    expected : `~collections.abc.Mapping`
        Expected data ID values and their relationships, as a mapping from
        ``(tract, patch, band)`` tuple to a `set` of ``visits`` that should
        contribute to the coadd for that ``(tract, patch, band)``.  Values
        may also be `None` to indicate a coadd that should be produced but
        whose inputs visits need not be checked.

    Notes
    -----
    This class manages a temporary data repository created by extracting a
    ``tar`` file from the ``data`` repository.  This is done on first use, not
    immediately, because it is expected to be initialized in
    `unittest.TestCase.setUpClass` implementations in test cases that might not
    use it in all test methods.  Since ``pytest-xdist`` will call
    ``setUpClass`` in each processes that might never run a test method that
    uses it, we don't want to spend time extracting a ``tar`` file and
    initializing a butler we don't (as this can be much slower than any
    particular test method).
    """

    def __init__(self, name: str, variant: str, expected: Mapping[tuple[int, int, str], set[int] | None]):
        self.name = name
        self.variant = variant
        self.expected = expected
        self._root: str | None = None
        self._butler: Butler | None = None

    @property
    def butler(self) -> Butler:
        if self._butler is None:
            self._root = makeTestTempDir(str(TEST_DIR))
            with tarfile.open(DATA_DIR.joinpath(self.name, f"{self.variant}.tgz")) as archive:
                archive.extractall(self._root)
            self._butler = Butler(self._root, collections=f"HSC/runs/{self.name}")
        return self._butler

    def get_quantum_graph(self, step: str | None = None, group: str | None = None) -> QuantumGraph:
        """Return the quantum graph for one step/group of this pipeline's
        execution.

        Note that there is only one QuantumGraph for all variants.
        """
        if step is None:
            step = "full"
        terms = [step]
        if group is not None:
            terms.append(group)
        return QuantumGraph.loadUri(DATA_DIR.joinpath(self.name, "-".join(terms) + ".qgraph"))

    def close(self) -> None:
        """Delete the temporary data repository.

        This should be called in `unittest.TestCase.tearDownClass` whenever an
        instance is constructed in `~unittest.TestCase.setUpClass`.
        """
        if self._root is not None:
            removeTestTempDir(self._root)

    def check_objects(self, test_case: unittest.TestCase) -> None:
        """Run tests on the objectTable and objectTable_tract datasets.

        This includes tests of storage class conversion, since objectTable
        tract is defined as a DataFrame in its producing and consuming tasks,
        but ArrowTable in the data repository."""
        patch_refs = {
            (cast(int, ref.dataId["tract"]), cast(int, ref.dataId["patch"])): ref
            for ref in self.butler.registry.queryDatasets(get_mock_name("objectTable"))
        }
        test_case.assertEqual(
            set(patch_refs.keys()), {(tract, patch) for tract, patch, _ in self.expected.keys()}
        )
        tract_refs = {
            cast(int, ref.dataId["tract"]): ref
            for ref in self.butler.registry.queryDatasets(get_mock_name("objectTable_tract"))
        }
        test_case.assertEqual(set(tract_refs.keys()), {tract for tract, _, _ in self.expected.keys()})
        for tract, tract_ref in tract_refs.items():
            test_case.assertEqual(tract_ref.datasetType.storageClass.name, get_mock_name("DataFrame"))
            tract_dataset: MockDataset = self.butler.get(tract_ref)
            test_case.assertIsNone(tract_dataset.converted_from)
            assert tract_dataset.quantum is not None
            for patch_dataset_as_input in tract_dataset.quantum.inputs["inputCatalogs"]:
                patch = cast(int, patch_dataset_as_input.data_id["patch"])
                patch_ref = patch_refs[tract, patch]
                # We pre-registered this dataset type with ArrowTable as its
                # storage class, even though the task connections all use
                # DataFrame.
                test_case.assertEqual(patch_ref.datasetType.storageClass.name, get_mock_name("ArrowTable"))
                patch_dataset: MockDataset = self.butler.get(patch_ref)
                test_case.assertEqual(patch_dataset.storage_class, get_mock_name("ArrowTable"))
                # Conversion from DataFrame should have happened on write.
                assert patch_dataset.converted_from is not None  # mypy-friendly assert
                test_case.assertEqual(patch_dataset.converted_from.storage_class, get_mock_name("DataFrame"))
                # The objectTable should have been read in as a DataFrame to
                # the task that makes objectTable_tract, i.e. converted on
                # read.
                test_case.assertEqual(patch_dataset_as_input.storage_class, get_mock_name("DataFrame"))
                assert patch_dataset_as_input.converted_from is not None
                test_case.assertEqual(patch_dataset_as_input.converted_from, patch_dataset)

    def check_coadds(self, test_case: unittest.TestCase) -> None:
        """Run tests on coadds and their inputs.

        This checks that the ``expected`` mapping passed at construction
        is consistent with the data repository content."""
        refs = {
            (
                cast(int, ref.dataId["tract"]),
                cast(int, ref.dataId["patch"]),
                cast(str, ref.dataId["band"]),
            ): ref
            for ref in self.butler.registry.queryDatasets(get_mock_name("deepCoadd"))
        }
        test_case.assertEqual(set(refs.keys()), set(self.expected.keys()))
        for key, ref in refs.items():
            dataset: MockDataset = self.butler.get(ref)
            if (expected := self.expected[key]) is not None:
                test_case.assertEqual(
                    {
                        input_dataset.data_id["visit"]
                        for input_dataset in cast(MockDatasetQuantum, dataset.quantum).inputs["inputWarps"]
                    },
                    expected,
                )
            # There should be no storage class conversions or component access.
            test_case.assertIsNone(dataset.converted_from)
            test_case.assertIsNone(dataset.parent)
            test_case.assertIsNone(dataset.converted_from)
            test_case.assertIsNone(dataset.parent)

    def check_final_visit_summary_full_visits(self, test_case: unittest.TestCase, n_visits: int) -> None:
        """Test that full visits were passed to visit summary tests."""
        visit_refs = set(self.butler.registry.queryDatasets(get_mock_name("finalVisitSummary")))
        print([(ref.dataId["visit"], ref.run) for ref in visit_refs])
        test_case.assertEqual(len(visit_refs), n_visits)
        for visit_ref in visit_refs:
            visit_dataset: MockDataset = self.butler.get(visit_ref)
            assert visit_dataset.quantum is not None  # for MyPy
            test_case.assertEqual(len(visit_dataset.quantum.inputs["input_exposures"]), 6)

    def check_property_set_metadata(self, test_case: unittest.TestCase) -> None:
        """Test reading metadata datasets pre-registered as PropertySet."""
        (data_id,) = (
            self.butler.registry.queryDataIds(
                ["exposure", "detector"], datasets=[get_mock_name("isr_metadata")]
            )
            .order_by("exposure", "detector")
            .limit(1)
        )
        test_case.assertIsInstance(self.butler.get(get_mock_name("isr_metadata"), data_id), PropertySet)
        test_case.assertIsInstance(
            self.butler.get(get_mock_name("isr_metadata"), data_id, storageClass="TaskMetadata"),
            TaskMetadata,
        )


if __name__ == "__main__":
    unittest.main()

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

import unittest
from typing import ClassVar

from lsst.ci.middleware.output_repo_tests import OutputRepoTests
from lsst.pipe.base.execution_reports import QuantumGraphExecutionReport
from lsst.pipe.base.tests.mocks import get_mock_name

# (tract, patch, band): {input visits} for coadds produced here.
# some visit lists elided because we're just spot-checking.
EXPECTED = {
    (0, 0, "r"): {96860, 96862},
    (0, 0, "i"): {95104},
    (0, 1, "r"): {96860, 96862},
    (0, 1, "i"): {95104},
    (0, 2, "r"): None,
    (0, 2, "i"): None,
    (0, 3, "r"): None,
    (0, 3, "i"): None,
    (1, 0, "r"): None,
    (1, 0, "i"): None,
    # tract=1, patch=1 has only a single i-band input, so no r-band coadd
    (1, 1, "i"): {96980},
    (1, 2, "r"): None,
    (1, 2, "i"): None,
    # tract=2, patch=3 has only a single i-band input, so no r-band coadd
    (1, 3, "i"): {96980},
    (2, 0, "r"): None,
    (2, 0, "i"): None,
    (2, 1, "r"): None,
    (2, 1, "i"): None,
    # tract=2, patch=2 has only two i-band inputs, so no r-band coadd
    (2, 2, "i"): {18202, 96954},
    # tract=2, patch=3 has only two i-band inputs, so no r-band coadd
    (2, 3, "i"): {18202, 96954},
    (3, 0, "r"): None,
    (3, 0, "i"): None,
    # tract=3, patch=1 has no inputs, and no coadds
    # tract=3, patch=2 has only a single i-band input, so no r-band coadd
    (3, 2, "i"): {96954},
    # tract=3, patch=3 has no inputs, and no coadds
}


class ProdOutputsTestCase(unittest.TestCase):
    """Tests that inspect the outputs of running the mocked Prod pipeline."""

    direct: ClassVar[OutputRepoTests]
    qbb: ClassVar[OutputRepoTests]

    @classmethod
    def setUpClass(cls) -> None:
        cls.direct = OutputRepoTests("Prod", "direct", EXPECTED)
        cls.qbb = OutputRepoTests("Prod", "qbb", EXPECTED)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.direct.close()
        cls.qbb.close()

    def setUp(self) -> None:
        self.maxDiff = None

    def test_direct_qbb_equivalence(self) -> None:
        """Test that the direct and QBB runs produce exactly the same
        collections, dataset types, and datasets."""
        self.assertEqual(
            list(
                self.direct.butler.registry.queryCollections(
                    "HSC/runs/Prod", flattenChains=True, includeChains=True
                )
            ),
            list(
                self.qbb.butler.registry.queryCollections(
                    "HSC/runs/Prod", flattenChains=True, includeChains=True
                )
            ),
        )
        self.assertEqual(
            set(self.direct.butler.registry.queryDatasetTypes(...)),
            set(self.qbb.butler.registry.queryDatasetTypes(...)),
        )
        self.assertEqual(
            set(self.direct.butler.registry.queryDatasets(get_mock_name("isr_config"))),
            set(self.qbb.butler.registry.queryDatasets(get_mock_name("isr_config"))),
        )

    def test_objects_direct(self) -> None:
        self.direct.check_objects(self)

    def test_objects_qbb(self) -> None:
        self.qbb.check_objects(self)

    def test_coadds_direct(self) -> None:
        self.direct.check_coadds(self)

    def test_coadds_qbb(self) -> None:
        self.qbb.check_coadds(self)

    def test_final_visit_summary_full_visits_direct(self) -> None:
        self.direct.check_final_visit_summary_full_visits(self, n_visits=12)

    def test_final_visit_summary_full_visits_qbb(self) -> None:
        self.qbb.check_final_visit_summary_full_visits(self, n_visits=12)

    def test_property_set_metadata_direct(self) -> None:
        self.direct.check_property_set_metadata(self)

    def test_property_set_metadata_qbb(self) -> None:
        self.qbb.check_property_set_metadata(self)

    def check_step1_manifest_checker(self, helper: OutputRepoTests) -> None:
        """Test that the fail-and-recover attempts in step1 worked as expected
        using the manifest checker.
        """

        # This task should have failed in attempt1 and should have been
        # rescued in attempt2.
        qg_1 = helper.get_quantum_graph("step1", "i-attempt1")
        report_1 = QuantumGraphExecutionReport.make_reports(helper.butler, qg_1)
        summary_1 = report_1.to_summary_dict(helper.butler)
        # Check that total between successful, blocked and failed is expected
        for task in summary_1:
            self.assertEqual(
                summary_1[task]["n_expected"],
                sum(
                    [
                        summary_1[task]["n_succeeded"],
                        summary_1[task]["n_quanta_blocked"],
                        len(summary_1[task]["failed_quanta"]),
                    ]
                ),
            )
        failures = summary_1["_mock_calibrate"]["failed_quanta"]
        failed_visits = set()
        for quantum_summary in failures.values():
            self.assertTrue(
                quantum_summary["error"][0].startswith("Execution of task '_mock_calibrate' on quantum")
            )
            failed_visits.add(quantum_summary["data_id"]["visit"])
        self.assertEqual(failed_visits, {18202})
        self.assertEqual(summary_1["_mock_isr"]["outputs"]["_mock_postISRCCD"]["produced"], 36)
        # Now we will make a human_readable report and assert that the outputs
        # are where we expect them to be, a second time.
        hr_summary_1 = report_1.to_summary_dict(helper.butler, human_readable=True)
        failures = hr_summary_1["_mock_calibrate"]["errors"]
        for failure in failures:
            self.assertEqual(failure["data_id"]["visit"], 18202)
            self.assertTrue(failure["error"][0].startswith("Execution of task '_mock_calibrate' on quantum"))
            self.assertEqual(hr_summary_1["_mock_isr"]["outputs"]["_mock_postISRCCD"]["produced"], 36)
        # This task should have succeeded in attempt1 and should not have been
        # included in attempt2.
        qg_2 = helper.get_quantum_graph("step1", "i-attempt2")
        report_2 = QuantumGraphExecutionReport.make_reports(helper.butler, qg_2)
        summary_2 = report_2.to_summary_dict(helper.butler)
        # Check that total between successful, blocked and failed is expected
        for task in summary_2:
            self.assertEqual(
                summary_2[task]["n_expected"],
                sum(
                    [
                        summary_2[task]["n_succeeded"],
                        summary_2[task]["n_quanta_blocked"],
                        len(summary_2[task]["failed_quanta"]),
                    ]
                ),
            )
        self.assertEqual(summary_2["_mock_calibrate"]["failed_quanta"], {})  # is empty ??
        # Making sure it works with the human-readable version,
        hr_summary_2 = report_2.to_summary_dict(helper.butler, human_readable=True)
        self.assertEqual(hr_summary_2["_mock_calibrate"]["failed_quanta"], [])

    def test_step1_manifest_checker_qbb(self) -> None:
        self.check_step1_manifest_checker(self.qbb)


if __name__ == "__main__":
    unittest.main()

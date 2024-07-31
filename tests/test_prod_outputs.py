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
from lsst.pipe.base.quantum_provenance_graph import QuantumProvenanceGraph
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

    def check_step1_execution_reports(self, helper: OutputRepoTests) -> None:
        """Test that the fail-and-recover attempts in step1 worked as expected
        using the `QuantumGraphExecutionReport`.
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

    def test_step1_execution_reports_qbb(self) -> None:
        self.check_step1_execution_reports(self.qbb)

    def check_step1_qpg(self, helper: OutputRepoTests) -> None:
        """Test that the fail-and-recover attempts in step1 worked as expected
        over each attempt, using the `QuantumProvenanceGraph`.
        """

        # Make the quantum provenance graph for the first attempt
        qg_1 = helper.get_quantum_graph("step1", "i-attempt1")
        qpg1 = QuantumProvenanceGraph()
        qpg1.add_new_graph(helper.butler, qg_1)
        qpg1.resolve_duplicates(
            helper.butler, collections=["HSC/runs/Prod/step1-i-attempt1"], where="instrument='HSC'"
        )
        qg_1_sum_only = qpg1.to_summary(helper.butler)
        qg_1_dict = qg_1_sum_only.model_dump()

        # Loop through the tasks in the dict
        for task in qg_1_dict["tasks"]:
            self.assertEqual(qg_1_dict["tasks"][task]["n_not_attempted"], 0)
            self.assertEqual(qg_1_dict["tasks"][task]["n_wonky"], 0)
            self.assertEqual(qg_1_dict["tasks"][task]["n_expected"], 36)
            self.assertListEqual(qg_1_dict["tasks"][task]["wonky_quanta"], [])
            self.assertListEqual(qg_1_dict["tasks"][task]["recovered_quanta"], [])
            self.assertEqual(
                qg_1_dict["tasks"][task]["n_expected"],
                qg_1_dict["tasks"][task]["n_successful"]
                + qg_1_dict["tasks"][task]["n_blocked"]
                + qg_1_dict["tasks"][task]["n_not_attempted"]
                + qg_1_dict["tasks"][task]["n_wonky"]
                + qg_1_dict["tasks"][task]["n_failed"],
            )
            match task:
                case "_mock_calibrate":
                    self.assertEqual(qg_1_dict["tasks"][task]["n_successful"], 30)
                    self.assertEqual(qg_1_dict["tasks"][task]["n_blocked"], 0)
                    self.assertEqual(qg_1_dict["tasks"][task]["n_failed"], 6)
                    for quantum in qg_1_dict["tasks"][task]["failed_quanta"]:
                        self.assertEqual(quantum["data_id"]["instrument"], "HSC")
                        self.assertIsInstance(quantum["data_id"]["detector"], int)
                        self.assertEqual(quantum["data_id"]["visit"], 18202)
                        self.assertDictEqual(quantum["runs"], {"HSC/runs/Prod/step1-i-attempt1": "failed"})
                        self.assertIsInstance(quantum["messages"], list)
                        for message in quantum["messages"]:
                            self.assertIsInstance(message, str)
                            self.assertTrue(
                                message.startswith("Execution of task '_mock_calibrate' on quantum")
                            )
                            self.assertIn(
                                "Exception ValueError: Simulated failure: task=_mock_calibrate", message
                            )
                case _:
                    if task == "_mock_writePreSourceTable" or task == "_mock_transformPreSourceTable":
                        self.assertEqual(qg_1_dict["tasks"][task]["n_successful"], 30)
                        self.assertEqual(qg_1_dict["tasks"][task]["n_blocked"], 6)
                        self.assertEqual(qg_1_dict["tasks"][task]["n_failed"], 0)
                        self.assertListEqual(qg_1_dict["tasks"][task]["failed_quanta"], [])
                    else:
                        self.assertEqual(qg_1_dict["tasks"][task]["n_successful"], 36)
                        self.assertEqual(qg_1_dict["tasks"][task]["n_blocked"], 0)
                        self.assertEqual(qg_1_dict["tasks"][task]["n_failed"], 0)
                        self.assertListEqual(qg_1_dict["tasks"][task]["failed_quanta"], [])

        # Test datasets for the first QPG.
        datasets = [
            "_mock_postISRCCD",
            "_mock_isr_metadata",
            "_mock_isr_log",
            "_mock_icExp",
            "_mock_icSrc",
            "_mock_icExpBackground",
            "_mock_characterizeImage_metadata",
            "_mock_characterizeImage_log",
            "_mock_calexpBackground",
            "_mock_srcMatch",
            "_mock_calexp",
            "_mock_src",
            "_mock_srcMatchFull",
            "_mock_calibrate_metadata",
            "_mock_calibrate_log",
            "_mock_preSource",
            "_mock_writePreSourceTable_metadata",
            "_mock_writePreSourceTable_log",
            "_mock_preSourceTable",
            "_mock_transformPreSourceTable_metadata",
            "_mock_transformPreSourceTable_log",
        ]
        for dataset in datasets:
            self.assertIn(dataset, qg_1_dict["datasets"].keys())
        for dataset in qg_1_dict["datasets"]:
            self.assertEqual(
                list(qg_1_dict["datasets"][dataset].keys()),
                [
                    "producer",
                    "n_published",
                    "n_unpublished",
                    "n_predicted_only",
                    "n_expected",
                    "cursed_datasets",
                    "unsuccessful_datasets",
                    "n_cursed",
                    "n_unsuccessful",
                ],
            )
            self.assertIsInstance(qg_1_dict["datasets"][dataset]["producer"], str)
            # For the expected failure
            if qg_1_dict["datasets"][dataset]["producer"] == "_mock_calibrate":
                # A bit hard to read, but this is actually asserting that it's
                # not empty.

                self.assertTrue(
                    qg_1_dict["datasets"][dataset]["unsuccessful_datasets"],
                    f"Expected failures were not stored as unsuccessful datasets for {dataset}.",
                )
                # Check that the published datasets = expected - (unsuccessful
                # + predicted_only)
                self.assertEqual(
                    qg_1_dict["datasets"][dataset]["n_published"],
                    qg_1_dict["datasets"][dataset]["n_expected"]
                    - qg_1_dict["datasets"][dataset]["n_unsuccessful"]
                    - qg_1_dict["datasets"][dataset]["n_predicted_only"],
                )
                # Check that the unsuccessful datasets are as expected
                self.assertIsInstance(qg_1_dict["datasets"][dataset]["unsuccessful_datasets"], list)
                self.assertEqual(
                    qg_1_dict["datasets"][dataset]["unsuccessful_datasets"][0]["instrument"], "HSC"
                )
                self.assertEqual(qg_1_dict["datasets"][dataset]["unsuccessful_datasets"][0]["visit"], 18202)
                self.assertEqual(qg_1_dict["datasets"][dataset]["unsuccessful_datasets"][0]["band"], "i")
                self.assertEqual(
                    qg_1_dict["datasets"][dataset]["unsuccessful_datasets"][0]["day_obs"], 20150117
                )
                self.assertEqual(
                    qg_1_dict["datasets"][dataset]["unsuccessful_datasets"][0]["physical_filter"],
                    "HSC-I",
                )
                # Check that there are the expected amount of failures
                # and that they are not published
                self.assertEqual(len(qg_1_dict["datasets"][dataset]["unsuccessful_datasets"]), 6)
                self.assertEqual(qg_1_dict["datasets"][dataset]["n_expected"], 36)
                self.assertEqual(qg_1_dict["datasets"][dataset]["n_published"], 30)

            # Check that all the counts add up for every task
            self.assertEqual(
                qg_1_dict["datasets"][dataset]["n_expected"],
                sum(
                    [
                        qg_1_dict["datasets"][dataset]["n_published"],
                        qg_1_dict["datasets"][dataset]["n_unpublished"],
                        qg_1_dict["datasets"][dataset]["n_predicted_only"],
                        qg_1_dict["datasets"][dataset]["n_cursed"],
                        qg_1_dict["datasets"][dataset]["n_unsuccessful"],
                    ]
                ),
            )
            # Check that there are no cursed datasets
            self.assertEqual(qg_1_dict["datasets"][dataset]["n_cursed"], 0)
            self.assertListEqual(qg_1_dict["datasets"][dataset]["cursed_datasets"], [])

        # Make an overall QPG and add the recovery attempt to the QPG
        qpg = QuantumProvenanceGraph()
        qg_2 = helper.get_quantum_graph("step1", "i-attempt2")
        qpg.add_new_graph(helper.butler, qg_1)
        qpg.add_new_graph(helper.butler, qg_2)
        qpg.resolve_duplicates(
            helper.butler,
            collections=["HSC/runs/Prod/step1-i-attempt2", "HSC/runs/Prod/step1-i-attempt1"],
            where="instrument='HSC'",
        )
        qg_sum = qpg.to_summary(helper.butler)

        qg_2_dict = qg_sum.model_dump()

        for task in qg_2_dict["tasks"]:
            self.assertEqual(qg_2_dict["tasks"][task]["n_successful"], 36)
            self.assertEqual(qg_2_dict["tasks"][task]["n_blocked"], 0)
            self.assertEqual(qg_2_dict["tasks"][task]["n_failed"], 0)
            self.assertEqual(qg_2_dict["tasks"][task]["n_not_attempted"], 0)
            self.assertEqual(qg_2_dict["tasks"][task]["n_wonky"], 0)
            self.assertEqual(qg_2_dict["tasks"][task]["n_expected"], 36)
            self.assertListEqual(qg_2_dict["tasks"][task]["wonky_quanta"], [])
            self.assertListEqual(qg_2_dict["tasks"][task]["failed_quanta"], [])
            self.assertEqual(
                qg_2_dict["tasks"][task]["n_expected"],
                qg_2_dict["tasks"][task]["n_successful"]
                + qg_2_dict["tasks"][task]["n_blocked"]
                + qg_2_dict["tasks"][task]["n_not_attempted"]
                + qg_2_dict["tasks"][task]["n_wonky"]
                + qg_2_dict["tasks"][task]["n_failed"],
            )
            if (
                task == "_mock_calibrate"
                or task == "_mock_writePreSourceTable"
                or task == "_mock_transformPreSourceTable"
            ):
                for quantum in qg_2_dict["tasks"][task]["recovered_quanta"]:
                    self.assertEqual(quantum["instrument"], "HSC")
                    self.assertEqual(quantum["visit"], 18202)
            else:
                self.assertListEqual(qg_2_dict["tasks"][task]["recovered_quanta"], [])

            # Test datasets for the overall QPG.
            # Check that we have the expected datasets
            datasets = [
                "_mock_postISRCCD",
                "_mock_isr_metadata",
                "_mock_isr_log",
                "_mock_icExp",
                "_mock_icSrc",
                "_mock_icExpBackground",
                "_mock_characterizeImage_metadata",
                "_mock_characterizeImage_log",
                "_mock_calexpBackground",
                "_mock_srcMatch",
                "_mock_calexp",
                "_mock_src",
                "_mock_srcMatchFull",
                "_mock_calibrate_metadata",
                "_mock_calibrate_log",
                "_mock_preSource",
                "_mock_writePreSourceTable_metadata",
                "_mock_writePreSourceTable_log",
                "_mock_preSourceTable",
                "_mock_transformPreSourceTable_metadata",
                "_mock_transformPreSourceTable_log",
            ]
            for dataset in datasets:
                self.assertIn(dataset, qg_2_dict["datasets"].keys())
            # Check that they are the same datasets
            self.assertEqual(qg_2_dict["datasets"].keys(), qg_1_dict["datasets"].keys())
            for dataset in qg_2_dict["datasets"]:
                # Check that each dataset has the same information
                self.assertEqual(
                    list(qg_2_dict["datasets"][dataset].keys()),
                    [
                        "producer",
                        "n_published",
                        "n_unpublished",
                        "n_predicted_only",
                        "n_expected",
                        "cursed_datasets",
                        "unsuccessful_datasets",
                        "n_cursed",
                        "n_unsuccessful",
                    ],
                )
                self.assertIsInstance(qg_2_dict["datasets"][dataset]["producer"], str)
                # Check counts: we should have recovered everything, so
                # published should equal expected for each dataset.
                self.assertEqual(
                    qg_2_dict["datasets"][dataset]["n_expected"],
                    qg_2_dict["datasets"][dataset]["n_published"],
                )
                # Check that this is the expected number
                self.assertEqual(qg_2_dict["datasets"][dataset]["n_published"], 36)
                # Check that they all add up
                self.assertEqual(
                    qg_2_dict["datasets"][dataset]["n_expected"],
                    sum(
                        [
                            qg_2_dict["datasets"][dataset]["n_published"],
                            qg_2_dict["datasets"][dataset]["n_unpublished"],
                            qg_2_dict["datasets"][dataset]["n_predicted_only"],
                            qg_2_dict["datasets"][dataset]["n_cursed"],
                            qg_2_dict["datasets"][dataset]["n_unsuccessful"],
                        ]
                    ),
                )
                # Check that there are no cursed or unsuccessful datasets
                self.assertEqual(qg_2_dict["datasets"][dataset]["n_cursed"], 0)
                self.assertListEqual(qg_2_dict["datasets"][dataset]["cursed_datasets"], [])
                self.assertEqual(qg_2_dict["datasets"][dataset]["n_unsuccessful"], 0)
                self.assertListEqual(qg_2_dict["datasets"][dataset]["unsuccessful_datasets"], [])

                # Since we have recovered everything, we should have the same
                # numbers for every task:
                self.assertEqual(qg_2_dict["datasets"][dataset]["n_expected"], 36)
                self.assertEqual(qg_2_dict["datasets"][dataset]["n_published"], 36)
                self.assertEqual(qg_2_dict["datasets"][dataset]["n_unpublished"], 0)
                self.assertEqual(qg_2_dict["datasets"][dataset]["n_predicted_only"], 0)

    def test_step1_quantum_provenance_graph_qbb(self) -> None:
        self.check_step1_qpg(self.qbb)


if __name__ == "__main__":
    unittest.main()

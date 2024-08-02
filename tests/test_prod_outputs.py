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
        qg_1_sum = qpg1.to_summary(helper.butler)

        # Loop through the tasks in the dict
        for label, task_summary in qg_1_sum.tasks.items():
            self.assertEqual(task_summary.n_not_attempted, 0)
            self.assertEqual(task_summary.n_wonky, 0)
            self.assertEqual(task_summary.n_expected, 36)
            self.assertListEqual(task_summary.wonky_quanta, [])
            self.assertListEqual(task_summary.recovered_quanta, [])
            self.assertEqual(
                task_summary.n_expected,
                task_summary.n_successful
                + task_summary.n_blocked
                + task_summary.n_not_attempted
                + task_summary.n_wonky
                + task_summary.n_failed,
            )
            match label:
                case "_mock_calibrate":
                    self.assertEqual(task_summary.n_successful, 30)
                    self.assertEqual(task_summary.n_blocked, 0)
                    self.assertEqual(task_summary.n_failed, 6)
                    for quantum_summary in task_summary.failed_quanta:
                        self.assertEqual(quantum_summary.data_id["instrument"], "HSC")
                        self.assertIsInstance(quantum_summary.data_id["detector"], int)
                        self.assertEqual(quantum_summary.data_id["visit"], 18202)
                        self.assertDictEqual(
                            quantum_summary.runs, {"HSC/runs/Prod/step1-i-attempt1": "failed"}
                        )
                        self.assertIsInstance(quantum_summary.messages, list)
                        for message in quantum_summary.messages:
                            self.assertIsInstance(message, str)
                            self.assertTrue(
                                message.startswith("Execution of task '_mock_calibrate' on quantum")
                            )
                            self.assertIn(
                                "Exception ValueError: Simulated failure: task=_mock_calibrate", message
                            )
                case _:
                    if label == "_mock_writePreSourceTable" or label == "_mock_transformPreSourceTable":
                        self.assertEqual(task_summary.n_successful, 30)
                        self.assertEqual(task_summary.n_blocked, 6)
                        self.assertEqual(task_summary.n_failed, 0)
                        self.assertListEqual(task_summary.failed_quanta, [])
                    else:
                        self.assertEqual(task_summary.n_successful, 36)
                        self.assertEqual(task_summary.n_blocked, 0)
                        self.assertEqual(task_summary.n_failed, 0)
                        self.assertListEqual(task_summary.failed_quanta, [])

        # Test datasets for the first QPG.
        for dataset_type_name, dataset_summary in qg_1_sum.datasets.items():
            # For the expected failure
            if dataset_summary.producer == "_mock_calibrate":
                # A bit hard to read, but this is actually asserting that it's
                # not empty + showing an error if it is.

                self.assertTrue(
                    dataset_summary.unsuccessful_datasets,
                    f"Expected failures were not stored as unsuccessful datasets for {dataset_type_name}.",
                )
                # Check that the published datasets = expected - (unsuccessful
                # + predicted_only)
                self.assertEqual(
                    dataset_summary.n_published,
                    dataset_summary.n_expected
                    - dataset_summary.n_unsuccessful
                    - dataset_summary.n_predicted_only,
                )
                # Check that the unsuccessful datasets are as expected
                self.assertIsInstance(dataset_summary.unsuccessful_datasets, list)
                self.assertEqual(dataset_summary.unsuccessful_datasets[0]["instrument"], "HSC")
                self.assertEqual(dataset_summary.unsuccessful_datasets[0]["visit"], 18202)
                self.assertEqual(dataset_summary.unsuccessful_datasets[0]["band"], "i")
                self.assertEqual(dataset_summary.unsuccessful_datasets[0]["day_obs"], 20150117)
                self.assertEqual(
                    dataset_summary.unsuccessful_datasets[0]["physical_filter"],
                    "HSC-I",
                )
                # Check that there are the expected amount of failures
                # and that they are not published
                self.assertEqual(len(dataset_summary.unsuccessful_datasets), 6)
                self.assertEqual(dataset_summary.n_expected, 36)
                self.assertEqual(dataset_summary.n_published, 30)

            # Check that all the counts add up for every task
            self.assertEqual(
                dataset_summary.n_expected,
                sum(
                    [
                        dataset_summary.n_published,
                        dataset_summary.n_unpublished,
                        dataset_summary.n_predicted_only,
                        dataset_summary.n_cursed,
                        dataset_summary.n_unsuccessful,
                    ]
                ),
            )
            # Check that there are no cursed datasets
            self.assertEqual(dataset_summary.n_cursed, 0)
            self.assertListEqual(dataset_summary.cursed_datasets, [])

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

        for label, task_summary in qg_sum.tasks.items():
            self.assertEqual(task_summary.n_successful, 36)
            self.assertEqual(task_summary.n_blocked, 0)
            self.assertEqual(task_summary.n_failed, 0)
            self.assertEqual(task_summary.n_not_attempted, 0)
            self.assertEqual(task_summary.n_wonky, 0)
            self.assertEqual(task_summary.n_expected, 36)
            self.assertListEqual(task_summary.wonky_quanta, [])
            self.assertListEqual(task_summary.failed_quanta, [])
            self.assertEqual(
                task_summary.n_expected,
                task_summary.n_successful
                + task_summary.n_blocked
                + task_summary.n_not_attempted
                + task_summary.n_wonky
                + task_summary.n_failed,
            )
            if (
                label == "_mock_calibrate"
                or label == "_mock_writePreSourceTable"
                or label == "_mock_transformPreSourceTable"
            ):
                for quantum in task_summary.recovered_quanta:
                    self.assertEqual(quantum["instrument"], "HSC")
                    self.assertEqual(quantum["visit"], 18202)
            else:
                self.assertListEqual(task_summary.recovered_quanta, [])

        # Test datasets for the overall QPG.
        # Check that we have the expected datasets
        for dataset_summary in qg_sum.datasets.values():
            # Check counts: we should have recovered everything, so
            # published should equal expected for each dataset.
            self.assertEqual(
                dataset_summary.n_expected,
                dataset_summary.n_published,
            )
            # Check that this is the expected number
            self.assertEqual(dataset_summary.n_published, 36)
            # Check that they all add up
            self.assertEqual(
                dataset_summary.n_expected,
                sum(
                    [
                        dataset_summary.n_published,
                        dataset_summary.n_unpublished,
                        dataset_summary.n_predicted_only,
                        dataset_summary.n_cursed,
                        dataset_summary.n_unsuccessful,
                    ]
                ),
            )
            # Check that there are no cursed or unsuccessful datasets
            self.assertEqual(dataset_summary.n_cursed, 0)
            self.assertListEqual(dataset_summary.cursed_datasets, [])
            self.assertEqual(dataset_summary.n_unsuccessful, 0)
            self.assertListEqual(dataset_summary.unsuccessful_datasets, [])

            # Since we have recovered everything, we should have the same
            # numbers for every task:
            self.assertEqual(dataset_summary.n_expected, 36)
            self.assertEqual(dataset_summary.n_published, 36)
            self.assertEqual(dataset_summary.n_unpublished, 0)
            self.assertEqual(dataset_summary.n_predicted_only, 0)

    def test_step1_quantum_provenance_graph_qbb(self) -> None:
        self.check_step1_qpg(self.qbb)


if __name__ == "__main__":
    unittest.main()

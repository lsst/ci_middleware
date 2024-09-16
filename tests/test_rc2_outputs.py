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
from lsst.pipe.base.quantum_provenance_graph import QuantumProvenanceGraph, UnsuccessfulQuantumSummary
from lsst.pipe.base.tests.mocks import MockDataset, get_mock_name

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
}


class Rc2OutputsTestCase(unittest.TestCase):
    """Tests that inspect the outputs of running the mocked RC2 pipeline."""

    direct: ClassVar[OutputRepoTests]
    qbb: ClassVar[OutputRepoTests]

    @classmethod
    def setUpClass(cls) -> None:
        cls.direct = OutputRepoTests("RC2", "direct", EXPECTED)
        cls.qbb = OutputRepoTests("RC2", "qbb", EXPECTED)

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
                    "HSC/runs/RC2", flattenChains=True, includeChains=True
                )
            ),
            list(
                self.qbb.butler.registry.queryCollections(
                    "HSC/runs/RC2", flattenChains=True, includeChains=True
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
        self.direct.check_final_visit_summary_full_visits(self, n_visits=10)

    def test_final_visit_summary_full_visits_qbb(self) -> None:
        self.qbb.check_final_visit_summary_full_visits(self, n_visits=10)

    def test_property_set_metadata_direct(self) -> None:
        self.direct.check_property_set_metadata(self)

    def test_property_set_metadata_qbb(self) -> None:
        self.qbb.check_property_set_metadata(self)

    def check_step5_rescue(self, helper: OutputRepoTests) -> None:
        """Test that the fail-and-recover attempts in step5 worked as expected,
        by running all tasks but one in the first attempt.
        """
        # This task should have failed in attempt1 and should have been
        # rescued in attempt2.
        self.assertCountEqual(
            [
                ref.run
                for ref in set(
                    helper.butler.registry.queryDatasets(
                        get_mock_name("consolidateForcedSourceTable_metadata")
                    )
                )
            ],
            ["HSC/runs/RC2/step5-attempt2"],
        )
        # This task should have succeeded in attempt1 and should not have been
        # included in attempt2.
        self.assertCountEqual(
            [
                ref.run
                for ref in set(
                    helper.butler.registry.queryDatasets(get_mock_name("transformForcedSourceTable_metadata"))
                )
            ],
            ["HSC/runs/RC2/step5-attempt1"] * 4,  # one for each patch,
        )

    def test_step5_rescue_direct(self) -> None:
        self.check_step5_rescue(self.direct)
        # The attempt1 QG should have quanta for both tasks (and others, but we
        # won't list them all to avoid breaking if new ones are added).
        qg_1 = self.direct.get_quantum_graph("step5", "attempt1")
        qg_2 = self.direct.get_quantum_graph("step5", "attempt2")
        tasks_with_quanta_1 = {q.taskDef.label for q in qg_1}
        tasks_with_quanta_2 = {q.taskDef.label for q in qg_2}
        self.assertIn(get_mock_name("consolidateForcedSourceTable"), tasks_with_quanta_1)
        self.assertIn(get_mock_name("consolidateForcedSourceTable"), tasks_with_quanta_2)
        self.assertIn(get_mock_name("transformForcedSourceTable"), tasks_with_quanta_1)
        self.assertNotIn(get_mock_name("transformForcedSourceTable"), tasks_with_quanta_2)

    def test_step8_rescue_qbb(self) -> None:
        self.check_step5_rescue(self.qbb)

    def check_step8_qpg(self, helper: OutputRepoTests) -> None:
        """Check that the fail-and-recover attempts in step 8 are properly
        diagnosed using the `QuantumProvenanceGraph`.
        """
        # Make the quantum provenance graph for the first attempt
        qg_1 = helper.get_quantum_graph("step8", "attempt1")
        qpg1 = QuantumProvenanceGraph()
        qpg1.assemble_quantum_provenance_graph(helper.butler, qg_1, collections=["HSC/runs/RC2/step8-attempt1"], where="instrument='HSC'")
        qg_1_sum = qpg1.to_summary(helper.butler)

        # Check that expected, wonky and not attempted do not occur throughout
        # tasks:
        for label, task_summary in qg_1_sum.tasks.items():
            self.assertEqual(task_summary.n_unknown, 0)
            self.assertEqual(task_summary.n_wonky, 0)
            self.assertListEqual(task_summary.wonky_quanta, [])
            self.assertListEqual(task_summary.recovered_quanta, [])
            match label:
                # Check that the failure was documented in expected ways:
                case "_mock_analyzeObjectTableCore":
                    self.assertEqual(task_summary.n_expected, 1)
                    self.assertEqual(task_summary.n_failed, 1)
                    self.assertEqual(task_summary.n_successful, 0)
                    self.assertEqual(
                        task_summary.failed_quanta,
                        [
                            UnsuccessfulQuantumSummary(
                                data_id={"skymap": "ci_mw", "tract": 0},
                                runs={"HSC/runs/RC2/step8-attempt1": "failed"},
                                messages=[
                                    "Execution of task '_mock_analyzeObjectTableCore' on quantum {skymap: "
                                    "'ci_mw', tract: 0} failed. Exception ValueError: Simulated failure: "
                                    "task=_mock_analyzeObjectTableCore dataId={skymap: 'ci_mw', tract: 0}"
                                ],
                            )
                        ],
                    )
                    self.assertEqual(task_summary.n_blocked, 0)
                case _:
                    # If it's not the failed task, there should be no failures
                    self.assertEqual(task_summary.n_failed, 0)
                    self.assertListEqual(task_summary.failed_quanta, [])
                    # We also shouldn't have had anything to recover
                    self.assertListEqual(task_summary.recovered_quanta, [])
                    # The next few if's are making sure we have the same
                    # number of expected and successful quanta. We could also
                    # just assert that n_expected == n_successful.
                    if label in ["_mock_analyzeMatchedPreVisitCore", "_mock_analyzeMatchedVisitCore"]:
                        self.assertEqual(task_summary.n_expected, 4)
                        self.assertEqual(task_summary.n_successful, 4)
                        self.assertEqual(task_summary.n_blocked, 0)
                    elif label == "_mock_plotPropertyMapTract":
                        self.assertEqual(task_summary.n_expected, 2)
                        self.assertEqual(task_summary.n_successful, 2)
                        self.assertEqual(task_summary.n_blocked, 0)
                    elif label in [
                        "_mock_makeMetricTableObjectTableCore",
                        "_mock_objectTableCoreWholeSkyPlot",
                    ]:
                        self.assertEqual(task_summary.n_blocked, 1)
                        self.assertEqual(task_summary.n_successful, 0)
                    elif label == "_mock_analyzeAmpOffsetMetadata":
                        self.assertEqual(task_summary.n_expected, 60)
                        self.assertEqual(task_summary.n_successful, 60)
                        self.assertEqual(task_summary.n_blocked, 0)
                    else:
                        self.assertEqual(
                            task_summary.n_expected, 1, f"{label} had {task_summary.n_expected} tasks."
                        )
                        self.assertEqual(
                            task_summary.n_successful,
                            1,
                            f"{label} had {task_summary.n_successful} successful tasks.",
                        )
                        self.assertEqual(
                            task_summary.n_blocked, 0, f"{label} had {task_summary.n_blocked} blocked tasks."
                        )
        # Check on datasets
        for dataset_type_summary in qg_1_sum.datasets.values():
            # We shouldn't run into predicted only, unpublished or cursed.
            # Unpublished suggests that the dataset exists but is not included
            # in the final collection; cursed suggests that the dataset is
            # published but unsuccessful.
            self.assertEqual(dataset_type_summary.n_predicted_only, 0)
            self.assertEqual(dataset_type_summary.n_shadowed, 0)
            self.assertEqual(dataset_type_summary.n_cursed, 0)
            self.assertListEqual(dataset_type_summary.cursed_datasets, [])
            match dataset_type_summary.producer:
                # Check that the failure was documented in expected ways:
                case "_mock_analyzeObjectTableCore":
                    self.assertEqual(dataset_type_summary.n_visible, 0)
                    self.assertEqual(dataset_type_summary.n_expected, 1)
                    self.assertEqual(dataset_type_summary.n_unsuccessful, 1)
                    self.assertListEqual(
                        dataset_type_summary.unsuccessful_datasets,
                        [{"skymap": "ci_mw", "tract": 0}],
                    )
                case label if label in [
                    "_mock_makeMetricTableObjectTableCore",
                    "_mock_objectTableCoreWholeSkyPlot",
                ]:
                    self.assertEqual(dataset_type_summary.n_unsuccessful, 1)
                # These are the non-failed tasks:
                case _:
                    self.assertEqual(dataset_type_summary.n_unsuccessful, 0)
                    self.assertListEqual(dataset_type_summary.unsuccessful_datasets, [])
                    if (
                        dataset_type_summary.producer == "_mock_analyzeMatchedPreVisitCore"
                        or dataset_type_summary.producer == "_mock_analyzeMatchedVisitCore"
                    ):
                        self.assertEqual(dataset_type_summary.n_visible, 4)
                        self.assertEqual(dataset_type_summary.n_expected, 4)
                    elif dataset_type_summary.producer == "_mock_plotPropertyMapTract":
                        self.assertEqual(dataset_type_summary.n_visible, 2)
                        self.assertEqual(dataset_type_summary.n_expected, 2)
                    elif dataset_type_summary.producer == "_mock_analyzeAmpOffsetMetadata":
                        self.assertEqual(dataset_type_summary.n_visible, 60)
                        self.assertEqual(dataset_type_summary.n_expected, 60)
                    else:
                        self.assertEqual(dataset_type_summary.n_visible, 1)
                        self.assertEqual(dataset_type_summary.n_expected, 1)

        # Now examine the quantum provenance graph after the recovery attempt
        # has been made.
        # Make the quantum provenance graph for the first attempt
        qg_2 = helper.get_quantum_graph("step8", "attempt2")

        # Before we get into that, let's see if we correctly label a successful
        # task whose data products do not make it into the output collection
        # given as shadowed.

        qpg_shadowed = QuantumProvenanceGraph()
        qpg_shadowed.assemble_quantum_provenance_graph(helper.butler, [qg_1, qg_2], collections=["HSC/runs/RC2/step8-attempt1"], where="instrument='HSC'")
        qpg_shadowed_sum = qpg_shadowed.to_summary(helper.butler)

        for dataset_type_name, dataset_type_summary in qpg_shadowed_sum.datasets.items():
            if dataset_type_summary.producer == "_mock_analyzeObjectTableCore":
                if dataset_type_name == "_mock_analyzeObjectTableCore_log":
                    continue
                else:
                    self.assertEqual(dataset_type_summary.n_visible, 0)
                    self.assertEqual(dataset_type_summary.n_shadowed, 1)
                    self.assertEqual(dataset_type_summary.n_expected, 1)
                    self.assertEqual(dataset_type_summary.n_cursed, 0)
                    self.assertEqual(dataset_type_summary.n_predicted_only, 0)
                    self.assertEqual(dataset_type_summary.n_unsuccessful, 0)

        # Now for verifying the recovery properly -- the graph below is made
        # as intended.
        qpg2 = QuantumProvenanceGraph()
        qpg2.assemble_quantum_provenance_graph(helper.butler, [qg_1, qg_2], collections=["HSC/runs/RC2/step8-attempt2", "HSC/runs/RC2/step8-attempt1"], where="instrument='HSC'")
        qg_2_sum = qpg2.to_summary(helper.butler)

        for label, task_summary in qg_2_sum.tasks.items():
            self.assertEqual(task_summary.n_unknown, 0)
            self.assertEqual(task_summary.n_wonky, 0)
            self.assertEqual(task_summary.n_blocked, 0)
            self.assertListEqual(task_summary.wonky_quanta, [])
            # There should be no failures, so we can say for all tasks:
            self.assertEqual(task_summary.n_successful, task_summary.n_expected)
            self.assertEqual(task_summary.n_failed, 0)
            self.assertListEqual(task_summary.failed_quanta, [])
            match label:
                # Check that the failure was recovered:
                case label if label in [
                    "_mock_analyzeObjectTableCore",
                    "_mock_makeMetricTableObjectTableCore",
                    "_mock_objectTableCoreWholeSkyPlot",
                ]:
                    self.assertEqual(task_summary.n_expected, 1)
                    self.assertEqual(task_summary.n_successful, 1)
                    self.assertEqual(task_summary.n_blocked, 0)
                    if label == "_mock_analyzeObjectTableCore":
                        self.assertEqual(
                            task_summary.recovered_quanta,
                            [{"skymap": "ci_mw", "tract": 0}],
                        )
                    if label in ["_mock_makeMetricTableObjectTableCore", "_mock_objectTableCoreWholeSkyPlot"]:
                        self.assertEqual(
                            task_summary.recovered_quanta,
                            [{"skymap": "ci_mw"}],
                        )
                case _:
                    self.assertListEqual(task_summary.recovered_quanta, [])

        # Check on datasets
        for dataset_type_summary in qg_2_sum.datasets.values():
            # Check that all the data products are present and successful for
            # all tasks.
            self.assertEqual(dataset_type_summary.n_predicted_only, 0)
            self.assertEqual(dataset_type_summary.n_cursed, 0)
            self.assertListEqual(dataset_type_summary.cursed_datasets, [])
            self.assertEqual(dataset_type_summary.n_unsuccessful, 0)
            self.assertListEqual(dataset_type_summary.unsuccessful_datasets, [])
            self.assertEqual(dataset_type_summary.n_shadowed, 0)
            self.assertEqual(dataset_type_summary.n_visible, dataset_type_summary.n_expected)

    def test_step8_quantum_provenance_graph_qbb(self) -> None:
        self.check_step8_qpg(self.qbb)

    def test_fgcm_refcats(self) -> None:
        """Test that FGCM does not get refcats that don't overlap any of its
        inputs or outputs, despite not having a spatial data ID.
        """
        fgcm_reference_stars: MockDataset = self.qbb.butler.get(
            get_mock_name("fgcm_reference_stars"), instrument="HSC"
        )
        htm7_indices = {
            input.data_id["htm7"] for input in fgcm_reference_stars.quantum.inputs["ref_cat"]  # type: ignore
        }
        self.assertNotIn(231819, htm7_indices)
        self.assertIn(231865, htm7_indices)

    def test_partial_outputs(self) -> None:
        """Test that downstream tasks are run or not as appropriate when
        partial output errors are raised.
        """
        no_raise_direct = OutputRepoTests("RC2", "test-no-raise-partial-outputs-direct", {})
        no_raise_qbb = OutputRepoTests("RC2", "test-no-raise-partial-outputs-qbb", {})
        data_id = dict(instrument="HSC", detector=57)
        for helper in (no_raise_direct, no_raise_qbb):
            # When we don't raise, the ISR quantum that raised should have
            # metadata and logs written, and so should downstream tasks that
            # end up raising NoWorkFound.
            self.assertTrue(helper.butler.exists(get_mock_name("isr_metadata"), data_id, exposure=95104))
            self.assertTrue(helper.butler.exists(get_mock_name("isr_log"), data_id, exposure=95104))
            self.assertTrue(
                helper.butler.exists(get_mock_name("characterizeImage_metadata"), data_id, visit=95104)
            )
            self.assertTrue(
                helper.butler.exists(get_mock_name("characterizeImage_log"), data_id, visit=95104)
            )
        raise_direct = OutputRepoTests("RC2", "test-raise-partial-outputs-direct", {})
        raise_qbb = OutputRepoTests("RC2", "test-raise-partial-outputs-qbb", {})
        for helper in (raise_direct, raise_qbb):
            # When we do raise, the ISR quantum that raised should not have
            # metadata written, but it should have logs, and downstream tasks
            # should not have either, because they are never run.
            self.assertFalse(helper.butler.exists(get_mock_name("isr_metadata"), data_id, exposure=95104))
            self.assertTrue(helper.butler.exists(get_mock_name("isr_log"), data_id, exposure=95104))
            self.assertFalse(
                helper.butler.exists(get_mock_name("characterizeImage_metadata"), data_id, visit=95104)
            )
            self.assertFalse(
                helper.butler.exists(get_mock_name("characterizeImage_log"), data_id, visit=95104)
            )


if __name__ == "__main__":
    unittest.main()

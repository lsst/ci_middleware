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


if __name__ == "__main__":
    unittest.main()

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

import os
import tempfile
import unittest

from lsst.daf.butler import Butler, CollectionType, DataCoordinate, DatasetRef, DatasetType
from lsst.geom import Box2I, Point2I
from lsst.pipe.base.tests.mocks import MockDataset, MockStorageClass, get_mock_name, get_original_name

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class MockStorageClassTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.maxDiff = None

    def test_exposure_mock(self) -> None:
        """Test a mock of Exposure, which has both regular components and
        derived components and an inheritance relationship with ExposureF that
        allows for conversion tests.
        """
        with tempfile.TemporaryDirectory(dir=TESTDIR, ignore_cleanup_errors=True) as root:
            # Register some mock storage classes for the first time.
            for original_name in ["Exposure", "ExposureF", "ArrowTable", "ArrowAstropy"]:
                MockStorageClass.get_or_register_mock(original_name)
            # Instantiate a butler instance.  Note that we're getting the
            # storage class definitions via the in-memory StorageClassFactory
            # singleton, but the formatter entries for those storage classes
            # from the new config file.
            butler = Butler(Butler.makeRepo(root), writeable=True)
            # Proceed with the rest of the tests.
            storage_class = MockStorageClass.get_or_register_mock("Exposure")
            self.assertNotEqual("Exposure", storage_class.name)
            self.assertEqual("Exposure", get_original_name(storage_class.name))
            self.assertEqual("Exposure", storage_class.original.name)
            self.assertEqual(storage_class.components["wcs"], MockStorageClass.get_or_register_mock("Wcs"))
            self.assertEqual(
                storage_class.derivedComponents["bbox"], MockStorageClass.get_or_register_mock("Box2I")
            )
            self.assertTrue(storage_class.can_convert(MockStorageClass.get_or_register_mock("ExposureF")))
            dataset_type = DatasetType(
                "mock_exposure",
                dimensions=[],
                storageClass=storage_class,
                universe=butler.dimensions,
            )
            butler.registry.registerDatasetType(dataset_type)
            run = "test_exposure_mock"
            butler.registry.registerCollection(run, CollectionType.RUN)
            dataset_ref = DatasetRef(
                dataset_type,
                DataCoordinate.makeEmpty(butler.dimensions),
                run=run,
            )
            # Make a dataset with this storage class and put it.
            in_memory_dataset = MockDataset(ref=dataset_ref.to_simple())
            butler.put(in_memory_dataset, dataset_ref, run=run)
            # Get the original dataset back.
            got_direct: MockDataset = butler.get(dataset_ref)
            self.assertIsInstance(got_direct, MockDataset)
            self.assertEqual(got_direct, in_memory_dataset)
            # Get with parameters.
            bbox = Box2I(Point2I(1, 2), Point2I(5, 4))
            got_parameterized: MockDataset = butler.get(dataset_ref, parameters={"bbox": bbox})
            self.assertIsInstance(got_parameterized, MockDataset)
            self.assertEqual(got_parameterized.ref, in_memory_dataset.ref)
            self.assertEqual(got_parameterized.parameters, {"bbox": repr(bbox)})
            # Get a regular component.
            got_wcs: MockDataset = butler.get(dataset_ref.makeComponentRef("wcs"))
            self.assertIsInstance(got_wcs, MockDataset)
            self.assertEqual(got_wcs.parent, in_memory_dataset)
            self.assertEqual(got_wcs.storage_class, get_mock_name("Wcs"))
            self.assertEqual(got_wcs.dataset_type.name, "mock_exposure.wcs")
            # Get a derived component.
            got_bbox: MockDataset = butler.get(dataset_ref.makeComponentRef("bbox"))
            self.assertIsInstance(got_bbox, MockDataset)
            self.assertEqual(got_bbox.parent, in_memory_dataset)
            self.assertEqual(got_bbox.storage_class, get_mock_name("Box2I"))
            self.assertEqual(got_bbox.dataset_type.name, "mock_exposure.bbox")
            # Put a mock of ExposureF to a dataset type with a mock of
            # Exposure. This should convert on put due the inherintance
            # relationship.
            derived_storage_class = MockStorageClass.get_or_register_mock("ExposureF")
            put_convert_dataset_type = DatasetType(
                "mock_exposure_put_convert",
                dimensions=[],
                storageClass=storage_class,
                universe=butler.dimensions,
            )
            butler.registry.registerDatasetType(put_convert_dataset_type)
            put_convert_dataset_ref = DatasetRef(
                put_convert_dataset_type,
                DataCoordinate.makeEmpty(butler.dimensions),
                run=run,
            )
            put_convert_in = MockDataset(
                ref=put_convert_dataset_ref.overrideStorageClass(derived_storage_class).to_simple()
            )
            butler.put(put_convert_in, put_convert_dataset_ref)
            put_convert_out: MockDataset = butler.get(put_convert_dataset_ref)
            self.assertIsInstance(put_convert_out, MockDataset)
            self.assertEqual(put_convert_out.converted_from, put_convert_in)
            self.assertEqual(put_convert_out.storage_class, storage_class.name)
            # Trying to get that dataset back as (mock) ExposureF should fail,
            # since the butler now sees it as just (mock) Exposure and we don't
            # cast down the inheritance tree, only up.
            with self.assertRaises(ValueError):
                butler.get(put_convert_dataset_ref.overrideStorageClass(derived_storage_class))
            # Make dataset type for mock ExposureF.  We should only be able to
            # put mock ExposureF datasets to that, not mock Exposure, but we
            # can get as either.
            derived_dataset_type = DatasetType(
                "mock_exposure_derived",
                dimensions=[],
                storageClass=derived_storage_class,
                universe=butler.dimensions,
            )
            butler.registry.registerDatasetType(derived_dataset_type)
            derived_dataset_ref = DatasetRef(
                derived_dataset_type,
                DataCoordinate.makeEmpty(butler.dimensions),
                run=run,
            )
            # It's a bit unfortunate that failure-to-convert is a ValueError on
            # put but a TypeError on get.
            with self.assertRaises(TypeError):
                butler.put(
                    MockDataset(ref=derived_dataset_ref.overrideStorageClass(storage_class).to_simple()),
                    derived_dataset_ref,
                )
            derived_in = MockDataset(ref=derived_dataset_ref.to_simple())
            butler.put(derived_in, derived_dataset_ref)
            got_converted: MockDataset = butler.get(derived_dataset_ref.overrideStorageClass(storage_class))
            self.assertIsInstance(got_converted, MockDataset)
            self.assertEqual(got_converted.converted_from, derived_in)
            self.assertEqual(got_converted.storage_class, storage_class.name)


if __name__ == "__main__":
    unittest.main()

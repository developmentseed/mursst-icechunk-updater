# Import business logic (not Lambda handler)
import pytest
from src.updater import MursstUpdater, combine_attrs
from src.lambda_function import get_store_url
import numpy as np
import xarray as xr
import icechunk as ic


@pytest.fixture()
def updater_instance(tmp_path):
    """Create a MursstUpdater instance for testing."""
    return MursstUpdater(str(tmp_path / "base_temp_store"))


class TestMursstUpdater:
    """Test suite for MursstUpdater business logic."""

    def test_updater_initialization(self, updater_instance):
        """Test that updater initializes correctly."""
        assert isinstance(updater_instance, MursstUpdater)
        assert updater_instance.branchname.startswith("add_time_")

    def test_persistent_virt_container_config(self, tmp_path, updater_instance):
        """Making sure that the chunk containers are persisted to storage"""
        updater_instance.setup_repo()
        ## first assert that we have virtual chunk containers in class attribute repo
        assert (
            "s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/"
            in updater_instance.repo.config.virtual_chunk_containers.keys()
        )
        # reopen the repo with vanilla icechunk and confirm that this info is persisted into storage
        repo = ic.Repository.open(
            storage=updater_instance.get_icechunk_storage(updater_instance.store_target)
        )
        assert (
            "s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/"
            in repo.config.virtual_chunk_containers.keys()
        )


class TestUtilityFunctions:
    """Test standalone utility functions."""

    def test_get_store_url(self):
        """Test store URL building."""
        result = get_store_url("s3://bucket/prefix", "my_store")
        expected = "s3://bucket/prefix/my_store/"
        assert result == expected

    def test_combine_attrs_various_types(self):
        """Test combine_attrs with various data types including those that caused issues"""

        # Create test datasets with various attribute types
        attrs1 = {
            "title": "Test Dataset 1",  # string
            "version": 1,  # int
            "resolution": 0.1,  # float
            "lat_resolution": np.float32(0.01),  # numpy scalar
            "sensors": ["MODIS", "AVHRR"],  # list
            "flag_masks": np.array([1, 2, 4, 8, 16], dtype=np.int8),  # numpy array
            "start_time": "2025-01-01T00:00:00Z",  # string (time-like)
            "stop_time": "2025-01-01T23:59:59Z",  # string (time-like)
            "date_created": "2025-01-01",  # should be dropped
            "comment": "File 1 specific comment",  # should be dropped
        }

        attrs2 = {
            "title": "Test Dataset 1",  # same string
            "version": 1,  # same int
            "resolution": 0.1,  # same float
            "lat_resolution": np.float32(0.01),  # same numpy scalar
            "sensors": ["MODIS", "AVHRR"],  # same list
            "flag_masks": np.array([1, 2, 4, 8, 16], dtype=np.int8),  # same numpy array
            "start_time": "2025-01-02T00:00:00Z",  # different time (should take min)
            "stop_time": "2025-01-02T23:59:59Z",  # different time (should take max)
            "date_created": "2025-01-02",  # should be dropped
            "comment": "File 2 specific comment",  # should be dropped
        }

        # Create xarray datasets
        data = np.random.random((10, 10))
        ds1 = xr.Dataset(
            data_vars={"temperature": (["x", "y"], data)},
            coords={"x": range(10), "y": range(10)},
            attrs=attrs1,
        )

        ds2 = xr.Dataset(
            data_vars={"temperature": (["x", "y"], data)},
            coords={"x": range(10), "y": range(10)},
            attrs=attrs2,
        )

        # Test the combine_attrs function
        result = xr.concat([ds1, ds2], dim="time", combine_attrs=combine_attrs)

        # Verify results
        assert result.attrs["title"] == "Test Dataset 1"  # same values preserved
        assert result.attrs["version"] == 1
        assert result.attrs["resolution"] == 0.1
        assert result.attrs["lat_resolution"] == np.float32(0.01)
        assert result.attrs["sensors"] == ["MODIS", "AVHRR"]
        np.testing.assert_array_equal(
            result.attrs["flag_masks"], np.array([1, 2, 4, 8, 16], dtype=np.int8)
        )

        # Time attributes should be combined according to rules
        assert result.attrs["start_time"] == "2025-01-01T00:00:00Z"  # min
        assert result.attrs["stop_time"] == "2025-01-02T23:59:59Z"  # max

        # Dropped attributes should not be present
        assert "date_created" not in result
        assert "comment" not in result

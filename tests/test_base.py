# Import business logic (not Lambda handler)
import pytest
from src.updater import MursstUpdater
from src.lambda_function import get_store_url


@pytest.fixture()
def updater_instance():
    """Create a MursstUpdater instance for testing."""
    return MursstUpdater()


class TestMursstUpdater:
    """Test suite for MursstUpdater business logic."""

    def test_updater_initialization(self, updater_instance):
        """Test that updater initializes correctly."""
        assert isinstance(updater_instance, MursstUpdater)
        assert updater_instance.branchname.startswith("add_time_")

    def test_persistent_virt_container_config(self, tmp_path, updater_instance):
        """Making sure that the chunk containers are persisted to storage"""
        updater_instance.create_icechunk_repo(str(tmp_path))
        repo = updater_instance.open_icechunk_repo(str(tmp_path))
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

import pytest
from datetime import datetime, timedelta, timezone

from src.updater import MursstUpdater


@pytest.fixture(scope="module")
def full_vdataset(tmp_path_factory):
    """
    Get a single dataset that has enough missing timesteps to serve for all the append test cases below.
    These test here are written to ensure that past appends will again succeed, and not to detect problems with
    recently released data, so we dont care that these granules are a while back
    """
    # TODO: This is a bit dumb. I changed the logic to the init will always create an icechunk store
    # but I want to factor the vdataset creation out because it takes long. So for now Ill just create
    # the icechunk store here but still just return the dataset?
    tmp_path = tmp_path_factory.mktemp("test_data")
    updater = MursstUpdater(str(tmp_path / "temp_store_not_used"))

    # Use a generous time range and then cull updates to the earliest 2 available
    start_date = (
        datetime.now(timezone.utc) - timedelta(days=8)
    ).date().isoformat() + " 21:00:00"
    end_date = (
        datetime.now(timezone.utc) - timedelta(days=5)
    ).date().isoformat() + " 21:00:00"
    new_granules = updater.find_granules(start_date, end_date, limit_granules=2)

    vds = updater.dataset_from_granules(
        new_granules,
        virtual=True,
        parallel="lithops",
    )
    return vds


@pytest.fixture()
def temp_updater_instance(tmp_path, full_vdataset):
    """Create a temporary icechunk store that is missing the latest granules"""
    updater = MursstUpdater(str(tmp_path / "temp_store"))

    print(f"Full vds in fixture {full_vdataset}")
    updater.setup_repo()

    # Write initial data to store
    session = updater.repo.writable_session("main")
    full_vdataset.vz.to_icechunk(session.store)
    session.commit("Write Test Data")

    return updater


class TestIntegrationTests:
    """Integration tests using temporary icechunk stores."""

    @pytest.mark.parametrize("days_to_append", [1, 2])
    def test_append_data(self, temp_updater_instance, days_to_append):
        """Test appending new data to existing store."""
        ds_old = temp_updater_instance.open_xr_dataset_from_branch("main")
        initial_time_length = len(ds_old.time)
        print(f"OLD DATASET for comparison {ds_old}")

        # Run the update
        result = temp_updater_instance.update_icechunk_store(
            run_tests=True,  # Skip tests for faster execution
            dry_run=False,
            limit_granules=days_to_append,
            parallel=False,
        )

        # Verify results
        ds_new = temp_updater_instance.open_xr_dataset_from_branch("main")
        assert len(ds_new.time) == initial_time_length + days_to_append
        assert "Successfully updated store" in result

    def test_nothing_to_append(self, temp_updater_instance):
        """Test behavior when there is no data to append"""
        with pytest.raises(ValueError) as exc_info:
            temp_updater_instance.update_icechunk_store(
                run_tests=True,
                dry_run=False,
                limit_granules=0,
                parallel=False,
            )

        assert "No new data granules available" in str(exc_info.value)

    def test_dry_run_mode(self, temp_updater_instance):
        """Test dry run functionality."""
        ds_old = temp_updater_instance.open_xr_dataset_from_branch("main")
        initial_time_length = len(ds_old.time)

        # Run in dry run mode
        result = temp_updater_instance.update_icechunk_store(
            run_tests=True,
            dry_run=True,  # This should prevent merging to main
            limit_granules=1,
            parallel=False,
        )

        # Verify main branch is unchanged
        ds_after = temp_updater_instance.open_xr_dataset_from_branch("main")
        assert len(ds_after.time) == initial_time_length  # No change to main
        assert "Dry run completed" in result

        # Verify branch was created
        branches = temp_updater_instance.repo.list_branches()
        test_branches = [name for name in branches if name.startswith("add_time_")]
        assert len(test_branches) > 0  # Branch should exist

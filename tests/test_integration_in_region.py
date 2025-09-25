import pytest
from datetime import datetime, timedelta, timezone

from src.updater import MursstUpdater


@pytest.fixture(scope="module")
def full_vdataset():
    """
    Get a single dataset that has enough missing timesteps to serve for all the append test cases below.
    These test here are written to ensure that past appends will again succeed, and not to detect problems with
    recently released data, so we dont care that these granules are a while back
    """
    updater = MursstUpdater()

    # Use a generous time range and then cull to the latest 2 available
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
def temp_icechunk_store(tmp_path, full_vdataset):
    """Create a temporary icechunk store that is missing the latest granules"""
    updater = MursstUpdater()
    path = str(tmp_path / "temp_store")

    # Create empty repo
    updater.create_icechunk_repo(path)

    # Open the repo
    repo = updater.open_icechunk_repo(path)
    print(f"Full vds in fixture {full_vdataset}")

    # Write initial data to store
    session = repo.writable_session("main")
    full_vdataset.vz.to_icechunk(session.store)
    session.commit("Write Test Data")

    return path


class TestIntegrationTests:
    """Integration tests using temporary icechunk stores."""

    @pytest.mark.parametrize("days_to_append", [1, 2])
    def test_append_data(self, temp_icechunk_store, days_to_append):
        """Test appending new data to existing store."""
        # Get initial state
        updater_instance = MursstUpdater()
        repo = updater_instance.open_icechunk_repo(temp_icechunk_store)
        ds_old = updater_instance.open_xr_dataset_from_branch(repo, "main")
        initial_time_length = len(ds_old.time)
        print(f"OLD DATASET for comparison {ds_old}")

        # Run the update
        result = updater_instance.update_icechunk_store(
            store_target=temp_icechunk_store,
            run_tests=True,  # Skip tests for faster execution
            dry_run=False,
            limit_granules=days_to_append,
            parallel=False,
        )

        # Verify results
        ds_new = updater_instance.open_xr_dataset_from_branch(repo, "main")
        assert len(ds_new.time) == initial_time_length + days_to_append
        assert "Successfully updated store" in result

    def test_nothing_to_append(self, temp_icechunk_store):
        """Test behavior when there is no data to append"""
        updater_instance = MursstUpdater()
        with pytest.raises(ValueError) as exc_info:
            updater_instance.update_icechunk_store(
                store_target=temp_icechunk_store,
                run_tests=True,
                dry_run=False,
                limit_granules=0,
                parallel=False,
            )

        assert "No new data granules available" in str(exc_info.value)

    def test_dry_run_mode(self, temp_icechunk_store):
        """Test dry run functionality."""
        updater_instance = MursstUpdater()
        # Get initial state
        repo = updater_instance.open_icechunk_repo(temp_icechunk_store)
        ds_old = updater_instance.open_xr_dataset_from_branch(repo, "main")
        initial_time_length = len(ds_old.time)

        # Run in dry run mode
        result = updater_instance.update_icechunk_store(
            store_target=temp_icechunk_store,
            run_tests=True,
            dry_run=True,  # This should prevent merging to main
            limit_granules=1,
            parallel=False,
        )

        # Verify main branch is unchanged
        ds_after = updater_instance.open_xr_dataset_from_branch(repo, "main")
        assert len(ds_after.time) == initial_time_length  # No change to main
        assert "Dry run completed" in result

        # Verify branch was created
        branches = repo.list_branches()
        test_branches = [name for name in branches if name.startswith("add_time_")]
        assert len(test_branches) > 0  # Branch should exist

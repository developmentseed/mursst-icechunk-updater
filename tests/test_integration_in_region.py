import pytest
from cdk.aws_lambda.lambda_function import (
    create_icechunk_repo,
    open_icechunk_repo,
    dataset_from_search,
    open_xr_dataset_from_branch,
    write_to_icechunk,
)
from datetime import datetime, timedelta, timezone


@pytest.fixture(scope="module")
def full_vdataset():
    """Get a single dataset that has enough missing timesteps to serve for all the append test cases below.
    These test here are written to ensure that past appends will again succeed, and not to detect problems with
    recently released data, so we dont care that these granules are a while back
    """

    # Ill define a generous time range and then cull to the latest 5 available
    start_date = (
        datetime.now(timezone.utc) - timedelta(days=6)
    ).date().isoformat() + " 21:00:00"
    end_date = (
        datetime.now(timezone.utc) - timedelta(days=3)
    ).date().isoformat() + " 21:00:00"

    vds = dataset_from_search(
        start_date, end_date, virtual=True, parallel="lithops", limit_granules=2
    )

    return vds


@pytest.fixture()
def temp_icechunk_store(tmp_path, full_vdataset):
    """Create a temporary icechunk store that is missing the 3 latest granules"""
    path = str(tmp_path / "temp_store")
    # create empty repo
    create_icechunk_repo(path)
    # open the repo
    repo = open_icechunk_repo(path)

    print(f"Full vds in fixture {full_vdataset}")

    # crop dataset before saving
    vds = full_vdataset

    # Get data and combine into virtual dataset

    # vds = dataset_from_search(start_date, end_date, virtual=True, parallel=False)
    # # TODO. The lithops exec fails when using pyteset-xdist. I think this is due to the fact that they all share the same temp dict?
    # # TODO: Maybe it is possible to modify virtualizarr to pass a custom dir? This seems like a bunch of work.
    # # write to store
    session = repo.writable_session("main")
    vds.vz.to_icechunk(session.store)
    session.commit("Write Test Data")
    return path


@pytest.mark.parametrize("days_to_append", [1, 2])
def test_append(temp_icechunk_store, days_to_append):
    repo = open_icechunk_repo(temp_icechunk_store)
    ds_old = open_xr_dataset_from_branch(repo, "main")
    print(f"OLD DATASET for comparison {ds_old}")

    # now call the lambda wrapper function on this store
    result = write_to_icechunk(
        temp_icechunk_store, limit_granules=days_to_append, parallel=False
    )

    # Load the resulting dataset and test
    ds = open_xr_dataset_from_branch(repo, "main")
    assert len(ds.time) == days_to_append + 2
    assert result == "Success"


# def test_nothing_to_append(temp_icechunk_store):
def test_nothing_to_append(temp_icechunk_store):
    """Test behavior when there is no data to append"""
    # now call the lambda wrapper function on this store
    result = write_to_icechunk(temp_icechunk_store, limit_granules=0, parallel=False)
    assert result == "No new data granules available"

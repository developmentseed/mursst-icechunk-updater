# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///

# run with `uv run python scripts/rebuild_store_with_vz2_ic1.py`
# I do not really understand why the 'python' is necessary.

from lambda_function import (
    create_icechunk_repo,
    open_icechunk_repo,
    dataset_from_search,
)
# This script was used to rebuild the icecunk MUR store

# Get data and combine into virtual dataset
start_date = "2024-06-01 21:00:01"  # In my manual testing this was the earliest I could go without hitting: ValueError: Cannot concatenate arrays with inconsistent chunk shapes: (1, 1023, 2047) vs (1, 3600, 7200) .Requires ZEP003 (Variable-length Chunks).
end_date = "2025-07-29 21:00:00"

# store_url = "s3://nasa-veda-scratch/jbusecke/icechunk/mursst-testing/MUR_test_9" # take this one for testing, that wrote successfully!
store_url = "s3://nasa-veda-scratch/jbusecke/icechunk/mursst-testing/MUR_test_deployed"

# create a virtual dataset from search
vds = dataset_from_search(start_date, end_date, virtual=True)

# create empty repo and open it
create_icechunk_repo(store_url)
repo = open_icechunk_repo(store_url)

# write to store
session = repo.writable_session("main")
vds.vz.to_icechunk(session.store)
session.commit("Write Test Data")
print(f"Writing finished to {store_url}")

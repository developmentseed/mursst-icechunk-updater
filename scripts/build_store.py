# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///

# run with `uv run --env-file .env.<STAGE> python scripts/rebuild_store.py`, see readme for more information.

from src.updater import MursstUpdater
from src.lambda_function import RuntimeSettings, get_store_url
from datetime import datetime, timezone

settings = RuntimeSettings()

store_url = get_store_url(settings.icechunk_direct_prefix, settings.store_name)
print(f"Rebuilding Store in {store_url}")

updater = MursstUpdater(store_url)

# Get data and combine into virtual dataset
start_date = "2024-06-01 21:00:01"  # In my manual testing this was the earliest I could go without hitting: ValueError: Cannot concatenate arrays with inconsistent chunk shapes: (1, 1023, 2047) vs (1, 3600, 7200) .Requires ZEP003 (Variable-length Chunks).
# end_date = "2024-09-10 21:00:00"
end_date = "2024-06-15 21:00:00"

# Search for granules
print("Finding new granules")
new_granules = updater.find_granules(start_date, end_date)

# create a virtual dataset from granules
print("Creating new virtual dataset")
vds = updater.dataset_from_granules(new_granules, virtual=True, parallel="lithops")

print(f"{vds=}")

# write to store
print("Writing to store")
updater.setup_repo()
rebuild_branch = f"rebuild_store_{datetime.now(timezone.utc).isoformat()}"


# rewind to init snapshot
init_snapshot = list(updater.repo.ancestry(branch="main"))[-1].id
updater.repo.create_branch(rebuild_branch, snapshot_id=init_snapshot)

# start a branch at the repo init snapshot (this should work both for fresh repos as well as already populated ones. !!!This will eliminate all snapshots of earlier data!
session = updater.repo.writable_session(rebuild_branch)
vds.vz.to_icechunk(session.store)
session.commit(
    f"rebuild store from scratch on {datetime.now(timezone.utc).isoformat()}"
)
updater.repo.reset_branch("main", updater.repo.lookup_branch(rebuild_branch))
print(f"Writing finished to {store_url}")

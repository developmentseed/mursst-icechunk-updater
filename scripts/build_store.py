# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///

# run with `uv run --env-file .env.<STAGE> python scripts/rebuild_store.py`, see readme for more information.

from src.updater import MursstUpdater
from src.lambda_function import RuntimeSettings, get_store_url

settings = RuntimeSettings()

store_url = get_store_url(settings.icechunk_direct_prefix, settings.store_name)
print(f"Rebuilding Store in {store_url}")

updater = MursstUpdater(store_url)

# Get data and combine into virtual dataset
start_date = "2024-06-01 21:00:01"  # In my manual testing this was the earliest I could go without hitting: ValueError: Cannot concatenate arrays with inconsistent chunk shapes: (1, 1023, 2047) vs (1, 3600, 7200) .Requires ZEP003 (Variable-length Chunks).
end_date = "2025-09-10 21:00:00"

# Search for granules
print("Finding new granules")
new_granules = updater.find_granules(start_date, end_date)

# create a virtual dataset from granules
print("Creating new virtual dataset")
vds = updater.dataset_from_granules(new_granules, virtual=True, parallel="lithops")

print(f"{vds=}")

# write to store
print("Writing to store")
session = updater.repo.writable_session("main")
vds.vz.to_icechunk(session.store)
session.commit("First Batch Write")
print(f"Writing finished to {store_url}")

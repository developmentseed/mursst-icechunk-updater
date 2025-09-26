"""
MURSST Icechunk Updater - Pure Business Logic

This module contains all the core business logic for updating the MURSST icechunk store.
It has no AWS Lambda dependencies and can be run locally or in any environment.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

import logging
import earthaccess
from earthaccess import DataGranule
import json
import icechunk
import boto3
import os
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional, List
import xarray as xr
from urllib.parse import urlparse, urlunparse
from virtualizarr import open_virtual_mfdataset
from virtualizarr.parsers import HDFParser
from virtualizarr.registry import ObjectStoreRegistry
from obstore.store import S3Store
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from icechunk import S3StaticCredentials

# Configure logging for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Constants
# TODO: this shold be determined dynamically in the future
EXAMPLE_TARGET_URL = "s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20250702090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-GLOB-v02.0-fv04.1.nc"
# TODO: I am dropping these because they are file specific info
# It would be interesting to see if we could expose them in the
# store attributes as a mapping to the original file, or even
# the chunks?
# TODO, how could I provide these as a kwarg to the combine function?
DROP_ATTRS = ["date_created", "comment", "source", "platform", "sensor"]


def combine_attrs(dicts, context):
    def make_hashable(value):
        """Convert value to hashable form for set operations"""
        if isinstance(value, list):
            return tuple(value)
        elif hasattr(value, "tolist"):  # Handle numpy arrays and scalars
            converted = value.tolist()
            # If tolist() returns a list, convert to tuple; otherwise return as-is
            if isinstance(converted, list):
                return tuple(converted)
            else:
                return converted
        elif hasattr(value, "__array__"):  # Handle other array-like objects
            import numpy as np

            converted = np.asarray(value).tolist()
            if isinstance(converted, list):
                return tuple(converted)
            else:
                return converted
        return value

    combined_attrs = {}
    # Get keys from first dict as reference
    all_keys = set(dicts[0].keys())
    # Check that every key exists in all dicts
    for i, d in enumerate(dicts[1:], 1):
        if set(d.keys()) != all_keys:
            missing = all_keys - set(d.keys())
            extra = set(d.keys()) - all_keys
            raise KeyError(f"Dict {i} key mismatch. Missing: {missing}, Extra: {extra}")

    drop_keys = set(DROP_ATTRS)
    combine_keys = all_keys - drop_keys
    #
    same_value_keys = []
    handled_multi_value_keys = []
    non_handled_multi_value_keys = []
    for key in combine_keys:
        values = [d[key] for d in dicts]
        unique_values = set(
            make_hashable(v) for v in values
        )  # Convert to hashable for set
        logger.debug(f"DEBUG: {key=} {unique_values=}")
        if len(unique_values) == 1:
            # All values are the same
            combined_attrs[key] = values[0]  # Use original value, not hashable version
            same_value_keys.append(key)
        else:
            if key == "start_time":
                combined_attrs[key] = min(values)
                handled_multi_value_keys.append(key)
            elif key == "stop_time":
                combined_attrs[key] = max(values)
                handled_multi_value_keys.append(key)
            elif key == "time_coverage_start":
                combined_attrs[key] = min(values)
                handled_multi_value_keys.append(key)
            elif key == "time_coverage_end":
                combined_attrs[key] = max(values)
                handled_multi_value_keys.append(key)
            else:
                non_handled_multi_value_keys.append(key)

            if len(non_handled_multi_value_keys) > 0:
                raise ValueError(
                    f"No instructions provided how to handle {non_handled_multi_value_keys=}"
                )
            logger.info(f"Constant keys: {same_value_keys}")
            logger.info(f"Manually combined keys: {handled_multi_value_keys}")
    return combined_attrs


class MursstUpdater:
    """
    Main class for updating MURSST data in icechunk store.

    This class encapsulates all the business logic and can be used
    independently of AWS Lambda.

    Parameters
    ----------
    store_target : str
        Target location for the icechunk store. Can be either an S3 URL
        (e.g., 's3://bucket/path') or a local filesystem path.
    collection_short_name : str, optional
        Short name identifier for the data collection to search and process.
        Default is "MUR-JPL-L4-GLOB-v4.1".
    drop_vars : list of str or None, optional
        List of variable names to drop from the dataset during processing.
        Default is ["dt_1km_data", "sst_anomaly"].

    Attributes
    ----------
    repo : icechunk.Repository
        The icechunk repository instance for data storage operations.
    store_target : str
        Target location for the icechunk store.
    collection_short_name : str
        Short name identifier for the data collection.
    drop_vars : list of str or None
        List of variable names to drop from datasets.
    branchname : str
        Auto-generated branch name based on current UTC timestamp.
    """

    repo: icechunk.Repository
    store_target: str
    collection_short_name: str
    drop_vars: List[None | str]

    def __init__(
        self,
        store_target: str,
        collection_short_name: str = "MUR-JPL-L4-GLOB-v4.1",
        drop_vars: List[None | str] = ["dt_1km_data", "sst_anomaly"],
    ):
        """Initialize the updater with current timestamp for branch naming."""
        self.branchname = f"add_time_{datetime.now(timezone.utc).isoformat()}"
        self.store_target = store_target
        self.collection_short_name = collection_short_name
        self.drop_vars = drop_vars

    def setup_repo(self):
        self.repo = self.open_or_create_icechunk_repo()

    def get_container_credentials(
        self, example_url: str
    ) -> Dict[str, icechunk.AnyCredential]:
        """Get container credentials for icechunk."""
        return icechunk.containers_credentials(
            {
                self.get_prefix_from_url(
                    example_url
                ): icechunk.s3_refreshable_credentials(
                    get_credentials=self.get_icechunk_creds
                )
            }
        )

    @staticmethod
    def obstore_and_registry_from_url(url: str) -> Tuple[S3Store, ObjectStoreRegistry]:
        """Create obstore and registry from URL."""
        logger.info(f"Setting up obstore and registry for url: {url}")
        parsed = urlparse(url)
        parsed_wo_path = parsed._replace(path="")
        bucket = parsed.netloc
        logger.debug(f"Using bucket: {bucket}")

        credentials_url = "https://archive.podaac.earthdata.nasa.gov/s3credentials"
        cp = NasaEarthdataCredentialProvider(credentials_url)
        logger.debug(f"Using credential provider: {cp}")
        store = S3Store(bucket=bucket, region="us-west-2", credential_provider=cp)
        logger.debug(f"Created S3Store: {store}")
        registry = ObjectStoreRegistry({parsed_wo_path.geturl(): store})
        logger.debug(f"Created ObjectStoreRegistry: {registry}")
        return store, registry

    def get_icechunk_creds(self, daac: str = None) -> S3StaticCredentials:
        """Get refreshable earthdata credentials for icechunk."""
        if daac is None:
            daac = "PODAAC"

        auth = earthaccess.login(strategy="environment")
        if not auth.authenticated:
            raise PermissionError("Could not authenticate using environment variables")
        creds = auth.get_s3_credentials(daac=daac)
        return S3StaticCredentials(
            access_key_id=creds["accessKeyId"],
            secret_access_key=creds["secretAccessKey"],
            expires_after=datetime.fromisoformat(creds["expiration"]),
            session_token=creds["sessionToken"],
        )

    @staticmethod
    def get_icechunk_storage(target: str) -> icechunk.Storage:
        """Get icechunk storage configuration."""
        if target.startswith("s3://"):
            logger.info("Defining icechunk storage for s3")
            target_parsed = urlparse(target)
            logger.info(f"{target_parsed}")
            storage = icechunk.s3_storage(
                bucket=target_parsed.netloc,
                prefix=target_parsed.path.lstrip("/"),
                from_env=True,
            )
        else:
            logger.info("Defining icechunk storage for local filesystem")
            storage = icechunk.local_filesystem_storage(path=target)
        return storage

    def open_or_create_icechunk_repo(self) -> None:
        """Open an existing icechunk repository."""
        logger.info("Opening icechunk repo")
        storage = self.get_icechunk_storage(self.store_target)
        logger.info(f"{storage=}")
        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(
                self.get_prefix_from_url(EXAMPLE_TARGET_URL),
                icechunk.s3_store(region="us-west-2"),
            )
        )
        repo = icechunk.Repository.open_or_create(
            config=config,
            storage=storage,
            authorize_virtual_chunk_access=self.get_container_credentials(
                EXAMPLE_TARGET_URL
            ),
        )
        return repo

    def get_prefix_from_url(self, url: str) -> str:
        """Extract prefix from URL for icechunk."""
        parsed = urlparse(url)
        path_without_file = os.path.dirname(parsed.path)
        new_parsed = parsed._replace(path=path_without_file)
        prefix = urlunparse(new_parsed) + "/"
        return prefix

    def search_valid_granules(self, start_date: str, end_date: str):
        """Search and filter granules to only include final processed versions"""
        logger.info(f"Searching for granules between {start_date} and {end_date}")
        granules = earthaccess.search_data(
            temporal=(start_date, end_date), short_name=self.collection_short_name
        )
        files = earthaccess.open(granules)

        def is_reprocessed(ds: xr.Dataset) -> bool:
            if "replaced nrt (1-day latency) version." in ds.attrs["history"]:
                return True
            else:
                return False

        final_processing_granules = [
            g for g, f in zip(granules, files) if is_reprocessed(xr.open_dataset(f))
        ]
        return final_processing_granules

    def find_granules(
        self, start_date: str, end_date: str, limit_granules: Optional[int] = None
    ) -> list[DataGranule]:
        """Find granules within date range."""
        logger.info(f"Searching for granules between {start_date} and {end_date}")
        logger.debug(f"{limit_granules=}")
        granule_results = self.search_valid_granules(start_date, end_date)

        if limit_granules is not None:
            logger.info(f"Limiting the number of granules to {limit_granules}")
            granule_results = granule_results[:limit_granules]

        if len(granule_results) == 0:
            raise ValueError("No new data granules available")
        else:
            logger.info(f"Number of valid granules found: {len(granule_results)}")
            return granule_results

    def dataset_from_granules(
        self,
        granule_results: list[DataGranule],
        virtual: bool = True,
        parallel: Optional[str] = "lithops",
        access: str = "direct",
    ) -> xr.Dataset:
        """Create dataset from granule search."""
        logger.info("Creating Dataset from Granules")
        in_region = True if access == "direct" else False
        data_urls = [
            g.data_links(access=access, in_region=in_region)[0] for g in granule_results
        ]

        store, registry = self.obstore_and_registry_from_url(EXAMPLE_TARGET_URL)
        parser = HDFParser()

        drop_vars = self.drop_vars

        def preprocess(ds: xr.Dataset) -> xr.Dataset:
            return ds.drop_vars(drop_vars, errors="ignore")

        common_kwargs = dict(
            preprocess=preprocess,
            parallel=parallel,
            combine_attrs=combine_attrs,
        )

        if virtual:
            logger.info("Returning virtual dataset")
            return open_virtual_mfdataset(
                data_urls, registry=registry, parser=parser, **common_kwargs
            )
        else:
            logger.info("Returning non-virtual dataset")
            fileset = earthaccess.open(data_urls, provider="POCLOUD")
            return xr.open_mfdataset(fileset, chunks={}, **common_kwargs)

    def get_timestep_from_ds(self, ds: xr.Dataset, nt: str) -> datetime:
        """Get timestep from dataset."""
        return ds.time.data[nt].astype("datetime64[ms]").astype(datetime)

    def open_xr_dataset_from_branch(self, branch: str):
        """Open xarray dataset from icechunk branch."""
        session = self.repo.readonly_session(branch=branch)
        ds = xr.open_zarr(session.store, consolidated=False)
        return ds

    @staticmethod
    def get_filenames_from_granules(granule_results: list[DataGranule]):
        paths = [g.data_links()[0] for g in granule_results]
        filenames = [os.path.basename(path) for path in paths]
        return filenames

    def get_filenames_from_virtual_chunks(self, branch):
        session = self.repo.readonly_session(branch)
        vchunks_locations = set(session.all_virtual_chunk_locations())
        filenames = [os.path.basename(path) for path in vchunks_locations]
        return filenames

    def test_new_data(self, ds_old, new_granules):
        logger.info("Starting comprehensive dataset validation tests")
        errors = []

        # Check that virtual references contain all files in query
        files_granules = self.get_filenames_from_granules(new_granules)
        files_branch = self.get_filenames_from_virtual_chunks(self.branchname)
        missing_files = []
        for file in files_granules:
            if file not in files_branch:
                missing_files.append(file)

        if len(missing_files) > 0:
            raise ValueError(
                f"Did not find all files from the granules on new branch. {missing_files=}"
            )

        # load dataset on current branch
        ds_new = self.open_xr_dataset_from_branch(self.branchname)
        logger.info(f"Dataset on {self.branchname}: {ds_new}")

        # Time continuity check
        logger.info("Checking time continuity...")
        dt_actual = ds_new.time.diff("time")
        dt_expected = ds_new.isel(time=slice(0, 1)).time.diff("time")
        time_continuity = (dt_actual == dt_expected).all().item()
        if not time_continuity:
            errors.append(
                "Time intervals are not consistent across the dataset. Expected uniform time steps but found irregular intervals."
            )

        # Append consistency with granules
        logger.info("Validating granule count consistency...")
        expected_new_timesteps = len(new_granules)
        actual_new_timesteps = len(ds_new.time) - len(ds_old.time)
        if actual_new_timesteps != expected_new_timesteps:
            errors.append(
                f"Granule count mismatch: expected {expected_new_timesteps} new time steps but found {actual_new_timesteps}. "
                f"Original dataset had {len(ds_old.time)} timesteps, new dataset has {len(ds_new.time)} timesteps."
            )

        # Check attributes that should remain equal
        logger.info("Checking attributes that should remain unchanged...")
        check_equal_attrs = ["start_time", "time_coverage_start"]
        changed_equal_attrs = []
        for attr in check_equal_attrs:
            if attr not in ds_old.attrs:
                errors.append(
                    f"Required attribute '{attr}' is missing from original dataset."
                )
            elif attr not in ds_new.attrs:
                errors.append(
                    f"Required attribute '{attr}' is missing from new dataset."
                )
            elif ds_new.attrs[attr] != ds_old.attrs[attr]:
                changed_equal_attrs.append(
                    f"'{attr}': '{ds_old.attrs[attr]}' → '{ds_new.attrs[attr]}'"
                )

        if changed_equal_attrs:
            errors.append(
                f"The following attributes should remain unchanged but were modified: {', '.join(changed_equal_attrs)}"
            )

        # Check attributes that should have changed
        logger.info("Checking attributes that should be updated...")
        check_changed_attrs = ["stop_time", "time_coverage_end"]
        unchanged_attrs = []
        for attr in check_changed_attrs:
            if attr not in ds_old.attrs:
                errors.append(
                    f"Expected attribute '{attr}' is missing from original dataset."
                )
            elif attr not in ds_new.attrs:
                errors.append(
                    f"Expected attribute '{attr}' is missing from new dataset."
                )
            elif ds_new.attrs[attr] == ds_old.attrs[attr]:
                unchanged_attrs.append(f"'{attr}': '{ds_old.attrs[attr]}'")

        if unchanged_attrs:
            errors.append(
                f"The following attributes should have been updated but remained unchanged: {', '.join(unchanged_attrs)}"
            )

        # Check for required attributes presence
        logger.info("Validating presence of required attributes...")
        check_presence_attrs = ["publisher_name"]  # TODO: this is incomplete
        missing_attrs = []
        for attr in check_presence_attrs:
            if attr not in ds_new.attrs:
                missing_attrs.append(attr)

        if missing_attrs:
            errors.append(
                f"Required attributes are missing from the new dataset: {', '.join(missing_attrs)}"
            )

        # Check for attributes that should have been dropped
        logger.info("Checking for attributes that should have been removed...")
        unexpected_attrs = []
        for attr in DROP_ATTRS:
            if attr in ds_new.attrs:
                unexpected_attrs.append(attr)

        if unexpected_attrs:
            errors.append(
                f"The following attributes should have been removed but are still present: {', '.join(unexpected_attrs)}"
            )

        # Raise comprehensive error if any issues found
        if errors:
            error_summary = (
                f"Dataset validation failed with {len(errors)} issue(s):\n"
                + "\n".join(f"  {i + 1}. {error}" for i, error in enumerate(errors))
            )
            logger.error(error_summary)
            raise ValueError(error_summary)

        logger.info("All dataset validation tests passed successfully")
        return True

    def update_icechunk_store(
        self,
        run_tests: bool = True,
        dry_run: bool = False,
        limit_granules: int = None,
        # parallel: str = "lithops",
        parallel: str = None,
    ) -> str:
        """
        Main method to update the icechunk store with new data.

        Returns:
            Status message or error details
        """
        # setup the repo
        self.setup_repo()

        # Find the timerange that is new
        logger.info("Finding dates to append to existing store")
        ds_main = self.open_xr_dataset_from_branch("main")

        # MUR SST granules have a temporal range of date 1 21:00:00 to date 2 21:00:00
        last_date = self.get_timestep_from_ds(ds_main, -1).date()
        last_timestep = datetime.combine(
            last_date,
            datetime.strptime("21:00:01", "%H:%M:%S").time(),
            tzinfo=timezone.utc,
        ).isoformat(sep=" ")
        current_date_date = datetime.now(timezone.utc).date()
        current_date = datetime.combine(
            current_date_date,
            datetime.strptime("21:00:00", "%H:%M:%S").time(),
            tzinfo=timezone.utc,
        ).isoformat(sep=" ")

        # search for new data
        new_granules = self.find_granules(
            last_timestep, current_date, limit_granules=limit_granules
        )

        # Search for new data and create a virtual dataset
        vds = self.dataset_from_granules(
            new_granules,
            virtual=True,
            parallel=parallel,
        )
        logger.debug(f"New Data (Virtual): {vds}")

        # Write to the icechunk store
        main_snapshot = self.repo.lookup_branch("main")
        logger.debug(f"Latest main snapshot: {main_snapshot}")
        logger.info(f"Creating branch: {self.branchname}")
        self.repo.create_branch(self.branchname, snapshot_id=main_snapshot)

        logger.info(f"Writing to icechunk branch {self.branchname}")
        commit_message = f"MUR update {self.branchname}"
        # Attributes
        # AFAIKT virtualizarr (and perhaps xarray too?) just update/overwrite the attributes when appending to a store?
        # TODO:This definitely warrants a more detailed discussion, MRE etc in an issue
        # For now what I will attempt is to manually update the attrs on the virtual
        # dataset and then see if those are written to the store.
        # TODO: Check if I need to do this for each variable too?

        logger.info("Updating attrs")
        combined_attrs = combine_attrs([ds_main.attrs, vds.attrs], None)
        logger.info(f"{combined_attrs=}")
        vds.attrs.update(combined_attrs)

        # Append new data and commit
        session = self.repo.writable_session(branch=self.branchname)
        vds.vz.to_icechunk(session.store, append_dim="time")
        snapshot = session.commit(commit_message)
        logger.info(
            f"Commit successful to branch: {self.branchname} as snapshot:{snapshot} \n {commit_message}"
        )

        if run_tests:
            logger.info("Testing new Dataset from branch")
            try:
                self.test_new_data(ds_main, new_granules)
                logger.info("Tests passed.")
            except Exception as e:
                logger.error(f"Tests failed with {e}")
                raise
        else:
            logger.info(f"Got {run_tests=}. Tests skipped.")

        if dry_run:
            logger.info(f"Dry run, not merging {self.branchname} into main")
            return f"Dry run completed successfully. Branch {self.branchname} created but not merged."
        else:
            logger.info(f"Merging {self.branchname} into main")
            self.repo.reset_branch("main", self.repo.lookup_branch(self.branchname))
            return f"Successfully updated store and merged {self.branchname} to main"


# Utility functions that can be used independently
def get_secret_from_aws(secret_arn: str) -> dict:
    """Get secret from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager", region_name=session.region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_arn)
    except Exception as e:
        raise e
    else:
        if "SecretString" in get_secret_value_response:
            return json.loads(get_secret_value_response["SecretString"])
        else:
            raise ValueError("Secret is not a string")

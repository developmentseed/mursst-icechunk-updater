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
from typing import Dict, Tuple
import xarray as xr
from urllib.parse import urlparse, urlunparse
from virtualizarr import open_virtual_mfdataset
from virtualizarr.parsers import HDFParser
from virtualizarr.registry import ObjectStoreRegistry
from obstore.store import S3Store
from icechunk import S3StaticCredentials


# Configure logging for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Constants
COLLECTION_SHORT_NAME = "MUR-JPL-L4-GLOB-v4.1"
DROP_VARS = ["dt_1km_data", "sst_anomaly"]
EXAMPLE_TARGET_URL = "s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20250702090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-GLOB-v02.0-fv04.1.nc"


class MursstUpdater:
    """
    Main class for updating MURSST data in icechunk store.

    This class encapsulates all the business logic and can be used
    independently of AWS Lambda.
    """

    def __init__(self):
        """Initialize the updater with current timestamp for branch naming."""
        self.branchname = f"add_time_{datetime.now(timezone.utc).isoformat()}"

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

    def get_obstore_credentials(self):
        """Get obstore credentials from earthaccess."""
        auth = earthaccess.login(strategy="environment")
        creds = auth.get_s3_credentials(daac="PODAAC")
        return {
            "access_key_id": creds["accessKeyId"],
            "secret_access_key": creds["secretAccessKey"],
            "token": creds["sessionToken"],
            "expires_at": datetime.fromisoformat(creds["expiration"]),
        }

    def obstore_and_registry_from_url(
        self, url: str
    ) -> Tuple[S3Store, ObjectStoreRegistry]:
        """Create obstore and registry from URL."""
        logger.info(f"Setting up obstore and registry for url: {url}")
        parsed = urlparse(url)
        parsed_wo_path = parsed._replace(path="")
        bucket = parsed.netloc
        logger.debug(f"Using bucket: {bucket}")

        cp = self.get_obstore_credentials
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

    def get_icechunk_storage(self, target: str) -> icechunk.Storage:
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

    def create_icechunk_repo(self, store_target: str) -> None:
        """Create a new icechunk repository."""
        storage = self.get_icechunk_storage(store_target)
        logger.info(f"{storage=}")
        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(
                self.get_prefix_from_url(EXAMPLE_TARGET_URL),
                icechunk.s3_store(region="us-west-2"),
            )
        )
        icechunk.Repository.create(
            storage=storage,
            config=config,
            authorize_virtual_chunk_access=self.get_container_credentials(
                EXAMPLE_TARGET_URL
            ),
        )

    def open_icechunk_repo(self, store_target: str) -> icechunk.Repository:
        """Open an existing icechunk repository."""
        logger.info("Opening icechunk repo")
        storage = self.get_icechunk_storage(store_target)

        repo = icechunk.Repository.open(
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
            temporal=(start_date, end_date), short_name=COLLECTION_SHORT_NAME
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
        self, start_date: str, end_date: str, limit_granules: int = None
    ) -> list[DataGranule]:
        """Find granules within date range."""
        granule_results = self.search_valid_granules(start_date, end_date)

        if len(granule_results) == 0:
            logger.warning("No valid granules found")
            return None
        else:
            logger.info(f"Number of granules found: {len(granule_results)}")
            if limit_granules is not None:
                logger.info(f"Limiting the number of granules to {limit_granules}")
                return granule_results[:limit_granules]
            else:
                return granule_results

    def dataset_from_search(
        self,
        start_date: str,
        end_date: str,
        virtual=True,
        limit_granules: int = None,
        parallel="lithops",
        access: str = "direct",
    ) -> xr.Dataset:
        """Create dataset from granule search."""
        logger.debug(f"{limit_granules=}")
        granule_results = self.find_granules(
            start_date, end_date, limit_granules=limit_granules
        )
        if granule_results is None or len(granule_results) == 0:
            raise ValueError("No new data granules available")

        in_region = True if access == "direct" else False
        data_urls = [
            g.data_links(access=access, in_region=in_region)[0] for g in granule_results
        ]

        store, registry = self.obstore_and_registry_from_url(EXAMPLE_TARGET_URL)
        parser = HDFParser()

        def preprocess(ds: xr.Dataset) -> xr.Dataset:
            return ds.drop_vars(DROP_VARS, errors="ignore")

        if virtual:
            return open_virtual_mfdataset(
                data_urls,
                registry=registry,
                parser=parser,
                preprocess=preprocess,
                parallel=parallel,
            )
        else:
            fileset = earthaccess.open(data_urls, provider="POCLOUD")
            return xr.open_mfdataset(
                fileset,
                preprocess=preprocess,
                chunks={},
                parallel=True,
            )

    def get_timestep_from_ds(self, ds: xr.Dataset, nt: str) -> datetime:
        """Get timestep from dataset."""
        return ds.time.data[nt].astype("datetime64[ms]").astype(datetime)

    def open_xr_dataset_from_branch(self, repo: icechunk.Repository, branch: str):
        """Open xarray dataset from icechunk branch."""
        session = repo.readonly_session(branch=branch)
        ds = xr.open_zarr(session.store, consolidated=False)
        return ds

    def test_store_on_branch(
        self, ds_new: xr.Dataset, ds_expected: xr.Dataset
    ) -> Tuple[bool, str]:
        """Test data integrity on branch."""
        logger.info("Starting Tests")
        nt = len(ds_expected.time)

        # Test 1: time continuity
        logger.info("Testing Time continuity")
        try:
            dt_expected = ds_new.time.isel(time=slice(0, 1)).diff("time")
            dt_actual = ds_new.isel(time=slice(-(nt + 1), None)).time.diff("time")
            time_continuity = (dt_actual == dt_expected).all().item()
        except Exception as e:
            time_continuity = False
            time_continuity_error = str(e)
        else:
            time_continuity_error = None

        # Test 2: data equality
        logger.info("Testing Data equality")
        try:
            xr.testing.assert_allclose(ds_expected, ds_new.isel(time=slice(-nt, None)))
            data_equal = True
        except AssertionError as e:
            data_equal = False
            data_equal_error = str(e)
        except Exception as e:
            data_equal = False
            data_equal_error = f"Unexpected error during data comparison: {e}"
        else:
            data_equal_error = None

        # Compose result
        tests_passed = time_continuity and data_equal

        if not tests_passed:
            error_message = "Failures:\n"
            if not time_continuity:
                error_message += f"- Time continuity failed: {time_continuity_error or 'Mismatch in timestep differences'}\n"
            if not data_equal:
                error_message += f"- Data equality failed: {data_equal_error}\n"
        else:
            error_message = None

        return tests_passed, error_message

    def update_icechunk_store(
        self,
        store_target: str,
        run_tests: bool = True,
        dry_run: bool = False,
        limit_granules: int = None,
        parallel: str = "lithops",
    ) -> str:
        """
        Main method to update the icechunk store with new data.

        Args:
            store_target: URL or path to the icechunk store
            run_tests: Whether to run data validation tests
            dry_run: Whether to skip the final merge to main
            limit_granules: Limit number of granules to process
            parallel: Parallelization method for virtualizarr

        Returns:
            Status message or error details
        """
        repo = self.open_icechunk_repo(store_target)

        # Find the timerange that is new
        logger.info("Finding dates to append to existing store")
        ds_main = self.open_xr_dataset_from_branch(repo, "main")

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

        # Search for new data and create a virtual dataset
        vds = self.dataset_from_search(
            last_timestep,
            current_date,
            virtual=True,
            limit_granules=limit_granules,
            parallel=parallel,
        )
        logger.debug(f"New Data (Virtual): {vds}")

        # Write to the icechunk store
        main_snapshot = repo.lookup_branch("main")
        logger.debug(f"Latest main snapshot: {main_snapshot}")
        logger.info(f"Creating branch: {self.branchname}")
        repo.create_branch(self.branchname, snapshot_id=main_snapshot)

        logger.info(f"Writing to icechunk branch {self.branchname}")
        commit_message = f"MUR update {self.branchname}"
        session = repo.writable_session(branch=self.branchname)
        vds.vz.to_icechunk(session.store, append_dim="time")
        snapshot = session.commit(commit_message)
        logger.info(
            f"Commit successful to branch: {self.branchname} as snapshot:{snapshot} \n {commit_message}"
        )

        if run_tests:
            # Compare data committed and reloaded from granules not using icechunk
            logger.info("Reloading Dataset from branch")
            ds_new = self.open_xr_dataset_from_branch(repo, self.branchname)
            logger.info(f"Dataset on {self.branchname}: {ds_new}")

            logger.info("Building Test Datasets")
            ds_original = self.dataset_from_search(
                last_timestep,
                current_date,
                virtual=False,
                access="external",
                limit_granules=limit_granules,
            )
            logger.info(f"Test Dataset: {ds_original}")

            passed, message = self.test_store_on_branch(ds_new, ds_original)

            if not passed:
                logger.info(f"Tests did not pass with: {message}")
                return message
            else:
                logger.info("Tests passed.")
        else:
            logger.info(f"Got {run_tests=}. Tests skipped.")

        if dry_run:
            logger.info(f"Dry run, not merging {self.branchname} into main")
            return f"Dry run completed successfully. Branch {self.branchname} created but not merged."
        else:
            logger.info(f"Merging {self.branchname} into main")
            repo.reset_branch("main", repo.lookup_branch(self.branchname))
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

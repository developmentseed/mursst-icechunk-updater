# TODOs

## For development and testing

- [x] create a test dataset in nasa-eodc-scratch which should end at least 10 days before the current date
  - uv run --env-file=.env.dev bash
  - echo "$ICECHUNK_DIRECT_PREFIX$STORE_NAME" --> s3://nasa-veda-scratch/mursst-dev-aimee/icechunk/MUR-JPL-L4-GLOB-v4.1-virtual-test
  - on 03/13/2026, `start_date = "2026-02-28 21:00:01"` and `end_date = (datetime.now() - timedelta(days=12))`
  - created a dataset with just 1 timestamp: 2026-03-01T09:00:00
- [x] make sure the hub test works
  - using `updater.update_icechunk_store(store_url, parallel=False)` 2 timesteps are are added
  - `Successfully updated store and merged add_time_2026-03-13T15:39:41.273590+00:00 to main`
- [x] try appending to it using the lambda function without overwrite args
  - removed and rebuilt the store
  - ran the lambda, first with DRY_RUN=true and then DRY_RUN=false
- [x] test it works with the overwrite args
  - added to the lambda:
    ```json
    test_event = {
        "overwrite_start_date": "2026-03-01",
        "overwrite_start_date": "2026-03-03"
    }
    ```
  - `Not all values of coordinate 'time' in the new array were found in the original store`
- [ ] add overwrite_start_date and overwrite_start_date as an argument to the lambda manual run
- [x] Remove the local_dev notebook

## Deploy to staging

- [ ] test the staging deployment
- [ ] test overwriting with the staging deployment

## Deploy to prod

- [ ] test the prod deployment
- [ ] overwrite in the prod deployment

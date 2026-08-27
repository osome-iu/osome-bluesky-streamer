# osome-bluesky-streamer

## Quick start

Stream *live data to `yyyy-mm-dd.json` file, no Auth needed
```
conda activate ../venv/
python firehose_streamer.py
```

It will try to pickup from where it stopped and keep running indefinitely.

**Other Scripts**
- `alive_huh.sh` sometimes the `firehose_streamer.py` stall and not saving new activities, keep this script running so to force restart the python process when the file has not been updated for x amount of time
- `daily_workflow.sh` is used to summarize (with `count_types.py`), compress, and backup the generated `.json` file. Default to perform work on `yyyy-mm-dd.json` file from yesterday's date (UTC) but also allows optional args for specific date and dry-run.
    ```
    ./daily_workflow.sh --date 2025-10-22 --dry-run
    ```

## Documentation

This firehose streamer is built using the [AT Protocol SDK](https://atproto.blue/en/latest/index.html). 

`firehose/docs` contains design documentations.
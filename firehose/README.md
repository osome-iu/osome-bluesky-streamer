# osome-bluesky-streamer

## Quick start

Stream *live data to yyyy-mm-dd.json file, no Auth needed
```
conda activate bsky
python firehose_streamer.py
```

It will try to pickup from where it stopped and keep running indefinitely.

**Other Scripts**
- `alive_huh.sh` sometimes the `firehose_streamer.py` stall and not saving new activities, keep this script running so to force restart the python process when the file has not been updated for x amount of time
- `daily_workflow.sh` is used to summarize (with `count_types.py`), compress, and backup the generated `.json` file

## Documentation

This firehose streamer is built using the [AT Protocol SDK](https://atproto.blue/en/latest/index.html). 
# Labelers

Bsky uses Labelers as part of their moderation system, here are some scripts that helps to find the labelers info and get the labels they created.


1. `get_plc_data.py` save PLC operations to find the latest labeler info, automatically stops when reaching the latest events
2. `get_labeler_profiles.py` download profiles of labelers containing their label definitions and more
3. `get_labeler_endpoints.py` get labelers' streaming endpoints
4. `label_streamers.py` stream label operations from the labelers

----

Related Links
- https://docs.bsky.app/blog/blueskys-moderation-architecture
- https://web.plc.directory/
- https://docs.bsky.app/docs/api/app-bsky-labeler-get-services
- https://atproto.blue/en/latest/atproto_firehose/index.html#atproto_firehose.AsyncFirehoseSubscribeLabelsClient
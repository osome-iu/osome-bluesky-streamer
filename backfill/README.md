## Backfilling Data

[Backfilling](https://docs.bsky.app/docs/advanced-guides/backfill) is the process of collecting all data available across the Bluesky network. It would collect all user activities of every active user, including information such as profile, follows, followers, posting, reposting, liking...etc.

The main idea is to first discover all PDSs by examing the [PLC](https://web.plc.directory/api/redoc) history, then request data for all DIDs from their corresponding PDSs. As a note, part of `get_user_records.py` is generating a file (`pds_did_list.json`) containing all PDSs and their corresponding DIDs for later use in [requesting repo/CAR files](https://docs.bsky.app/docs/api/com-atproto-sync-get-repo) and user profile details. So you'd want to wait until that finishes before running the profile script.

**Steps:**

1. `get_plc_history.py`    : crawl all plc operations to file `plc_data.jsonl`
2. Collect user data:
    - Create a [bsky.app](https://bsky.app/) account and fill in your login credentials to `bsky_login_info.json`
    - `get_user_records.py`                   : retrieve and decode user Repo CAR files
    - `get_user_profile_follows_follower.py`  : saves user info

**Extra**
- `get_audit_log.py` : save audit log for each unique DIDs in `audit_logs/`, gathers PLC operation history for each DID

----

**Links**
- https://docs.bsky.app/docs/advanced-guides/atproto
- https://docs.bsky.app/docs/advanced-guides/backfill
- https://web.plc.directory/api/redoc
- https://docs.bsky.app/docs/api/com-atproto-sync-get-repo
- https://docs.bsky.app/docs/api/app-bsky-actor-get-profile
- https://docs.bsky.app/docs/api/app-bsky-graph-get-follows
- https://docs.bsky.app/docs/api/app-bsky-graph-get-followers
- https://arxiv.org/abs/2402.03239


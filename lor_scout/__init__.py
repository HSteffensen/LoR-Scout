import os
from datetime import timedelta
from pathlib import Path

from pyot.core import Settings

from . import api_collector, data_keeper, deck_sniffer

__all__ = ["api_collector", "data_keeper", "deck_sniffer"]


_API_KEY = os.environ.get("RIOT_API_KEY")
if not _API_KEY:
    with open("dev_riot_api_key.txt", "r") as f:
        _API_KEY = f.read().strip("\n")


Settings(
    MODEL="LOR",
    DEFAULT_REGION="AMERICAS",
    DEFAULT_LOCALE="EN_US",
    PIPELINE=[
        {
            "BACKEND": "pyot.stores.Omnistone",
            "EXPIRATIONS": {
                "match_v1_matchlist": timedelta(hours=1),
                "match_v1_match": timedelta(days=28),
            },
        },
        {
            "BACKEND": "pyot.stores.DiskCache",
            "DIRECTORY": Path.cwd() / "diskcache",
            "EXPIRATIONS": {
                "account_v1_by_puuid": timedelta(days=10),
                "account_v1_by_riot_id": timedelta(days=10),
                "account_v1_active_shard": timedelta(days=1),
                "ranked_v1_leaderboards": timedelta(hours=1),
                "match_v1_matchlist": timedelta(hours=23),
                "match_v1_match": timedelta(days=56),
            },
        },
        {"BACKEND": "pyot.stores.DDragon"},
        {
            "BACKEND": "pyot.stores.RiotAPI",
            "API_KEY": _API_KEY,  # API KEY
            "RATE_LIMITER": {
                "BACKEND": "pyot.limiters.MemoryLimiter",
                "LIMITING_SHARE": 1,
            },
            # default error handling
        },
    ],
).activate()  # <- DON'T FORGET TO ACTIVATE THE SETTINGS

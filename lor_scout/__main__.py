import logging
import argparse

from pandas import Timestamp, Timedelta
from pyot.utils import loop_run

from . import api_collector, data_keeper

_LOGGER = logging.getLogger("lor_scout")
_STDERR_HANDLER = logging.StreamHandler()
_STDERR_HANDLER.setLevel(logging.DEBUG)
_STDERR_FORMAT = logging.Formatter(
    "[%(asctime)s|%(module)s->%(funcName)s|%(levelname)s] %(message)s"
)  # for reference: https://docs.python.org/2/library/logging.html#logrecord-attributes
_STDERR_HANDLER.setFormatter(_STDERR_FORMAT)
_LOGGER.addHandler(_STDERR_HANDLER)
_LOGGER.setLevel(logging.DEBUG)

_DEFAULTS = {"collect_player_count": 2}


def read_args():
    args = argparse.ArgumentParser("LoR Scout", description="")
    args.add_argument("--puuids", nargs="*")
    args.add_argument(
        "--collect-player-count",
        dest="collect_player_count",
        type=int,
        default=_DEFAULTS["collect_player_count"],
    )  # todo: replace magic number
    return args.parse_args()


def main(args: argparse.Namespace):
    print(args)

    data_keeper.ALL_MATCHES.save(backup=True)
    data_keeper.ALL_PLAYERS.save(backup=True)
    data_keeper.RECENT_MATCHES.save(backup=True)
    data_keeper.RECENT_PLAYERS.save(backup=True)

    try:
        puuids = args.puuids or api_collector.puuids_to_collect_from(
            desired_count=args.collect_player_count or _DEFAULTS["collect_player_count"]
        )
        loop_run(api_collector.collect_from_puuids(puuids))
    finally:

        # print(data_keeper.RECENT_PLAYERS.dataframe)
        # print(data_keeper.RECENT_MATCHES.dataframe)

        data_keeper.ALL_MATCHES.update(data_keeper.RECENT_MATCHES.dataframe)
        data_keeper.ALL_PLAYERS.update(data_keeper.RECENT_PLAYERS.dataframe)
        recent_since = Timestamp.utcnow() - Timedelta(days=7)
        data_keeper.RECENT_MATCHES.dataframe = data_keeper.RECENT_MATCHES.dataframe[
            data_keeper.RECENT_MATCHES.dataframe.game_start_time_utc > recent_since
        ]
        data_keeper.RECENT_PLAYERS.dataframe = data_keeper.RECENT_PLAYERS.dataframe[
            data_keeper.RECENT_PLAYERS.dataframe.last_ranked_match_time > recent_since
        ]

        data_keeper.ALL_MATCHES.save()
        data_keeper.ALL_PLAYERS.save()
        data_keeper.RECENT_MATCHES.save()
        data_keeper.RECENT_PLAYERS.save()


main(read_args())

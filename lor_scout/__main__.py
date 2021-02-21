from pandas import Timestamp, Timedelta
from pyot.utils import loop_run

from . import collector, data_keeper


def main():
    data_keeper.ALL_MATCHES.save(backup=True)
    data_keeper.ALL_PLAYERS.save(backup=True)
    data_keeper.RECENT_MATCHES.save(backup=True)
    data_keeper.RECENT_PLAYERS.save(backup=True)

    try:
        puuids = collector.puuids_to_collect_from()
        loop_run(collector.collect_from_puuids(puuids))
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


main()

"""Collect data from the Riot API, primarily match information and deckcodes.

https://riot-api-libraries.readthedocs.io/en/latest/collectingdata.html
https://developer.riotgames.com/docs/lor
https://developer.riotgames.com/apis
"""

import logging
from typing import Iterable

from pandas import Timedelta, Timestamp, to_datetime as pandas_to_datetime
from pyot.models import lor
from pyot.core import Queue, exceptions as pyot_exceptions

from . import data_keeper

LOGGER = logging.getLogger("lor_scout.collector")
LOGGER.setLevel(logging.DEBUG)


def puuids_to_collect_from(
    count: int = 2,  # todo: replace magic number
    since: Timedelta = Timedelta(
        hours=24
    ),  # todo: maybe replace magic number, but I think this one is fine
) -> Iterable[str]:
    run_since = Timestamp.utcnow() - since
    players = data_keeper.RECENT_PLAYERS.dataframe
    players_to_update = players[
        players.last_check_time.isnull() | (players.last_check_time < run_since)
    ]
    LOGGER.debug(
        f"[collector.puuids_to_collect_from] Of {len(players.index)} recent players, "
        f"{len(players_to_update.index)} have not been updated in the past {since}."
    )
    puuids = players_to_update.reset_index().puuid
    if count > 0:
        puuids = puuids.sample(count)
    return puuids


async def collect_from_puuids(
    puuids: Iterable[str],
    time_cutoff: Timedelta = Timedelta(days=7),  # todo: replace magic number
) -> None:
    match_cutoff = Timestamp.utcnow() - time_cutoff
    LOGGER.debug("[collector.collect_from_puuids] Entering Queue")
    async with Queue() as queue:
        for puuid in puuids:
            await queue.put(
                consume_match_history(
                    queue, lor.MatchHistory(puuid=puuid), match_cutoff
                )
            )
        await queue.join()
    LOGGER.debug("[collector.collect_from_puuids] Completed Queue")


async def consume_match_history(
    queue: Queue, match_history: lor.MatchHistory, match_cutoff: Timestamp = None
) -> None:
    match_history = await match_history.get(sid=queue.sid)
    current_puuid = match_history.puuid
    last_match_time: Timestamp = None

    for match in match_history:
        try:
            match = await match.get(sid=queue.sid)
        except pyot_exceptions.NotFound:
            # https://github.com/RiotGames/developer-relations/issues/381
            pass
        except pyot_exceptions.PyotException as e:
            # not the cleanest solution, but simply halting is better than nothing?
            # and this shouldn't get in the way of Pyot's nice rate limiting.
            LOGGER.error(f"[collector.consume_match_history] {e}")
            break

        if match.info.mode == "Constructed" and match.info.type == "Ranked":
            data_keeper.RECENT_MATCHES.store_match(match)

            match_time = pandas_to_datetime(match.info.creation)
            if not last_match_time or match_time > last_match_time:
                last_match_time = match_time
            for puuid in (
                p
                for p in match.metadata.participant_puuids
                if p not in data_keeper.RECENT_PLAYERS.dataframe.index
            ):
                data_keeper.RECENT_PLAYERS.discover_player(puuid, match_time)
        if pandas_to_datetime(match.info.creation) < match_cutoff:
            # to save API calls by not caring about old matches,
            # stop the loop because the match history list is in time order
            LOGGER.debug(
                "[collector.consume_match_history] "
                f"Breaking early for puuid={current_puuid} because of old match_time."
            )
            break

    data_keeper.RECENT_PLAYERS.store_player(
        current_puuid, last_match_time, Timestamp.utcnow()
    )

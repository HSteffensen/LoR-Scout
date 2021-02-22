"""Collect data from the Riot API, primarily match information and deckcodes.

https://riot-api-libraries.readthedocs.io/en/latest/collectingdata.html
https://developer.riotgames.com/docs/lor
https://developer.riotgames.com/apis
"""

import logging
from typing import Iterable, Optional

from pandas import Timedelta, Timestamp
from pandas import to_datetime as pandas_to_datetime
from pyot.core import Queue
from pyot.core import exceptions as pyot_exceptions
from pyot.models import lor

from . import data_keeper

LOGGER = logging.getLogger("lor_scout.api_collector")


def puuids_to_collect_from(
    desired_count: int = 2,  # todo: replace magic number
    not_updated_since: Timedelta = Timedelta(
        hours=24
    ),  # todo: maybe replace magic number, but I think this one is fine
) -> Iterable[str]:
    now = Timestamp.utcnow()
    run_since = now - not_updated_since
    players = data_keeper.RECENT_PLAYERS.dataframe
    players_to_update = players[
        players.last_check_time.isnull() | (players.last_check_time < run_since)
    ].sort_values(by="last_ranked_match_time", ascending=False)
    LOGGER.debug(
        f"{len(players_to_update.index)} of {len(players.index)} recent players "
        f"have not been updated in the past {not_updated_since}"
    )
    puuids = players_to_update.reset_index().puuid
    return puuids[: min(desired_count, len(puuids))]


async def collect_from_puuids(
    puuids: Iterable[str],
    time_cutoff: Timedelta = Timedelta(days=7),  # todo: replace magic number
) -> None:
    match_cutoff = Timestamp.utcnow() - time_cutoff
    async with Queue() as queue:
        for puuid in puuids:
            await queue.put(
                consume_match_history(
                    queue, lor.MatchHistory(puuid=puuid), match_cutoff
                )
            )
        await queue.join()


async def consume_match_history(
    queue: Queue, match_history: lor.MatchHistory, match_cutoff: Timestamp = None
) -> None:
    match_history = await match_history.get(sid=queue.sid)
    current_puuid = match_history.puuid
    last_match_time: Optional[Timestamp] = None
    ranked_match_count = 0

    # design note:
    # Could filter out match_ids we've seen before (in RECENT_MATCHES dataframe),
    # but API caching makes it fine to repeat them anyway. Not sure which is actually
    # best, but either seems fine right now.
    for match in match_history:
        try:
            match = await match.get(sid=queue.sid)
        except pyot_exceptions.NotFound:
            # https://github.com/RiotGames/developer-relations/issues/381
            continue
        except pyot_exceptions.PyotException as e:
            # not the cleanest solution, but simply halting is better than nothing?
            # and this shouldn't get in the way of Pyot's nice rate limiting.
            LOGGER.error(e)
            break

        match_time = pandas_to_datetime(match.info.creation)
        if match.info.mode == "Constructed" and match.info.type == "Ranked":
            ranked_match_count += 1
            data_keeper.RECENT_MATCHES.store_match(match)

            if not last_match_time or match_time > last_match_time:
                last_match_time = match_time
            for puuid in (
                p
                for p in match.metadata.participant_puuids
                if p not in data_keeper.RECENT_PLAYERS.dataframe.index
            ):
                LOGGER.debug(f"Found new player puuid='{puuid}'")
                data_keeper.RECENT_PLAYERS.discover_player(puuid, match_time)
        if match_time < match_cutoff:
            # to save API calls by not caring about old matches,
            # stop the loop because the match history list is in time order
            LOGGER.debug(
                f"Breaking early for puuid='{current_puuid}' because of old match, "
                f"match_id='{match.id}' and game_start_time_utc='{match_time}'"
            )
            break

    LOGGER.debug(
        f"Updated player puuid='{current_puuid}', saw {ranked_match_count}"
        " ranked matches"
    )
    data_keeper.RECENT_PLAYERS.store_player(
        current_puuid, last_match_time, Timestamp.utcnow()
    )

"""Collect data from the Riot API, primarily match information and deckcodes.

Intentionally don't log games played by myself, but do still collect the other
player's puuid. This is because I will be intentionally grabbing my own games
only at the start, until I have a larger library of puuids than my own.

https://riot-api-libraries.readthedocs.io/en/latest/collectingdata.html
https://developer.riotgames.com/docs/lor
https://developer.riotgames.com/apis
"""

from typing import Iterable, List

from pandas import Timedelta, Timestamp, to_datetime
from pyot.models import lor
from pyot.core import Queue, Gatherer
from pyot.core.exceptions import NotFound
from pyot.utils import FrozenGenerator


from . import data_keeper


def puuids_to_collect_from(since: Timedelta = Timedelta(hours=24)) -> Iterable[str]:
    run_since = Timestamp.utcnow() - since
    players = data_keeper.RECENT_PLAYERS.dataframe
    players_to_update = players[
        players.last_check_time.isnull() | (players.last_check_time < run_since)
    ]
    puuids = players_to_update.reset_index().puuid
    return puuids.sample(10)


async def collect_from_puuids(puuids: Iterable[str]) -> None:
    async with Queue() as queue:
        for puuid in puuids:
            await queue.put(consume_match_history(queue, lor.MatchHistory(puuid=puuid)))
        await queue.join()


async def consume_match_history(queue: Queue, match_history: lor.MatchHistory) -> None:
    match_history = await match_history.get(sid=queue.sid)
    current_puuid = match_history.puuid
    last_match_time: Timestamp = None

    async with Gatherer() as gatherer:
        gatherer.statements = [
            m
            for m in match_history.matches
            if m.id not in data_keeper.RECENT_MATCHES.dataframe.index
        ]
        responses: List[lor.Match] = await gatherer.gather()

    responses = [
        r for r in responses if r.info.mode == "Constructed" and r.info.type == "Ranked"
    ]
    for match in responses:
        data_keeper.RECENT_MATCHES.store_match(match)

        match_time = to_datetime(match.info.creation)
        if not last_match_time or match_time > last_match_time:
            last_match_time = match_time
        for puuid in (
            p for p in match.metadata.participant_puuids if p != current_puuid
        ):
            data_keeper.RECENT_PLAYERS.discover_player(puuid, match_time)

    data_keeper.RECENT_PLAYERS.store_player(
        current_puuid, last_match_time, Timestamp.utcnow()
    )


# async def consume_match_history(queue: Queue, match_history: lor.MatchHistory) -> None:
#     match_history = await match_history.get(sid=queue.sid)
#     current_puuid = match_history.puuid
#     last_match_time: Timestamp = None

#     for match in match_history:
#         if match.id in data_keeper.RECENT_MATCHES.dataframe.index:
#             continue
#         try:
#             match = await match.get(sid=queue.sid)
#         except NotFound:
#             data_keeper.RECENT_MATCHES.store_match_id(str(match.id))
#             continue
#         if match.info.mode == "Constructed" and match.info.type == "Ranked":
#             data_keeper.RECENT_MATCHES.store_match(match)

#             match_time = to_datetime(match.info.creation)
#             if not last_match_time or match_time > last_match_time:
#                 last_match_time = match_time
#             for puuid in (
#                 p for p in match.metadata.participant_puuids if p != current_puuid
#             ):
#                 data_keeper.RECENT_PLAYERS.discover_player(puuid, match_time)

#     data_keeper.RECENT_PLAYERS.store_player(
#         current_puuid, last_match_time, Timestamp.utcnow()
#     )

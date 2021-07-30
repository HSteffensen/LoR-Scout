"""Handle data read and write.

Phase 1: Storing locally
Phase 2: Cloud storage
"""

from ast import literal_eval
from pathlib import Path
from typing import Dict, List, Optional

import pandas
from pandas.core.frame import DataFrame
from pyot.models.lor import Match

_DATA_FOLDER = Path("long_term_data")


class DataKeeperLocal:
    def __init__(self, name: str, read_csv_kwargs: dict = None):
        read_csv_kwargs = read_csv_kwargs or {}
        self._name = name
        self._path = _DATA_FOLDER / f"{name}.csv"
        self._backup_path = _DATA_FOLDER / f"{name}.backup.csv"
        self.dataframe: pandas.DataFrame = pandas.read_csv(
            self._path, **read_csv_kwargs
        )

    def update(self, other: pandas.DataFrame, overwrite=True):
        # update rows whose index is already there
        self.dataframe.update(other, overwrite=overwrite)
        # then concat rows whose index isn't already in
        self.dataframe = pandas.concat(
            [self.dataframe, other[~other.index.isin(self.dataframe.index)]]
        )

    def save(self, backup=False):
        path = self._backup_path if backup else self._path
        self.dataframe.to_csv(path)


class PlayerKeeper:
    def store_player(
        self,
        puuid: str,
        last_ranked_match_time: Optional[pandas.Timestamp],
        last_check_time: pandas.Timestamp,
    ) -> None:
        raise NotImplementedError()

    def discover_player(
        self, puuid: str, discovered_match_time: pandas.Timestamp
    ) -> None:
        raise NotImplementedError()


class PlayerKeeperLocal(DataKeeperLocal, PlayerKeeper):
    def __init__(self, type: str):
        type = type.lower()
        valid_types = ("all", "recent")
        if type not in valid_types:
            raise ValueError(f"Invalid type: {type}")

        read_csv_kwargs = {
            "index_col": "puuid",
            "dtype": {
                "puuid": "str",
                "last_ranked_match_time": "str",
                "last_check_time": "str",
            },
            "parse_dates": ["last_ranked_match_time", "last_check_time"],
        }
        super().__init__(f"{type}_players", read_csv_kwargs)

    def store_player(
        self,
        puuid: str,
        last_ranked_match_time: Optional[pandas.Timestamp],
        last_check_time: pandas.Timestamp,
    ):
        self.update(
            players_dataframe_row(puuid, last_ranked_match_time, last_check_time)
        )

    def discover_player(self, puuid: str, discovered_match_time: pandas.Timestamp):
        self.update(
            players_dataframe_row(puuid, discovered_match_time, None), overwrite=False
        )


class MatchKeeper:
    dataframe: DataFrame

    def store_match(self, match: Match):
        raise NotImplementedError()

    def store_match_id(self, match_id: str):
        raise NotImplementedError()


class MatchKeeperLocal(DataKeeperLocal, MatchKeeper):
    def __init__(self, type: str):
        # type = type.lower()
        # valid_types = ("all", "recent")
        # if type not in valid_types:
        #     raise ValueError(f"Invalid type: {type}")

        read_csv_kwargs = {
            "index_col": "match_id",
            "dtype": {
                "game_start_time_utc": "str",
            },
            "parse_dates": ["game_start_time_utc"],
        }
        super().__init__(f"{type}_matches", read_csv_kwargs)
        # self.dataframe.players = self.dataframe.players.apply(literal_eval) # todo: uncomment

    def store_match(self, match: Match):
        players = [
            {"puuid": player.puuid, "deck_code": player.deck_code}
            for player in match.info.players
        ]
        self.update(
            matches_dataframe_row(
                str(match.id),
                pandas.to_datetime(match.info.creation),
                match.info.version,
                players,
                next(player.deck_code for player in match.info.players if player.win),
                next(
                    player.deck_code
                    for player in match.info.players
                    if player.order_of_play == 0
                ),
            )
        )

    def store_match_id(self, match_id: str):
        self.update(
            matches_dataframe_row(
                match_id,
                None,
                None,
                None,
                None,
                None,
            )
        )


ALL_MATCHES = MatchKeeperLocal("all")
ALL_PLAYERS = PlayerKeeperLocal("all")
RECENT_MATCHES = MatchKeeperLocal("recent")
RECENT_PLAYERS = PlayerKeeperLocal("recent")


def players_dataframe_row(
    puuid: str,
    last_ranked_match_time: Optional[pandas.Timestamp],
    last_check_time: Optional[pandas.Timestamp],
):
    return pandas.DataFrame(
        data=[[puuid, last_ranked_match_time, last_check_time]],
        columns=["puuid", "last_ranked_match_time", "last_check_time"],
    ).set_index("puuid")


def matches_dataframe_row(
    match_id: str,
    game_start_time_utc: Optional[pandas.Timestamp],
    game_version: Optional[str],
    players: Optional[List[Dict[str, str]]],
    winner_deck: Optional[str],
    first_attacker_deck: Optional[str],
):
    return pandas.DataFrame(
        data=[
            [
                match_id,
                game_start_time_utc,
                game_version,
                players,
                winner_deck,
                first_attacker_deck,
            ]
        ],
        columns=[
            "match_id",
            "game_start_time_utc",
            "game_version",
            "players",
            "winner_deck",
            "first_attacker_deck",
        ],
    ).set_index("match_id")


async def update_old_matches_to_new():
    new_keeper = MatchKeeperLocal("new")
    for match_row in ALL_MATCHES.dataframe.reset_index().itertuples(index=True):
        match_id = match_row.match_id
        if match_id in new_keeper.dataframe.index:
            continue
        print(f"updating match_id='{match_id}'")
        match = Match(id=match_id)
        match = await match.get()
        new_keeper.store_match(match)
        new_keeper.save()
    new_keeper.save()


"""
import lor_scout
from pyot.utils import loop_run
loop_run(lor_scout.data_keeper.update_old_matches_to_new())
"""

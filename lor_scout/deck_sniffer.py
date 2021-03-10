from pandas import DataFrame, Timedelta, Timestamp
from typing import NamedTuple

from . import data_keeper


class GameVersion(NamedTuple):
    major: int
    minor: int
    patch: int

    @classmethod
    def from_version_string(cls, version: str) -> "GameVersion":
        """
        >>> GameVersion.from_version_string("live_1_2_34")
        GameVersion(major=1, minor=2, patch=34)
        """
        split = version.split("_")
        return cls(*[int(s) for s in split[1:4]])


def decks_from_matches(
    matches: DataFrame, min_version_string: str = "live_1_0_0"
) -> DataFrame:
    """Make a decks DataFrame from a matches DataFrame.

    Arguments:
        matches - data_keeper.RECENT_MATCHES probably
        min_version_string - minimum game version to keep

    Return:
        DataFrame with the following columns -
          - deck_code (str) - index, deck code
          - match_count (int) - total matches seen since the min_version
          - win_count (int) - total match wins seen since the min_version
          - first_attacker_count (int) - total matches with first attack token seen
                since the min_version
          - win_as_first_attacker_count (int) - total match wins with first attack
                token seen since the min_version
    """
    min_version = GameVersion.from_version_string(min_version_string)
    matches = matches.where(
        matches.game_version.apply(GameVersion.from_version_string) >= min_version
    )

    all_decks = matches.decks.explode(ignore_index=True)
    decks_df = DataFrame(index=all_decks.unique())
    decks_df.index.rename("deck_code")
    decks_df["match_count"] = [
        len(all_decks[all_decks == deck]) for deck in decks_df.index
    ]
    decks_df["win_count"] = [
        len(matches[matches.winner_deck == deck]) for deck in decks_df.index
    ]
    decks_df["first_attacker_count"] = [
        len(matches[matches.first_attacker_deck == deck]) for deck in decks_df.index
    ]
    decks_df["win_as_first_attacker_count"] = [
        len(
            matches[
                (matches.first_attacker_deck == deck) & (matches.winner_deck == deck)
            ]
        )
        for deck in decks_df.index
    ]

    return decks_df

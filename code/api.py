from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse, parse_qs, urlunparse
from datetime import date

import requests
import polars as pl
import helpers as h

#====================================================================================
# API Callers
#====================================================================================

# Games
def call_schedule_api(triCode, seasonId) -> pl.DataFrame: 
    url = f"https://api-web.nhle.com/v1/club-schedule-season/{triCode}/{seasonId}"
    response = requests.get(url)
    response.raise_for_status()  
    json_data = response.json()

    df = pl.DataFrame(json_data.get("games", []))

    df = df.select(
            pl.col("id").alias("gameId").cast(pl.Utf8),
            pl.col("season").cast(pl.Utf8),
            pl.col("gameState"),
            pl.col("gameType"),
            pl.col("startTimeUTC"),
            pl.col("venue").struct.field("default").alias("venue"),

            pl.col("homeTeam").struct.field("id").alias("homeTeamId").cast(pl.Utf8),
            pl.col("awayTeam").struct.field("id").alias("awayTeamId").cast(pl.Utf8),
            pl.col("homeTeam").struct.field("abbrev").alias("homeTeamAbbrev").cast(pl.Utf8),
            pl.col("awayTeam").struct.field("abbrev").alias("awayTeamAbbrev").cast(pl.Utf8),

            pl.col("homeTeam").struct.field("score").alias("homeTeamScore"),
            pl.col("awayTeam").struct.field("score").alias("awayTeamScore"),

            pl.col("gameOutcome").struct.field("lastPeriodType").alias("gameOutcome"),

            # pl.col("homeTeam").struct.field("logo").alias("homeTeam_logo"),
            # pl.col("awayTeam").struct.field("logo").alias("awayTeam_logo"),
    )


    df = df.with_columns(
        pl.lit(date.today()).alias("evalDate")
    )

    return df

# Play by Play 
def call_play_by_play_api(gameId: str) -> pl.DataFrame:
    url = f"https://api-web.nhle.com/v1/gamecenter/{gameId}/play-by-play"

    response = requests.get(url)
    response.raise_for_status()
    json_data = response.json()

    df = pl.DataFrame(json_data.get("plays", []))

    away_team_id = (json_data.get("awayTeam") or {}).get("id")
    home_team_id = (json_data.get("homeTeam") or {}).get("id")


    # helper: safely pull optional keys from the nested `details` struct/dict
    # (works even if Polars inferred `details` without that field)
    def details_get(field: str, alias: str | None = None) -> pl.Expr:
        out_name = alias or field
        return pl.col("details").map_elements(
            lambda d: (d.get(field) if d is not None else None)
        ).alias(out_name)

    df = df.select([
        pl.col("eventId").cast(pl.Utf8),
        pl.col("periodDescriptor").struct.field("number").alias("periodNumber"),
        pl.col("timeInPeriod"),
        pl.col("timeRemaining"),
        pl.col("situationCode"),
        pl.col("homeTeamDefendingSide"),
        pl.col("typeCode"),
        pl.col("typeDescKey"),
        pl.col("sortOrder"),

        # details (all treated as optional / blank if missing)
        details_get("eventOwnerTeamId").cast(pl.Utf8),
        details_get("losingPlayerId").cast(pl.Utf8),
        details_get("winningPlayerId").cast(pl.Utf8),
        details_get("xCoord"),
        details_get("yCoord"),
        details_get("zoneCode"),
        details_get("shotType"),
        details_get("shootingPlayerId").cast(pl.Utf8),
        details_get("goalieInNetId").cast(pl.Utf8),
        details_get("awaySOG"),
        details_get("homeSOG"),
        details_get("reason"),
        details_get("playerId").cast(pl.Utf8),
        details_get("blockingPlayerId").cast(pl.Utf8),
        details_get("scoringPlayerId").cast(pl.Utf8),
        details_get("scoringPlayerTotal").cast(pl.Utf8),
        details_get("assist1PlayerId").cast(pl.Utf8),
        details_get("assist1PlayerTotal").cast(pl.Utf8),
        details_get("assist2PlayerId").cast(pl.Utf8),
        details_get("assist2PlayerTotal").cast(pl.Utf8),
        details_get("awayScore"),
        details_get("homeScore"),
        details_get("hittingPlayerId").cast(pl.Utf8),
        details_get("hitteePlayerId").cast(pl.Utf8),
        details_get("secondaryReason"),
        details_get("typeCode", alias="details_typeCode"),  # avoid name clash with top-level typeCode
        details_get("descKey"),
        details_get("duration"),
        details_get("committedByPlayerId").cast(pl.Utf8),
        details_get("drawnByPlayerId").cast(pl.Utf8),
    ])

    df = df.with_columns(
        pl.lit(gameId).alias("gameId").cast(pl.Utf8),
        pl.lit(away_team_id).cast(pl.Utf8).alias("awayTeamId"),
        pl.lit(home_team_id).cast(pl.Utf8).alias("homeTeamId"),
    )

    return df

# Shifts
def call_shift_api(gameId: str) -> pl.DataFrame:

    url = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gameId}"

    r = requests.get(url)
    r.raise_for_status()
    rows = r.json().get("data", [])

    if not rows:
        return h.empty_df(h.SHIFT_SCHEMA)

    df = pl.DataFrame(
        rows,
        schema_overrides={"eventDescription": pl.Utf8},  # <-- fixes EVG/PPG/EN
        infer_schema_length=10000,
    )
    
    df = h.normalize_schema(df, h.SHIFT_SCHEMA)

    df = df.select(
                pl.col("gameId").cast(pl.Utf8),
                pl.col("playerId").cast(pl.Utf8),
                pl.col("startTime"),
                pl.col("endTime"),
                pl.col("duration"),
                pl.col("period"),
                pl.col("shiftNumber"),
                pl.col("teamId").cast(pl.Utf8),
                pl.col("eventDescription"),
    )

    return df 

def call_shift_html(gameId: str, team:str) -> pl.DataFrame: 
    seasonId = h.get_season_id(gameId)

    url = f"https://www.nhl.com/scores/htmlreports/{seasonId}/T{team}{gameId[-6:]}.HTM"

    response = requests.get(url)

    response.raise_for_status() 

    return response.text


# Players
def call_players_api(season: str) -> pl.DataFrame:
    url = f"https://api.nhle.com/stats/rest/en/skater/summary?limit=-1&sort=points&cayenneExp=seasonId={season}"


    response = requests.get(url)
    response.raise_for_status()
    json_data = response.json()

    df = pl.DataFrame(json_data.get("data", []))

    return df

def call_goalies_api(season: str) -> pl.DataFrame:
    url = f"https://api.nhle.com/stats/rest/en/goalie/summary?limit=-1&sort=wins&cayenneExp=seasonId={season}"


    response = requests.get(url)
    response.raise_for_status()
    json_data = response.json()

    df = pl.DataFrame(json_data.get("data", []))

    return df


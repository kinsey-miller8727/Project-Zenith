from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse, parse_qs, urlunparse
from datetime import date
from pathlib import Path
from bs4 import BeautifulSoup

import requests
import polars as pl
import json

import vars
import api
#========================================================================================================
# Simple Helpers
#========================================================================================================



#========================================================================================================
# time converstion
def mmss_to_sec(expr: pl.Expr) -> pl.Expr:
    parts = expr.str.split(":")
    return parts.list.get(0).cast(pl.Int64) * 60 + parts.list.get(1).cast(pl.Int64)

def sec_to_period_mmss(abs_sec: pl.Expr) -> tuple[pl.Expr, pl.Expr]:
    period = (abs_sec // (20 * 60) + 1).cast(pl.Int64)
    sec_in_period = (abs_sec % (20 * 60)).cast(pl.Int64)

    mm = (sec_in_period // 60).cast(pl.Int64).cast(pl.Utf8).str.zfill(2)
    ss = (sec_in_period % 60).cast(pl.Int64).cast(pl.Utf8).str.zfill(2)

    mmss = mm + pl.lit(":") + ss
    return period, mmss

def time_mmss_to_seconds(expr: pl.Expr) -> pl.Expr:
    """
    Convert 'MM:SS' string column to integer seconds.
    """
    mins = expr.str.slice(0, 2).cast(pl.Int64)
    secs = expr.str.slice(3, 2).cast(pl.Int64)
    return (mins * 60 + secs)

#========================================================================================================
# ad hoc functions
def get_season_id(gameId: str) -> str:
    start_year = int(gameId[:4])
    return f"{start_year}{start_year + 1}"

def pull_live_game(triCode, seasonId) -> pl.DataFrame:
    df = api.call_schedule_api(triCode,seasonId)
    df = df.filter(pl.col("gameState") == "LIVE")
    
    return df

def single_game_shifts(gameId: str, playersAll: pl.DataFrame | None = None) -> pl.DataFrame:
    seasonId = get_season_id(gameId)

    if playersAll is None:
        playersAll = (
            api.call_players_api(seasonId)
            .with_columns(pl.col("playerId").cast(pl.Utf8))
        )

    shifts = api.call_shift_api(gameId)

    shifts = (
        shifts
        .with_columns(
            pl.col("startTime").rank("dense").over(["gameId", "playerId", "period"]).alias("shift"),
            (pl.col("period") + pl.col("shiftNumber") / 100).alias("shiftId"),
            pl.col("playerId").cast(pl.Utf8),
        )
        .join(playersAll, on="playerId", how="left")
        .filter(pl.col("skaterFullName") != "")
    )

    out = (
        shifts
        .with_columns(
            pl.col("gameId").cast(pl.Utf8),
            pl.col("playerId").cast(pl.Utf8),
            pl.col("teamId").cast(pl.Utf8),

            pl.col("period").cast(pl.Int64),
            pl.col("shiftNumber").cast(pl.Int64),

            startSec=mmss_to_sec(pl.col("startTime")).cast(pl.Int64),
            endSec=mmss_to_sec(pl.col("endTime")).cast(pl.Int64),
        )
        .with_columns(
            absStart=((pl.col("period") - 1) * 20 * 60 + pl.col("startSec")).cast(pl.Int64),
            absEnd=((pl.col("period") - 1) * 20 * 60 + pl.col("endSec")).cast(pl.Int64),
        )
        .filter(pl.col("absEnd") > pl.col("absStart"))
        .with_columns(
            durationSec=(pl.col("absEnd") - pl.col("absStart")).cast(pl.Int64)
        )
        .drop([c for c in ["duration", "shiftId", "eventDescription", "shift"] if c in shifts.columns])
    )

    keepCols = [
        "gameId", "playerId", "teamId",
        "period", "shiftNumber",
        "startTime", "endTime",
        "startSec", "endSec",
        "absStart", "absEnd",
        "durationSec",
        "roleCode",
    ]

    return out.select([c for c in keepCols if c in out.columns])

def empty_df(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame({k: pl.Series([], dtype=v) for k, v in schema.items()})

def add_shift_strength(
    shifts: pl.DataFrame,
    *,
    game_col: str = "gameId",
    team_col: str = "teamId",
    player_col: str = "playerId",
    start_abs_col: str = "absStart",
    end_abs_col: str = "absEnd",
) -> pl.DataFrame:
    """
    Optimized strength assignment using change-points (no per-second explode).
    Assumes shifts includes only skaters (no goalies available in schema).

    Adds:
      - team_skaters, opp_skaters
      - strength (e.g. "5v5", "5v4", "4v5", "3v3")
      - empty_net_for / empty_net_against (always False here)
    """

    df = shifts.with_columns(
        pl.col(game_col).cast(pl.Utf8),
        pl.col(team_col).cast(pl.Utf8),
        pl.col(player_col).cast(pl.Utf8),
        pl.col(start_abs_col).cast(pl.Int64),
        pl.col(end_abs_col).cast(pl.Int64),
    )

    # --- teams per game (expects exactly 2 teams) ---
    teams_per_game = (
        df.select([game_col, team_col])
        .unique()
        .group_by(game_col)
        .agg(pl.col(team_col).sort().alias("teams"))
        .filter(pl.col("teams").list.len() == 2)
        .with_columns(
            pl.col("teams").list.get(0).alias("teamA"),
            pl.col("teams").list.get(1).alias("teamB"),
        )
        .select([game_col, "teamA", "teamB"])
    )

    # --- change points: +1 at start, -1 at end (skaters only) ---
    deltas = pl.concat(
        [
            df.select([game_col, team_col, start_abs_col]).rename({start_abs_col: "t"}).with_columns(pl.lit(1).alias("delta")),
            df.select([game_col, team_col, end_abs_col]).rename({end_abs_col: "t"}).with_columns(pl.lit(-1).alias("delta")),
        ],
        how="vertical",
    )

    # collapse multiple deltas at same time
    deltas = (
        deltas.group_by([game_col, team_col, "t"])
        .agg(pl.col("delta").sum().alias("delta"))
        .sort([game_col, team_col, "t"])
        .with_columns(
            pl.col("delta").cum_sum().over([game_col, team_col]).alias("team_skaters")
        )
        .select([game_col, team_col, "t", "team_skaters"])
    )

    # --- game-level interval boundaries (union of all change points) ---
    times = (
        deltas.select([game_col, "t"])
        .unique()
        .sort([game_col, "t"])
        .with_columns(
            pl.col("t").shift(-1).over(game_col).alias("t_next")
        )
        .filter(pl.col("t_next").is_not_null())
        .with_columns((pl.col("t_next") - pl.col("t")).alias("seg_dur"))
        .filter(pl.col("seg_dur") > 0)
        .select([game_col, "t", "t_next", "seg_dur"])
    )

    # --- expand intervals to both teams in game ---
    segs_long = (
        times.join(teams_per_game, on=game_col, how="inner")
        .with_columns(pl.col("teamA").alias("team_list"))
        .select([game_col, "t", "t_next", "seg_dur", "teamA", "teamB"])
        .with_columns(pl.concat_list([pl.col("teamA"), pl.col("teamB")]).alias("team_list"))
        .explode("team_list")
        .rename({"team_list": team_col})
        .with_columns(
            pl.when(pl.col(team_col) == pl.col("teamA"))
            .then(pl.col("teamB"))
            .otherwise(pl.col("teamA"))
            .alias("oppTeamId")
        )
        .select([game_col, "t", "t_next", "seg_dur", team_col, "oppTeamId"])
        .sort([game_col, team_col, "t"])
    )

    # --- asof join to get team skater count at segment start t ---
    # Need deltas sorted by (game, team, t) for join_asof
    deltas_sorted = deltas.sort([game_col, team_col, "t"])

    segs_long = (
        segs_long.join_asof(
            deltas_sorted,
            left_on="t",
            right_on="t",
            by=[game_col, team_col],
            strategy="backward",
        )
        .with_columns(pl.col("team_skaters").fill_null(0))
        .rename({"team_skaters": "team_skaters"})
    )

    # --- attach opponent skaters via another asof join (same t, different team) ---
    opp_counts = deltas_sorted.rename({team_col: "oppTeamId", "team_skaters": "opp_skaters"})

    segs_long = (
        segs_long.join_asof(
            opp_counts,
            left_on="t",
            right_on="t",
            by=[game_col, "oppTeamId"],
            strategy="backward",
        )
        .with_columns(pl.col("opp_skaters").fill_null(0))
        .with_columns(
            (
                pl.col("team_skaters").cast(pl.Utf8)
                + pl.lit("v")
                + pl.col("opp_skaters").cast(pl.Utf8)
            ).alias("strength")
        )
        .select([game_col, team_col, "t", "t_next", "seg_dur", "team_skaters", "opp_skaters", "strength"])
    )

    # --- assign each shift the strength with max overlap duration ---
    shifts_keyed = df.with_row_index("shift_row_id")

    # cartesian within (game, team), then filter to overlaps
    overlaps = (
        shifts_keyed.join(segs_long, on=[game_col, team_col], how="inner")
        .filter((pl.col("t") < pl.col(end_abs_col)) & (pl.col("t_next") > pl.col(start_abs_col)))
        .with_columns(
            (
                pl.min_horizontal(pl.col(end_abs_col), pl.col("t_next"))
                - pl.max_horizontal(pl.col(start_abs_col), pl.col("t"))
            ).alias("overlap_dur")
        )
        .filter(pl.col("overlap_dur") > 0)
    )

    best_strength = (
        overlaps.sort(["shift_row_id", "overlap_dur"], descending=[False, True])
        .group_by("shift_row_id")
        .agg(
            pl.first("team_skaters").alias("team_skaters"),
            pl.first("opp_skaters").alias("opp_skaters"),
            pl.first("strength").alias("strength"),
        )
    )

    out = (
        shifts_keyed.join(best_strength, on="shift_row_id", how="left")
        .drop("shift_row_id")
    )

    return out

#========================================================================================================
# normalizing play by play tables before joining

PBP_SCHEMA: dict[str, pl.DataType] = {
    "eventId": pl.Utf8,
    "periodNumber": pl.Int64,
    "timeInPeriod": pl.Utf8,
    "timeRemaining": pl.Utf8,
    "situationCode": pl.Utf8,
    "homeTeamDefendingSide": pl.Utf8,
    "typeCode": pl.Int64,
    "typeDescKey": pl.Utf8,
    "sortOrder": pl.Int64,
    "eventOwnerTeamId": pl.Utf8,
    "losingPlayerId": pl.Utf8,
    "winningPlayerId": pl.Utf8,
    "xCoord": pl.Int64,
    "yCoord": pl.Int64,
    "zoneCode": pl.Utf8,
    "shotType": pl.Utf8,
    "shootingPlayerId": pl.Utf8,
    "goalieInNetId": pl.Utf8,
    "awaySOG": pl.Int64,
    "homeSOG": pl.Int64,
    "reason": pl.Utf8,
    "playerId": pl.Utf8,
    "blockingPlayerId": pl.Utf8,
    "scoringPlayerId": pl.Utf8,
    "scoringPlayerTotal": pl.Utf8,
    "assist1PlayerId": pl.Utf8,
    "assist1PlayerTotal": pl.Utf8,
    "assist2PlayerId": pl.Utf8,
    "assist2PlayerTotal": pl.Utf8,
    "awayScore": pl.Int64,
    "homeScore": pl.Int64,
    "hittingPlayerId": pl.Utf8,
    "hitteePlayerId": pl.Utf8,
    "secondaryReason": pl.Utf8,
    "details_typeCode": pl.Utf8,
    "descKey": pl.Utf8,
    "duration": pl.Int64,
    "committedByPlayerId": pl.Utf8,
    "drawnByPlayerId": pl.Utf8,
    "gameId": pl.Utf8,
    "awayTeamId": pl.Utf8,
    "homeTeamId": pl.Utf8,
}

PBP_COLS = list(PBP_SCHEMA.keys())

def coerce_to_schema(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """
    Ensure df has all columns in schema with correct dtypes.
    - Missing columns are added as typed nulls
    - Existing columns are cast to expected dtype (non-strict to avoid hard fails)
    - Extra columns are dropped
    - Columns are ordered to schema order
    """
    # add missing columns as typed nulls
    missing = [c for c in schema.keys() if c not in df.columns]
    if missing:
        df = df.with_columns([pl.lit(None, dtype=schema[c]).alias(c) for c in missing])

    # cast expected columns that exist
    df = df.with_columns(
        [pl.col(c).cast(schema[c], strict=False).alias(c) for c in schema.keys()]
    )

    # drop extras + order consistently
    return df.select(list(schema.keys()))

#========================================================================================================
# normalizing shift tables before joining
def normalize_schema(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
        # add missing cols as nulls with correct dtype
        for col, dtype in schema.items():
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))

        # cast existing cols (strict=False prevents hard failures; bad casts -> null)
        df = df.with_columns([
            pl.col(col).cast(dtype, strict=False)
            for col, dtype in schema.items()
            if col in df.columns
        ])

        return df.select(list(schema.keys()))

SHIFT_SCHEMA = {
        "id": pl.Int64,
        "gameId": pl.Int64,
        "playerId": pl.Int64,
        "period": pl.Int64,
        "shiftNumber": pl.Int64,

        "startTime": pl.Utf8,
        "endTime": pl.Utf8,
        "duration": pl.Utf8,

        # troublemakers: keep as text
        "detailCode": pl.Utf8,
        "typeCode": pl.Utf8,
        "teamId": pl.Utf8,
        "teamAbbrev": pl.Utf8,

        "teamName": pl.Utf8,
        "firstName": pl.Utf8,
        "lastName": pl.Utf8,

        "eventNumber": pl.Int64,
        "eventDescription": pl.Utf8,
        "eventDetails": pl.Utf8,
        "hexValue": pl.Utf8,
    }

#helper for html pull of shifts
def parse_shift_report_html(html: str, *, debug: bool = False) -> pl.DataFrame:
    soup = BeautifulSoup(html, "html.parser")

    teamName = soup.find("td", class_=["teamHeading"]).text.strip().title()

    rows = []
    current_player = None

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        # 1) Player header row: <td class="playerHeading ...">6 LARSSON, ADAM</td>
        player_td = tr.find("td", class_=lambda c: c and "playerHeading" in c)
        if player_td is not None:
            current_player = player_td.get_text(" ", strip=True).title()
            if debug:
                print("PLAYER:", current_player)
            continue

        # 2) Skip the column header row (Shift #, Per, Start of Shift, ...)
        first_txt = tds[0].get_text(" ", strip=True)
        if first_txt.lower().startswith("shift"):
            continue

        # 3) Shift rows look like 6 tds:
        # [shift#, per, start, end, duration, event]
        if len(tds) == 6 and current_player:
            shift_txt = tds[0].get_text(" ", strip=True)
            per_txt = tds[1].get_text(" ", strip=True)

            # must be numeric shift# and period
            if shift_txt.isdigit() and per_txt.isdigit():
                start_txt = tds[2].get_text(" ", strip=True)
                end_txt = tds[3].get_text(" ", strip=True)
                dur_txt = tds[4].get_text(" ", strip=True)

                # event cell sometimes is &nbsp; (becomes '' after strip)
                event_txt = tds[5].get_text(" ", strip=True) if len(tds) > 5 else ""
                event_txt = event_txt if event_txt else None

                rows.append(
                    {
                        "player": current_player,
                        "shiftNumber": int(shift_txt),
                        "period": int(per_txt),
                        "startOfShift": start_txt,   
                        "endOfShift": end_txt,       
                    }
                )

    df = pl.DataFrame(rows)

    df = df.with_columns(pl.lit(teamName).alias("teamName"))

    return df 
#========================================================================================================

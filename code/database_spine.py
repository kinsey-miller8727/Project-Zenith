from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse, parse_qs, urlunparse
from datetime import date

import requests
import polars as pl

import api
import helpers as h
import vars

#====================================================================================
# Database Spine Tables
#====================================================================================

# filtering only to games that have happened
def db_games(schedTables, seasonId, gameType) -> pl.DataFrame:
    dfs = []

    for row in schedTables.iter_rows(named=True):
        df = api.call_schedule_api(row['triCode'],seasonId)
        dfs.append(df)

    sched_output = pl.concat(dfs, how="diagonal")

    if gameType > -1:
        sched_output = sched_output.filter(pl.col("gameType") == gameType)

    # de-duplicate games pulled from multiple teams' schedules
    games = sched_output.unique(subset=["gameId"])

    games = (
        games
        .with_columns(
            pl.col("startTimeUTC")
            .str.slice(0, 10)
            .alias("gameDate")
        )
        .filter(pl.col("gameState") != "FUT")
    )
    return games

def db_teams(url: str = "https://api-web.nhle.com/v1/standings/now") -> pl.DataFrame:
    """
    Pull NHL standings snapshot and return a lightweight team lookup table with:
      - conferenceAbbrev
      - divisionAbbrev
      - teamName
      - teamAbbrev
      - teamLogo
    """
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    standings = data.get("standings", [])
    df = pl.DataFrame(standings)

    out = (
        df.select([
            pl.col("conferenceAbbrev"),
            pl.col("divisionAbbrev"),
            pl.col("teamName").struct.field("default").alias("teamName"),
            pl.col("teamAbbrev").struct.field("default").alias("teamAbbrev"),
            pl.col("teamLogo"),
        ])
        .unique(subset=["teamAbbrev"])  # should already be unique, but safe
        .sort(["conferenceAbbrev", "divisionAbbrev", "teamAbbrev"])
    )

    # pull games from last full season to get teamTds
    season_table = pl.DataFrame({
            "triCode": ["MIN"],
        }
    )
    games = db_games(season_table, vars.last_season, -1)

    teams_from_games = (
        pl.concat([
            games.select([pl.col("homeTeamId").alias("teamId"), pl.col("homeTeamAbbrev").alias("teamAbbrev")]),
            games.select([pl.col("awayTeamId").alias("teamId"), pl.col("awayTeamAbbrev").alias("teamAbbrev")]),
        ])
        .unique(subset=["teamAbbrev"])
    )
    
    out = (
        out
        .join(teams_from_games, on="teamAbbrev", how="left")
        .select("teamId","teamAbbrev","teamName","conferenceAbbrev","divisionAbbrev","teamLogo")
    )

    return out

def db_players(seasonId) -> pl.DataFrame:
    players = api.call_players_api(seasonId)
    goalies = api.call_goalies_api(seasonId)

    # filter players to those in the shift list
    players = (
        players
        .with_columns(
            pl.col("playerId").cast(pl.Utf8),
        )
        .select(
            pl.col("playerId").cast(pl.Utf8),
            pl.col("skaterFullName"),
            pl.col("positionCode"),
            pl.col("shootsCatches"),
        )
        .with_columns(
            pl.when(pl.col("positionCode")=='D')
            .then(pl.lit("D"))
            .otherwise(pl.lit("F"))
            .alias("roleCode")
        )
    )
    goalies = (
        goalies
        .with_columns(
            pl.col("playerId").cast(pl.Utf8),
        )
        .select(
            pl.col("playerId").cast(pl.Utf8),
            pl.col("goalieFullName").alias("skaterFullName"),
            pl.col("shootsCatches"),
        )
        .with_columns(
            pl.lit("G").alias("positionCode"),
            pl.lit("G").alias("roleCode"),
        )
        .select("playerId", "skaterFullName", "positionCode", "shootsCatches", "roleCode")
    )

    return pl.concat([players, goalies], how="vertical")

def db_pbp(games: pl.DataFrame) -> pl.DataFrame:
    gameIds = (
        games
        .select(pl.col("gameId"))
        .unique()
        .get_column("gameId")
        .to_list()
    )

    dfs: list[pl.DataFrame] = []
    for gid in gameIds:
        try:
            pbp = api.call_play_by_play_api(gid)

            # ensure gameId is present (and correct type) even if the API didn't include it
            if "gameId" not in pbp.columns:
                pbp = pbp.with_columns(pl.lit(str(gid), dtype=pl.Utf8).alias("gameId"))
            else:
                pbp = pbp.with_columns(pl.col("gameId").cast(pl.Utf8, strict=False))

            pbp = h.coerce_to_schema(pbp, h.PBP_SCHEMA)
            dfs.append(pbp)

        except Exception as e:
            print(f"Failed game {gid}: {e}")

    if not dfs:
        return pl.DataFrame(schema=h.PBP_SCHEMA)

    return pl.concat(dfs, how="vertical")

def db_shifts(games, players, teams) -> pl.DataFrame:
    gameIds = (
        games
        .select(pl.col("gameId").cast(pl.Utf8).unique())
        .get_column("gameId")
        .to_list()
    )

    playersCache: dict[str, pl.DataFrame] = {}
    dfs: list[pl.DataFrame] = []

    def _is_blank_shift_report(df: pl.DataFrame) -> bool:
        if df is None:
            return True
        if df.height == 0:
            return True
        # optional: treat "all nulls" as blank
        try:
            return df.select(pl.all().null_count().sum()).item() == df.height * df.width
        except Exception:
            return False

    for gameId in gameIds:
        try:
            # --- normal path ---
            seasonId = h.get_season_id(gameId)

            if seasonId not in playersCache:
                playersCache[seasonId] = (
                    api.call_players_api(seasonId)
                    .with_columns(pl.col("playerId").cast(pl.Utf8))
                )

            df_game = h.single_game_shifts(gameId, playersAll=playersCache[seasonId])

            # --- fallback if blank ---
            if _is_blank_shift_report(df_game):
                df_game = db_shift_html(gameId, players, teams)

            dfs.append(df_game)

        except Exception as e:
            # if anything blows up, still try html fallback before giving up
            try:
                df_game = db_shift_html(gameId, players, teams)
                dfs.append(df_game)
            except Exception as e2:
                print(f"shifts failed for gameId={gameId}: api_err={e} | html_err={e2}")

    if not dfs:
        return pl.DataFrame()

    shifts = pl.concat(dfs, how="diagonal")

    # Optimized strength (uses absStart/absEnd, no goalies)
    shifts = h.add_shift_strength(
        shifts,
        game_col="gameId",
        team_col="teamId",
        player_col="playerId",
        start_abs_col="absStart",
        end_abs_col="absEnd",
    )

    return shifts

def db_shift_html(
    gameId: str,
    players: pl.DataFrame,
    teams: pl.DataFrame,
) -> pl.DataFrame:
    

    df = pl.DataFrame(
        schema={
            "player": pl.Utf8,
            "shiftNumber": pl.Int64,
            "period": pl.Int64,
            "startOfShift": pl.Utf8,
            "endOfShift": pl.Utf8,
            "teamName": pl.Utf8,
        }
    )


    html_home = api.call_shift_html(gameId,"H")
    df_home = h.parse_shift_report_html(html_home, debug=False)

    html_away = api.call_shift_html(gameId,"V")
    df_away = h.parse_shift_report_html(html_away, debug=False)

    df_combined = pl.concat([df_home, df_away], how="vertical")
    
    df_combined = (
        df_combined
        .with_columns(
            pl.lit(gameId).alias("gameId"),
            (pl.col("player").str.split(by=" ").list.get(2)+" "+pl.col("player").str.split(by=" ").list.get(1)).str.replace(",", "").alias("skaterFullName"),
            pl.col("startOfShift").str.split(by=" ").list.get(0).alias("startTime"),
            pl.col("endOfShift").str.split(by=" ").list.get(0).alias("endTime"),
        )
        .with_columns(
            startSec=h.mmss_to_sec(pl.col("startTime")).cast(pl.Int64),
            endSec=h.mmss_to_sec(pl.col("endTime")).cast(pl.Int64),
        )
        .with_columns(
            absStart=((pl.col("period") - 1) * 20 * 60 + pl.col("startSec")).cast(pl.Int64),
            absEnd=((pl.col("period") - 1) * 20 * 60 + pl.col("endSec")).cast(pl.Int64),
        )
        .with_columns(
            durationSec=(pl.col("absEnd") - pl.col("absStart")).cast(pl.Int64),
        )
        .join(players.select("skaterFullName", "playerId", "roleCode"), on="skaterFullName", how="left")
        .join(teams.select("teamName", "teamId"), on="teamName", how="left")
    )

    df_combined = (
        df_combined
        .filter(pl.col("roleCode") != "G")
    )

    df_combined = (df_combined.select("gameId", "playerId", "teamId", "period", "shiftNumber", "startTime", "endTime", "startSec", "endSec", "absStart", "absEnd", "durationSec"))

    return df_combined

def db_eventOnIce(pbp: pl.DataFrame, shifts: pl.DataFrame) -> pl.DataFrame:
    """
    Build normalized eventOnIce bridge table.

    Input:
      pbp: event-grain table containing at least
           ["gameId", "eventId", "periodNumber", "timeInPeriod"]
      shifts: shift-grain table containing at least
           ["gameId", "playerId", "teamId", "absStart", "absEnd"]
           (and optionally "roleCode" to drop goalies)

    Output (grain: gameId-eventId-playerId):
      ["gameId", "eventId", "playerId", "teamId"]
    """

    # --- event seconds (absolute) ---
    pbpEvents = (
        pbp
        .with_columns(
            pl.col("gameId").cast(pl.Utf8),
            pl.col("eventId").cast(pl.Utf8),
            eventSec=((pl.col("periodNumber") - 1) * 20 * 60 + h.mmss_to_sec(pl.col("timeInPeriod"))).cast(pl.Int64),
        )
        .select(["gameId", "eventId", "eventSec"])
        .unique(subset=["gameId", "eventId"])
    )

    # --- shift intervals (absolute) ---
    shiftsClean = (
        shifts
        .with_columns(
            pl.col("gameId").cast(pl.Utf8),
            pl.col("playerId").cast(pl.Utf8),
            pl.col("teamId").cast(pl.Utf8),
            pl.col("absStart").cast(pl.Int64),
            pl.col("absEnd").cast(pl.Int64),
        )
        .filter(pl.col("absEnd") > pl.col("absStart"))
        .select(["gameId", "teamId", "playerId", "absStart", "absEnd"] + (["roleCode"] if "roleCode" in shifts.columns else []))
    )

    # Drop goalies if roleCode is available
    if "roleCode" in shiftsClean.columns:
        shiftsClean = shiftsClean.filter(pl.col("roleCode") != "G").drop("roleCode")

    # --- interval match: absStart <= eventSec < absEnd ---
    eventOnIce = (
        pbpEvents
        .join(shiftsClean, on="gameId", how="inner")
        .filter((pl.col("absStart") < pl.col("eventSec")) & (pl.col("eventSec") <= pl.col("absEnd")))
        .select(["gameId", "eventId", "playerId", "teamId"])
        .unique()
        .sort(["gameId", "eventId", "teamId", "playerId"])
    )

    return eventOnIce

def db_eventStrength(eventOnIce: pl.DataFrame, pbp: pl.DataFrame,) -> pl.DataFrame:
    """
    Build eventStrength table.

    Inputs:
      eventOnIce: columns ["gameId","eventId","playerId","teamId"] (goalies excluded upstream)
      pbp: must contain ["gameId","eventId","homeTeamId","awayTeamId"]

    Output grain: (gameId, eventId)
    Columns:
      gameId, eventId,
      homeSkaters, awaySkaters,
      strengthState,
      is5v5, is5v4, is4v5, is4v4, is3v3,
      is6v5, is5v6, is6v4, is4v6, is6v6,
      homeEmptyNet, awayEmptyNet, isEmptyNet
    """

    eventTeams = (
        pbp
        .select(["gameId", "eventId", "homeTeamId", "awayTeamId"])
        .with_columns(
            pl.col("gameId").cast(pl.Utf8),
            pl.col("eventId").cast(pl.Utf8),
            pl.col("homeTeamId").cast(pl.Utf8),
            pl.col("awayTeamId").cast(pl.Utf8),
        )
        .unique(subset=["gameId", "eventId"])
    )

    counts = (
        eventOnIce
        .select(["gameId", "eventId", "teamId", "playerId"])
        .with_columns(
            pl.col("gameId").cast(pl.Utf8),
            pl.col("eventId").cast(pl.Utf8),
            pl.col("teamId").cast(pl.Utf8),
            pl.col("playerId").cast(pl.Utf8),
        )
        .unique(subset=["gameId", "eventId", "teamId", "playerId"])
        .group_by(["gameId", "eventId", "teamId"])
        .agg(homeAwaySkaters=pl.n_unique("playerId").cast(pl.Int64).alias("skaters"))
        .rename({"homeAwaySkaters": "skaters"})
    )

    # attach home/away + pivot to homeSkaters/awaySkaters
    out = (
        eventTeams
        .join(counts, on=["gameId", "eventId"], how="left")
        .with_columns(
            side=pl.when(pl.col("teamId") == pl.col("homeTeamId"))
                  .then(pl.lit("home"))
                  .when(pl.col("teamId") == pl.col("awayTeamId"))
                  .then(pl.lit("away"))
                  .otherwise(pl.lit(None)),
        )
        .filter(pl.col("side").is_not_null())
        .select(["gameId", "eventId", "side", "skaters"])
        .pivot(
            values="skaters",
            index=["gameId", "eventId"],
            on="side",
        )
        .rename({"home": "homeSkaters", "away": "awaySkaters"})
        .with_columns(
            pl.col("homeSkaters").fill_null(0).cast(pl.Int64),
            pl.col("awaySkaters").fill_null(0).cast(pl.Int64),
        )
        .with_columns(
            strengthState=pl.format("{}v{}", pl.col("homeSkaters"), pl.col("awaySkaters")),

            # common even/PP states
            is5v5=((pl.col("homeSkaters") == 5) & (pl.col("awaySkaters") == 5)).cast(pl.Int8),
            is5v4=((pl.col("homeSkaters") == 5) & (pl.col("awaySkaters") == 4)).cast(pl.Int8),
            is4v5=((pl.col("homeSkaters") == 4) & (pl.col("awaySkaters") == 5)).cast(pl.Int8),
            is4v4=((pl.col("homeSkaters") == 4) & (pl.col("awaySkaters") == 4)).cast(pl.Int8),
            is3v3=((pl.col("homeSkaters") == 3) & (pl.col("awaySkaters") == 3)).cast(pl.Int8),

            # pulled-goalie / extra-attacker states
            is6v5=((pl.col("homeSkaters") == 6) & (pl.col("awaySkaters") == 5)).cast(pl.Int8),
            is5v6=((pl.col("homeSkaters") == 5) & (pl.col("awaySkaters") == 6)).cast(pl.Int8),
            is6v4=((pl.col("homeSkaters") == 6) & (pl.col("awaySkaters") == 4)).cast(pl.Int8),
            is4v6=((pl.col("homeSkaters") == 4) & (pl.col("awaySkaters") == 6)).cast(pl.Int8),
            is6v6=((pl.col("homeSkaters") == 6) & (pl.col("awaySkaters") == 6)).cast(pl.Int8),

            # empty net flags (approximation via skater count)
            homeEmptyNet=(pl.col("homeSkaters") > 5).cast(pl.Int8),
            awayEmptyNet=(pl.col("awaySkaters") > 5).cast(pl.Int8),
        )
        .with_columns(
            isEmptyNet=(pl.col("homeEmptyNet") | pl.col("awayEmptyNet")).cast(pl.Int8)
        )
        .sort(["gameId", "eventId"])
    )

    return out

def db_eventTeamFlags(pbp: pl.DataFrame, shifts: pl.DataFrame) -> pl.DataFrame:
    """
    Build eventTeamFlags table (NST-style attribution).

    Grain: (gameId, eventId, teamId)

    Output columns:
      gameId, eventId, teamId,
      GF, GA, CF, CA, FF, FA

    Attribution:
      - FOR team for shot attempts/goals = shooter's team
        (shootingPlayerId -> shifts playerId/teamId mapping)
      - fallback to eventOwnerTeamId when shooter team missing
    """

    shotEvents = ["shot-on-goal", "goal", "missed-shot", "blocked-shot"]
    fenwickEvents = ["shot-on-goal", "goal", "missed-shot"]

    # Map playerId -> teamId (within game) from shifts
    playerTeam = (
        shifts
        .select(["gameId", "playerId", "teamId"])
        .with_columns(
            pl.col("gameId").cast(pl.Utf8),
            pl.col("playerId").cast(pl.Utf8),
            pl.col("teamId").cast(pl.Utf8),
        )
        .unique(subset=["gameId", "playerId"])
    )

    # Minimal pbp view with shooter team attached
    pbp2 = (
        pbp
        .select(["gameId", "eventId", "typeDescKey", "eventOwnerTeamId", "shootingPlayerId", "homeTeamId", "awayTeamId"])
        .with_columns(
            pl.col("gameId").cast(pl.Utf8),
            pl.col("eventId").cast(pl.Utf8),
            pl.col("eventOwnerTeamId").cast(pl.Utf8),
            pl.col("shootingPlayerId").cast(pl.Utf8),
            pl.col("homeTeamId").cast(pl.Utf8),
            pl.col("awayTeamId").cast(pl.Utf8),
        )
        .join(
            playerTeam.rename({"playerId": "shootingPlayerId", "teamId": "shooterTeamId"}),
            on=["gameId", "shootingPlayerId"],
            how="left",
        )
        .with_columns(
            shotForTeamId=pl.coalesce([pl.col("shooterTeamId"), pl.col("eventOwnerTeamId")])
        )
        .select(["gameId", "eventId", "typeDescKey", "shotForTeamId", "homeTeamId", "awayTeamId"])
    )

    # Build two rows per event: home+away
    homeRows = pbp2.select(["gameId", "eventId", "typeDescKey", "shotForTeamId", "homeTeamId"]).rename({"homeTeamId": "teamId"})
    awayRows = pbp2.select(["gameId", "eventId", "typeDescKey", "shotForTeamId", "awayTeamId"]).rename({"awayTeamId": "teamId"})

    et = pl.concat([homeRows, awayRows], how="vertical")

    et = (
        et
        .with_columns(
            isGoal=(pl.col("typeDescKey") == "goal").cast(pl.Int8),
            isCorsiEvent=pl.col("typeDescKey").is_in(shotEvents).cast(pl.Int8),
            isFenwickEvent=pl.col("typeDescKey").is_in(fenwickEvents).cast(pl.Int8),
            isFor=(pl.col("teamId") == pl.col("shotForTeamId")).cast(pl.Int8),
        )
        .with_columns(
            GF=(pl.col("isGoal") & (pl.col("isFor") == 1)).cast(pl.Int8),
            GA=(pl.col("isGoal") & (pl.col("isFor") == 0)).cast(pl.Int8),
            CF=(pl.col("isCorsiEvent") & (pl.col("isFor") == 1)).cast(pl.Int8),
            CA=(pl.col("isCorsiEvent") & (pl.col("isFor") == 0)).cast(pl.Int8),
            FF=(pl.col("isFenwickEvent") & (pl.col("isFor") == 1)).cast(pl.Int8),
            FA=(pl.col("isFenwickEvent") & (pl.col("isFor") == 0)).cast(pl.Int8),
        )
        .select(["gameId", "eventId", "teamId", "GF", "GA", "CF", "CA", "FF", "FA"])
    )

    return et



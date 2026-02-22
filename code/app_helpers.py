import os
import streamlit as st
import polars as pl
import pandas as pd
import plotly.express as px
import math
import base64
from io import BytesIO
import base64
from pathlib import Path
import matplotlib.pyplot as plt

from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode



RINK_X_MIN, RINK_X_MAX = -100, 100
RINK_Y_MIN, RINK_Y_MAX = -42.5, 42.5
MARGIN = 1  # feet; tune this

THEME = {
    "primary": "#2F3A4A",      
    "secondary": "#2A4D72",    
    "background": "#F7F8FA",
    "border": "rgba(47, 58, 74, 0.25)",
    "border_light": "rgba(47, 58, 74, 0.12)",
    "text_primary": "#1F2933",
    "text_secondary": "#6B7280",
    "positive": "#2E8B57",
    "negative": "#C94C4C",
    "header_bg": "rgba(47, 58, 74, 0.06)",
    "final_row": "#EDEFF2",
}

TEAM_COLORS = {
    "ANA": "#F47A38",
    "BOS": "#FFB81C",
    "BUF": "#003087",
    "CAR": "#CC0000",
    "CBJ": "#002654",
    "CGY": "#C8102E",
    "CHI": "#CF0A2C",
    "COL": "#6F263D",
    "DAL": "#006847",
    "DET": "#CE1126",
    "EDM": "#FF4C00",
    "FLA": "#C8102E",
    "LAK": "#111111",
    "MIN": "#154734",
    "MTL": "#AF1E2D",
    "NJD": "#CE1126",
    "NSH": "#FFB81C",
    "NYI": "#00539B",
    "NYR": "#0038A8",
    "OTT": "#C52032",
    "PHI": "#F74902",
    "PIT": "#FFB81C",
    "SEA": "#001628",
    "SJS": "#006D75",
    "STL": "#002F87",
    "TBL": "#002868",
    "TOR": "#00205B",
    "VAN": "#00205B",
    "VGK": "#B4975A",
    "WSH": "#041E42",
    "WPG": "#041E42",
}

TEAM_ALT_COLORS = {
    "ANA": "#F47A38",  # orange
    "BOS": "#000000",  # black
    "BUF": "#ADB2B7",  # silver
    "CGY": "#F1BE48",  # gold
    "CAR": "#000000",  # black
    "CHI": "#000000",  # black
    "COL": "#236192",  # steel blue
    "CBJ": "#A4A9AD",  # silver
    "DAL": "#8F8F8C",  # silver
    "DET": "#000000",  # black
    "EDM": "#041E42",  # navy
    "FLA": "#041E42",  # navy
    "LAK": "#A2AAAD",  # silver
    "MIN": "#A6192E",  # red
    "MTL": "#192168",  # blue
    "NSH": "#041E42",  # navy
    "NJD": "#000000",  # black
    "NYI": "#F47D30",  # orange
    "NYR": "#C8102E",  # red
    "OTT": "#B79257",  # gold
    "PHI": "#000000",  # black
    "PIT": "#000000",  # black
    "SEA": "#99D9D9",  # ice blue
    "SJS": "#EA7200",  # orange
    "STL": "#FCB514",  # gold
    "TBL": "#000000",  # black
    "TOR": "#558AEC",  # light blue**
    "VAN": "#00843D",  # green
    "VGK": "#B4975A",  # gold
    "WSH": "#C8102E",  # red
    "WPG": "#7B303E",  # maroon
}

CONFLICT_MATCHUPS = {
    # reds
    frozenset(("CAR", "CGY")),	frozenset(("CAR", "CHI")),	frozenset(("CAR", "DET")),	frozenset(("CAR", "FLA")),	frozenset(("CAR", "MTL")),	frozenset(("CAR", "NJD")),	frozenset(("CAR", "OTT")),
    frozenset(("CGY", "CAR")),	frozenset(("CGY", "CHI")),	frozenset(("CGY", "DET")),	frozenset(("CGY", "FLA")),	frozenset(("CGY", "MTL")),	frozenset(("CGY", "NJD")),	frozenset(("CGY", "OTT")),
    frozenset(("CHI", "CAR")),	frozenset(("CHI", "CGY")),	frozenset(("CHI", "DET")),	frozenset(("CHI", "FLA")),	frozenset(("CHI", "MTL")),	frozenset(("CHI", "NJD")),	frozenset(("CHI", "OTT")),
    frozenset(("DET", "CAR")),	frozenset(("DET", "CGY")),	frozenset(("DET", "CHI")),	frozenset(("DET", "FLA")),	frozenset(("DET", "MTL")),	frozenset(("DET", "NJD")),	frozenset(("DET", "OTT")),
    frozenset(("FLA", "CAR")),	frozenset(("FLA", "CGY")),	frozenset(("FLA", "CHI")),	frozenset(("FLA", "DET")),	frozenset(("FLA", "MTL")),	frozenset(("FLA", "NJD")),	frozenset(("FLA", "OTT")),
    frozenset(("MTL", "CAR")),	frozenset(("MTL", "CGY")),	frozenset(("MTL", "CHI")),	frozenset(("MTL", "DET")),	frozenset(("MTL", "FLA")),	frozenset(("MTL", "NJD")),	frozenset(("MTL", "OTT")),
    frozenset(("NJD", "CAR")),	frozenset(("NJD", "CGY")),	frozenset(("NJD", "CHI")),	frozenset(("NJD", "DET")),	frozenset(("NJD", "FLA")),	frozenset(("NJD", "MTL")),	frozenset(("NJD", "OTT")),
    frozenset(("OTT", "CAR")),	frozenset(("OTT", "CGY")),	frozenset(("OTT", "CHI")),	frozenset(("OTT", "DET")),	frozenset(("OTT", "FLA")),	frozenset(("OTT", "MTL")),	frozenset(("OTT", "NJD")),
    # blues
    frozenset(("BUF", "NYI")),	frozenset(("BUF", "NYR")),	frozenset(("BUF", "STL")),	frozenset(("BUF", "TBL")),	frozenset(("BUF", "TOR")),	frozenset(("BUF", "VAN")),
    frozenset(("NYI", "BUF")),	frozenset(("NYI", "NYR")),	frozenset(("NYI", "STL")),	frozenset(("NYI", "TBL")),	frozenset(("NYI", "TOR")),	frozenset(("NYI", "VAN")),
    frozenset(("NYR", "BUF")),	frozenset(("NYR", "NYI")),	frozenset(("NYR", "STL")),	frozenset(("NYR", "TBL")),	frozenset(("NYR", "TOR")),	frozenset(("NYR", "VAN")),
    frozenset(("STL", "BUF")),	frozenset(("STL", "NYI")),	frozenset(("STL", "NYR")),	frozenset(("STL", "TBL")),	frozenset(("STL", "TOR")),	frozenset(("STL", "VAN")),
    frozenset(("TBL", "BUF")),	frozenset(("TBL", "NYI")),	frozenset(("TBL", "NYR")),	frozenset(("TBL", "STL")),	frozenset(("TBL", "TOR")),	frozenset(("TBL", "VAN")),
    frozenset(("TOR", "BUF")),	frozenset(("TOR", "NYI")),	frozenset(("TOR", "NYR")),	frozenset(("TOR", "STL")),	frozenset(("TOR", "TBL")),	frozenset(("TOR", "VAN")),
    frozenset(("VAN", "BUF")),	frozenset(("VAN", "NYI")),	frozenset(("VAN", "NYR")),	frozenset(("VAN", "STL")),	frozenset(("VAN", "TBL")),	frozenset(("VAN", "TOR")),
    # yellow
    frozenset(("BOS", "NSH")),
    frozenset(("NSH", "BOS")),
    frozenset(("PIT", "BOS")),
    # orange
    frozenset(("EDM", "PHI")),
    # green
    frozenset(("MIN", "DAL")),

    # add "too close" pairs here as you discover them
}

EVENT_SYMBOLS = {
    "goal": "circle-dot",
    "sog": "circle-open-dot",
    "miss": "circle-open-dot",
    "block": "circle-x-open",
    "hit": "x",
}

SHOT_TYPE = {
    "wrist": "Wrist Shot",
    "tip-in": "Tip In",
    "snap": "Snap Shot",
    "deflected": "Deflected",
    "bat": "Bat",
    "backhand": "Backhand",
    "between-legs": "Between Legs",
    "wrap-around": "Wrap Around",
    "poke": "Poke",
    "slap": "Slap Shot",
    "cradle": "Cradle",
}

REASON = {
    "wide-right": "Wide Right",
    "wide-left": "Wide Left",
    "short": "Short",
    "hit-crossbar": "Hit Crossbar",
    "high-and-wide-right": "High and Wide Right",
    "hit-left-post": "Hit Left Post",
    "above-crossbar": "Above Crossbar",
    "failed-bank-attempt": "Failed Bank Attempt",
    "hit-right-post": "Hit Right Post",
    "high-and-wide-left": "High and Wide Left",
}

def mmss_to_sec_str(mmss: str) -> int:
    if mmss is None or mmss == "":
        return 0
    m, s = mmss.split(":")
    return int(m) * 60 + int(s)

def sec_to_mmss(sec: int) -> str:
    if sec is None:
        return "0:00"

    sec = int(sec)
    minutes = sec // 60
    seconds = sec % 60

    return f"{minutes}:{seconds:02d}"

# formatting ----------------------
def ensure_str(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    out = df
    for c in cols:
        if c in out.columns:
            out = out.with_columns(pl.col(c).cast(pl.Utf8))
    return out

def highlight_final_row(row):
    is_final = str(row["Period"]) == "Final" if "Period" in row.index else False
    return [f"background-color: {THEME['final_row']}" if is_final else "" for _ in row]

def section_banner(text: str):
    st.markdown(
        f"""
        <div style="
            width: 100%;
            background: {THEME['primary']};
            color: white;
            padding: 10px 16px;
            border-radius: 6px;
            font-family: 'Sora', system-ui, sans-serif;
            font-weight: 700;
            font-size: 20px;
            letter-spacing: 0.3px;
            margin: 10px 0 14px 0;
        ">
            {text}
        </div>
        """,
        unsafe_allow_html=True,
    )

def add_section_banner(text: str, add_pad=False):
    if add_pad:
        st.markdown("<div style='height: 32px;'></div>", unsafe_allow_html=True)
    section_banner(text)

def add_hover_html(shots: pl.DataFrame, players: pl.DataFrame | None = None) -> pl.DataFrame:

    df = shots

    # ---- optional: attach player names if lookup provided ----
    if players is not None and {"playerId", "skaterFullName"}.issubset(players.columns):
        p = players.select(["playerId", "skaterFullName"]).unique(subset=["playerId"])

        def join_name(id_col: str, name_col: str) -> None:
            nonlocal df
            if id_col in df.columns:
                df = df.join(
                    p.rename({"playerId": id_col, "skaterFullName": name_col}),
                    on=id_col,
                    how="left",
                )

        join_name("shootingPlayerId", "shootingPlayerName")
        join_name("blockingPlayerId", "blockingPlayerName")
        join_name("hittingPlayerId", "hittingPlayerName")
        join_name("hitteePlayerId", "hitteePlayerName")
        join_name("scoringPlayerId", "scoringPlayerName")
        join_name("assist1PlayerId", "assist1PlayerName")
        join_name("assist2PlayerId", "assist2PlayerName")
        join_name("goalieInNetId", "goalieInNetName")

    def name_or_id(name_col: str, id_col: str) -> pl.Expr:
        name_expr = pl.col(name_col).cast(pl.Utf8) if name_col in df.columns else pl.lit(None)
        id_expr   = pl.col(id_col).cast(pl.Utf8)   if id_col in df.columns else pl.lit(None)

        return (
            pl.when(name_expr.is_not_null()).then(name_expr)
            .when(id_expr.is_not_null()).then(id_expr)
            .otherwise(pl.lit("—"))   # <- NEVER NULL
        )

    shooter =   name_or_id("shootingPlayerName", "shootingPlayerId")
    blocker =   name_or_id("blockingPlayerName", "blockingPlayerId")
    hitter  =   name_or_id("hittingPlayerName", "hittingPlayerId")
    hittee  =   name_or_id("hitteePlayerName", "hitteePlayerId")
    scorer =    name_or_id("scoringPlayerName", "scoringPlayerId")
    a1 =        name_or_id("assist1PlayerName", "assist1PlayerId")
    a2 =        name_or_id("assist2PlayerName", "assist2PlayerId")
    goalie =    name_or_id("goalieInNetName", "goalieInNetId")

    a1_present = a1.is_not_null() & (a1 != "") & (a1 != "—")
    a2_present = a2.is_not_null() & (a2 != "") & (a2 != "—")

    assist_text = (
        pl.when(a1_present & a2_present)
        .then(pl.format("Assists: {}, {}<br>", a1, a2))
        .when(a1_present)
        .then(pl.format("Assists: {}<br>", a1))
        .otherwise(pl.lit(""))
    )

    # map shot type and reason
    df = df.with_columns(
        pl.col("shotType")
            .map_elements(lambda x: SHOT_TYPE.get(x), return_dtype=pl.Utf8)
            .alias("shotTypeLabel"),
        pl.col("reason")
            .map_elements(lambda x: REASON.get(x), return_dtype=pl.Utf8)
            .alias("reasonLabel")
    )

    # period label (OT instead of 4)
    period_label = (
        pl.when(pl.col("periodNumber") >= 4).then(pl.lit("OT"))
        .otherwise(pl.col("periodNumber").cast(pl.Utf8))
    )

    # ---- build event-specific hover HTML ----
    df = df.with_columns(
        hover_html=
            # blocked shot
            pl.when(pl.col("typeDescKey") == "blocked-shot")
              .then(pl.format("<b>P{} &#183; {}</b><br>{} shot blocked by {}",
                period_label, pl.col("timeInPeriod"), shooter, blocker))
            # hit
            .when(pl.col("typeDescKey") == "hit")
              .then(pl.format("<b>P{} &#183; {}</b><br>{} hit {}",
                period_label, pl.col("timeInPeriod"), hitter, hittee))
            # goal
            .when(pl.col("typeDescKey") == "goal")
            .then(pl.format("<b>P{} &#183; {}</b><br>{} Goal - {}<br>{}",
                period_label, pl.col("timeInPeriod"), scorer, pl.col("shotTypeLabel"), assist_text))
            # shot on goal
            .when(pl.col("typeDescKey") == "shot-on-goal")
              .then(pl.format("<b>P{} &#183; {}</b><br>{} {} saved by {}",
                 period_label, pl.col("timeInPeriod"), shooter, pl.col("shotTypeLabel"), goalie))
            # missed shot
            .when(pl.col("typeDescKey") == "missed-shot")
              .then(pl.format("<b>P{} &#183; {}</b><br>{} {} {}", 
                period_label, pl.col("timeInPeriod"), shooter, pl.col("shotTypeLabel"), pl.col("reasonLabel")))
            #
            .otherwise(pl.format("<b>{}</b><br>P{} &#183; {}", pl.col("typeDescKey"), period_label, pl.col("timeInPeriod")))
    )

    return df

def resolve_game_team_colors(home_abbrev: str, away_abbrev: str) -> dict[str, str]:

    """
    Returns a dict mapping teamAbbrev -> hex color for THIS GAME.
    Rule: if matchup is in CONFLICT_MATCHUPS, home keeps primary; away uses its alt if available.
    """
    home_color = TEAM_COLORS.get(home_abbrev, "#999999")
    away_color = TEAM_COLORS.get(away_abbrev, "#999999")

    if frozenset((home_abbrev, away_abbrev)) in CONFLICT_MATCHUPS:
        # swap AWAY to alt if we have one; otherwise fall back to primary
        away_color = TEAM_ALT_COLORS.get(away_abbrev, away_color)


    return {home_abbrev: home_color, away_abbrev: away_color}


# team x period table ----------------------
def build_team_period_table(
    eventTeamFlagsG: pl.DataFrame,
    pbpG: pl.DataFrame,
    teamsLookup: pl.DataFrame,
    team_id: str,          # NEW
) -> pl.DataFrame:
    # Base with periodNumber
    base = (
        eventTeamFlagsG
        .join(pbpG.select(["gameId", "eventId", "periodNumber"]), on=["gameId", "eventId"], how="left")
        .with_columns(periodNumber=pl.col("periodNumber").cast(pl.Int64))
        .filter(pl.col("teamId") == team_id)   # ✅ key line
    )

    # --- build the "spine": every team x every period present in pbp ---
    periods = (
        pbpG.select(pl.col("periodNumber").cast(pl.Int64))
        .filter(pl.col("periodNumber") != 5)
        .unique()
        .sort("periodNumber")
    )

    teams_in_game = pl.DataFrame(
        {"teamId": [team_id]}
    )

    spine = teams_in_game.join(periods, how="cross")  # teamId x periodNumber

    # --- aggregates that may be missing some team/periods ---
    per = (
        base
        .group_by(["teamId", "periodNumber"])
        .agg(
            GF=pl.sum("GF"),
            GA=pl.sum("GA"),
            CF=pl.sum("CF"),
            CA=pl.sum("CA"),
            FF=pl.sum("FF"),
            FA=pl.sum("FA"),
        )
    )

    # --- left join onto spine and fill missing with zeros ---
    per = (
        spine
        .join(per, on=["teamId", "periodNumber"], how="left")
        .with_columns(
            pl.col(["GF","GA","CF","CA","FF","FA"]).fill_null(0),
            Period=pl.col("periodNumber").cast(pl.Utf8),
            _order=pl.col("periodNumber").cast(pl.Int64),
        )
    )

    # --- final row ---
    fin = (
        base
        .group_by("teamId")
        .agg(
            GF=pl.sum("GF"),
            GA=pl.sum("GA"),
            CF=pl.sum("CF"),
            CA=pl.sum("CA"),
            FF=pl.sum("FF"),
            FA=pl.sum("FA"),
        )
        .with_columns(
            periodNumber=pl.lit(None, dtype=pl.Int64),
            Period=pl.lit("Final", dtype=pl.Utf8),
            _order=pl.lit(99, dtype=pl.Int64),
        )
        .with_columns(
            pl.col(["GF","GA","CF","CA","FF","FA"]).fill_null(0)
        )
    )

    out = pl.concat([per, fin], how="diagonal").with_columns(
        CF_pct=pl.when(pl.col("CF") + pl.col("CA") > 0)
            .then(100 * pl.col("CF") / (pl.col("CF") + pl.col("CA")))
            .otherwise(0.0),
        FF_pct=pl.when(pl.col("FF") + pl.col("FA") > 0)
            .then(100 * pl.col("FF") / (pl.col("FF") + pl.col("FA")))
            .otherwise(0.0),
    )

    out = (
        out
        .join(teamsLookup.select(["teamId", "teamAbbrev"]), on="teamId", how="left")
        .select(["teamAbbrev", "Period", "CF", "CA", "CF_pct", "FF", "FA", "FF_pct", "GF", "GA", "_order",])
        .sort(["teamAbbrev", "_order"])
        .drop("_order")
    )

    return out

def render_team_table(df_pl: pl.DataFrame, title: str):
    display_names = {
        "teamAbbrev": "Team",
        "Period": "Period",
        "CF": "CF",
        "CA": "CA",
        "CF_pct": "CF%",
        "FF": "FF",
        "FA": "FA",
        "FF_pct": "FF%",
        "GF": "GF",
        "GA": "GA",
    }

    df_pd = (
        df_pl
        .with_columns(
            pl.col("CF_pct").round(1),
            pl.col("FF_pct").round(1),
        )
        .rename(display_names)
        .with_columns(
            pl.when(pl.col("Period") == "4")
            .then(pl.lit("OT"))
            .otherwise(pl.col("Period"))
            .alias("Period")
        )
        .to_pandas()
        .drop(columns=["Team"], errors="ignore")
    )

    #header
    st.markdown(
        f"""
        <div style="
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            font-size: 1.4rem;
            font-weight: 600;
            margin-bottom: 0.5rem;
        ">
            {title}
        </div>
        """,
        unsafe_allow_html=True,
    )


    # Render as HTML so the CSS actually applies
    COL_WIDTH_PX = 80
    OUTER_BORDER = f"2px solid {THEME['border']}"
    GROUP_DIVIDER = f"2px solid {THEME['border']}"
    CELL_BORDER = f"1px solid {THEME['border_light']}"

    styler = (
        df_pd.style
        .apply(highlight_final_row, axis=1)
        .format({
            "CF%": lambda x: "—" if pd.isna(x) else f"{x:.1f}%",
            "FF%": lambda x: "—" if pd.isna(x) else f"{x:.1f}%",
        })
        .hide(axis="index")
        .set_table_styles([
            # ---- table container ----
            {"selector": "table", "props": [
                ("border-collapse", "separate"),
                ("border-spacing", "0"),
                ("width", "100%"),
                ("border", OUTER_BORDER),
                ("border-radius", "10px"),
                ("overflow", "hidden"),
                ("font-family", "Inter, system-ui, sans-serif"),
                ("font-size", "14px"),
            ]},

            # ---- header cells ----
            {"selector": "th", "props": [
                ("text-align", "center"),
                ("background", THEME["header_bg"]),
                ("padding", "8px 6px"),
                ("font-weight", "600"),
                ("border-bottom", CELL_BORDER),
            ]},

            # ---- body cells ----
            {"selector": "td", "props": [
                ("text-align", "center"),
                ("padding", "8px 6px"),
                ("border-bottom", CELL_BORDER),
                ("white-space", "nowrap"),
                ("min-width", f"{COL_WIDTH_PX}px"),
                ("max-width", f"{COL_WIDTH_PX}px"),
            ]}, 

            # ---- vertical group dividers ----
            {"selector": "th.col0, td.col0", "props": [("border-right", GROUP_DIVIDER)]},  # Period |
            {"selector": "th.col3, td.col3", "props": [("border-right", GROUP_DIVIDER)]},  # CF% |
            {"selector": "th.col6, td.col6", "props": [("border-right", GROUP_DIVIDER)]},  # FF% |

        ])
    )

    st.markdown(styler.to_html(), unsafe_allow_html=True)
    styler = styler.hide(axis="index")


# team x player table ----------------------
def build_team_player_table(
    eventTeamFlagsG: pl.DataFrame,
    eventOnIceG: pl.DataFrame,
    playersLookup: pl.DataFrame,
    playerTOI: pl.DataFrame, 
    team_id: str,
) -> pl.DataFrame:
    """
    Returns one row per player (on-ice events), with CF/CA/CF%, FF/FA/FF%, GF/GA
    filtered to a single teamId using the eventTeamFlags attribution.
    """

    # --- base: team-attributed events + who was on ice for those events ---
    base = (
        eventTeamFlagsG
        .filter(pl.col("teamId") == team_id)
        .join(
            eventOnIceG
            .select(["gameId", "eventId", "playerId", "teamId"])
            .rename({"teamId": "onIceTeamId"}),
            on=["gameId", "eventId"],
            how="inner",
        )
        .filter(pl.col("onIceTeamId") == team_id)
        .with_columns(playerId=pl.col("playerId").cast(pl.Utf8))
    )

    base = base.unique(subset=["gameId", "eventId", "playerId"])


    # --- aggregate per player ---
    per_player = (
        base
        .group_by(["teamId", "playerId"])
        .agg(
            GF=pl.sum("GF"),
            GA=pl.sum("GA"),
            CF=pl.sum("CF"),
            CA=pl.sum("CA"),
            FF=pl.sum("FF"),
            FA=pl.sum("FA"),
        )
        .with_columns(
            pl.col(["GF","GA","CF","CA","FF","FA"]).fill_null(0),
            CF_pct=pl.when(pl.col("CF") + pl.col("CA") > 0)
                .then(100 * pl.col("CF") / (pl.col("CF") + pl.col("CA")))
                .otherwise(0.0),
            FF_pct=pl.when(pl.col("FF") + pl.col("FA") > 0)
                .then(100 * pl.col("FF") / (pl.col("FF") + pl.col("FA")))
                .otherwise(0.0),
        )
    )

    # --- attach player names (your players table uses skaterFullName in helpers) ---
    # If your column is named differently, change "skaterFullName" below.
    add_names = (
        per_player
        .join(
            playersLookup.select(["playerId", "skaterFullName", "positionCode", "roleCode"]).unique(subset=["playerId"]),
            on="playerId",
            how="left",
        )
        .with_columns(
            Player=pl.col("skaterFullName").fill_null(pl.col("playerId")),
        )
        .sort("CF", descending=True)
    )

    add_TOI = (
        add_names
        .join(
            playerTOI,
            on="playerId",
            how="left"
        )
        .select([
            "Player", "durationSec",
            "positionCode", "roleCode",
            "CF", "CA", "CF_pct",
            "FF", "FA", "FF_pct",
            "GF", "GA",
        ])
    )

    return add_TOI

def render_player_table_html(df_pl: pl.DataFrame, title: str):
    display_names = {
        "Player": "Player",
        "duration": "TOI",
        "positionCode": "Position",
        "CF": "CF",
        "CA": "CA",
        "CF_pct": "CF%",
        "FF": "FF",
        "FA": "FA",
        "FF_pct": "FF%",
        "GF": "GF",
        "GA": "GA",
    }

    # clean up duration from seconds to ##:## formatting
    df_pd = (
        df_pl
        .with_columns(
            ((pl.col("durationSec") // 60).cast(pl.Utf8)
                + pl.lit(":")
                + (pl.col("durationSec") % 60).cast(pl.Utf8).str.zfill(2)
            ).alias("duration"),
            pl.col("CF_pct").round(1),
            pl.col("FF_pct").round(1),
        )
        .drop("durationSec")
    )

    # reorder and rename
    df_pd = (
        df_pd
        .select(
            "Player", "positionCode", "duration",
            "CF", "CA", "CF_pct",
            "FF", "FA", "FF_pct",
            "GF", "GA",
        )
        .rename(display_names)
        .to_pandas()
    )

    # header
    st.markdown(
        f"""
        <div style="
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            font-size: 1.4rem;
            font-weight: 600;
            margin-bottom: 0.5rem;
        ">
            {title}
        </div>
        """,
        unsafe_allow_html=True,
    )


    # Render as HTML so the CSS actually applies

    OUTER_BORDER = f"2px solid {THEME['border']}"
    GROUP_DIVIDER = f"2px solid {THEME['border']}"
    CELL_BORDER = f"1px solid {THEME['border_light']}"

    styler = (
        df_pd.style
        .apply(highlight_final_row, axis=1)
        .format({
            "CF%": lambda x: "—" if pd.isna(x) else f"{x:.1f}%",
            "FF%": lambda x: "—" if pd.isna(x) else f"{x:.1f}%",
        })
        .hide(axis="index")
        .set_table_styles([
            # ---- table container ----
            {"selector": "table", "props": [
                ("border-collapse", "separate"),
                ("border-spacing", "0"),
                ("width", "100%"),
                ("border", OUTER_BORDER),
                ("border-radius", "10px"),
                ("overflow", "hidden"),
                ("font-family", "Inter, system-ui, sans-serif"),
                ("font-size", "14px"),
            ]},

            # ---- player names ----
            {"selector": "th.col0, td.col0", "props": [
                ("text-align", "left"),
                ("padding-left", "12px"),
                ("padding", "4px 4px"),
                ("min-width", "200px"),
                ("max-width", "200px"),            
            ]},

            # ---- all other ----
            {"selector": "td", "props": [
                ("text-align", "center"),
                ("padding", "4px 4px"),
                ("border-bottom", CELL_BORDER),
                ("min-width", "60px"),
                ("max-width", "60px"),
            ]},

            # ---- header cells ----
            {"selector": "th", "props": [
                ("text-align", "center"),
                ("padding", "6px 6px"),
                ("font-weight", "600"),
                ("border-bottom", CELL_BORDER),
                ("background", THEME["header_bg"]),
            ]},

            # ---- vertical group dividers ----
            {"selector": "th.col2, td.col2", "props": [("border-right", GROUP_DIVIDER)]},  # Position |
            {"selector": "th.col5, td.col5", "props": [("border-right", GROUP_DIVIDER)]},  # CF% |
            {"selector": "th.col8, td.col8", "props": [("border-right", GROUP_DIVIDER)]},  # FF% |

        ])
    )

    st.markdown(styler.to_html(), unsafe_allow_html=True)
    styler = styler.hide(axis="index")


# shot map ----------------------
def shots_for_display(
    pbpG: pl.DataFrame,
    homeTeamId: str,
    awayTeamId: str,
    teamsLookup: pl.DataFrame,
) -> pl.DataFrame:

    return (
        pbpG
        .filter(pl.col("typeDescKey").is_in([
            "goal",
            "shot-on-goal",
            "missed-shot",
            "blocked-shot",
            "hit",
        ]))
        .filter(pl.col("xCoord").is_not_null() & pl.col("yCoord").is_not_null())
        .select([
            "eventId", "periodNumber", "timeInPeriod",
            "typeDescKey", "xCoord", "yCoord",
            "eventOwnerTeamId", "shotType", "reason",
            "shootingPlayerId",
            "blockingPlayerId",
            "hittingPlayerId",
            "hitteePlayerId",
            "scoringPlayerId",
            "assist1PlayerId",
            "assist2PlayerId",
            "goalieInNetId",

        ])

        # --- classify events for your dropdown ---
        .with_columns(
            eventClass=(
                pl.when(pl.col("typeDescKey") == "goal").then(pl.lit("goal"))
                .when(pl.col("typeDescKey") == "shot-on-goal").then(pl.lit("sog"))
                .when(pl.col("typeDescKey") == "missed-shot").then(pl.lit("miss"))
                .when(pl.col("typeDescKey") == "blocked-shot").then(pl.lit("block"))
                .when(pl.col("typeDescKey") == "hit").then(pl.lit("hit"))
                .otherwise(pl.lit("other"))
            ),

            # "All Shots" convenience class (so you don't have to do it in pandas)
            isShot=pl.col("typeDescKey").is_in(["goal", "shot-on-goal", "missed-shot", "blocked-shot"])
            
        )

        # --- swap blocked-shot team to defending team for coloring/side ---
        .with_columns(colorTeamId=pl.col("eventOwnerTeamId"))

        # --- attach abbrev for the (color) team, not the shooting team ---
        .join(
            teamsLookup.select(["teamId", "teamAbbrev"]),
            left_on="colorTeamId",
            right_on="teamId",
            how="left",
        )

        # --- side + plotted coords (based on colorTeamId) ---
        .with_columns(
            x_plot=pl.when(pl.col("eventOwnerTeamId") == awayTeamId)
                .then(-pl.col("xCoord").abs())
                .otherwise(pl.col("xCoord").abs()),
            y_plot=pl.col("yCoord"),
        )
        .with_columns(
            x_plot=pl.col("x_plot").clip(RINK_X_MIN + MARGIN, RINK_X_MAX - MARGIN),
            y_plot=pl.col("y_plot").clip(RINK_Y_MIN + MARGIN, RINK_Y_MAX - MARGIN),
        )
    )

def svg_to_base64(path: str) -> str:
    p = Path(path)

    # If a relative path was passed, resolve it relative to this file
    if not p.is_absolute():
        p = Path(__file__).resolve().parent / p

    if not p.exists():
        raise FileNotFoundError(f"SVG not found at: {p}")

    return base64.b64encode(p.read_bytes()).decode("utf-8")



# misc ----------------------
def strength_events_for(selection: str, eventStrengthG: pl.DataFrame) -> pl.DataFrame:
    if selection == "All":
        return eventStrengthG.select(["gameId", "eventId"])

    if selection == "EV":
        return (
            eventStrengthG
            .filter((pl.col("is5v5") == 1) | (pl.col("is4v4") == 1) | (pl.col("is3v3") == 1))
            .select(["gameId", "eventId"])
        )

    if selection == "5v5":
        return eventStrengthG.filter(pl.col("is5v5") == 1).select(["gameId", "eventId"])

    if selection == "5v4 PP":
        return eventStrengthG.filter(pl.col("is5v4") == 1).select(["gameId", "eventId"])

    if selection == "5v4 PK":
        return eventStrengthG.filter(pl.col("is4v5") == 1).select(["gameId", "eventId"])

    return eventStrengthG.select(["gameId", "eventId"])

def strength_player_TOI(selection: str, shifts: pl.DataFrame) -> pl.DataFrame:
    if selection == "All":
        filtered_shifts = shifts
    
    if selection == "EV":
        filtered_shifts = shifts.filter((pl.col("strength") == "5v5") | (pl.col("strength") == "4v4") | (pl.col("strength") == "3v3"))

    if selection == "5v5":
        filtered_shifts = shifts.filter(pl.col("strength") == "5v5")

    if selection == "5v4 PP":
        filtered_shifts = shifts.filter(pl.col("strength") == "5v4")

    if selection == "5v4 PK":
        filtered_shifts = shifts.filter(pl.col("strength") == "4v5")

    return filtered_shifts.group_by("playerId").agg(pl.col("durationSec").sum())

import streamlit as st
import polars as pl
import pandas as pd
import plotly.express as px
import html
from textwrap import dedent
import plotly.graph_objects as go

import app_helpers as ah
import file_definitions as file

# to run: streamlit run code/app.py         

EVENT_LABELS = {
    "goal": "Goal",
    "sog": "SOG",
    "miss": "Miss",
    "block": "Block",
    "hit": "Hit",
}

# --------------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------------

st.set_page_config(page_title="NHL Game Dashboard", layout="wide")

LOGO_WIDTH = 200

# ----------------------------------------------------------------------------------------------
# Global CSS
# ----------------------------------------------------------------------------------------------

# fonts
st.markdown(
    """
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Sora:wght@600;700;800&display=swap');
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');
    </style>
    """,
    unsafe_allow_html=True
)

#idk
st.markdown(
    f"""
    <style>
    /* Page background */
    html, body, [data-testid="stApp"] {{
        background-color: {ah.THEME["background"]};
    }}

    /* Main content container */
    .block-container {{
        background-color: {ah.THEME["background"]};
        padding-top: 1rem;
        padding-bottom: 1rem;
    }}

    /* Sidebar background */
    section[data-testid="stSidebar"] {{
        background-color: {ah.THEME["background"]};
    }}

    /* Sidebar inner content */
    section[data-testid="stSidebar"] > div {{
        background-color: {ah.THEME["background"]};
    }}
    </style>
    """,
    unsafe_allow_html=True
)

# tab formatting
st.markdown(
    """
    <style>
    /* --- SUPER BROAD: hit the actual clickable tab buttons --- */
    [data-testid="stTabs"] button[role="tab"] {
        font-family: 'Inter', system-ui, -apple-system, 'Segoe UI', sans-serif !important;
        font-size: 20px !important;
        font-weight: 700 !important;          /* ← BOLD */
        line-height: 1.1 !important;
        padding: 10px 14px !important;
    }

    /* Make sure all nested elements inherit font settings */
    [data-testid="stTabs"] button[role="tab"] * {
        font-family: inherit !important;
        font-size: inherit !important;
        font-weight: inherit !important;
        line-height: inherit !important;
    }

    /* Active tab emphasis */
    [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
        font-weight: 800 !important;          /* ← EXTRA BOLD */
    }

    /* Optional: spacing between tabs */
    [data-testid="stTabs"] [role="tablist"] {
        gap: 24px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

#tab color
st.markdown(
    f"""
    <style>
    [data-testid="stTabs"] button[role="tab"] {{
        font-family: 'Inter', system-ui, -apple-system, 'Segoe UI', sans-serif !important;
        font-size: 24px !important;
        font-weight: 700 !important;
        color: {ah.THEME["text_secondary"]} !important;   /* inactive */
    }}

    [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {{
        color: {ah.THEME["primary"]} !important;          /* active */
        font-weight: 800 !important;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

#tab underline color
st.markdown(
    f"""
    <style>
    /* --- BaseWeb tab underline (the red line you’re seeing) --- */
    [data-testid="stTabs"] div[data-baseweb="tab-highlight"] {{
        background-color: {ah.THEME["secondary"]} !important;
        height: 2px !important;
    }}


    </style>
    """,
    unsafe_allow_html=True,
)

# table sizing
st.markdown(
    f"""
    <style>
    .player-table-scroll {{
        max-height: 420px;          /* 👈 adjust to taste */
        overflow-y: auto;
        border-radius: 10px;
    }}

    /* Optional: nicer scrollbar (Chrome/Edge) */
    .player-table-scroll::-webkit-scrollbar {{
        width: 8px;
    }}

    .player-table-scroll::-webkit-scrollbar-thumb {{
        background: {ah.THEME["border"]};
        border-radius: 8px;
    }}

    .player-table-scroll::-webkit-scrollbar-track {{
        background: transparent;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)


# --------------------------------------------------------------------------------------------
# Load tables
# --------------------------------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def load_tables():
    games = file.load_table("games")
    teams = file.load_table("teams")
    players = file.load_table("players")
    pbp = file.load_table("pbp")
    shifts = file.load_table("shifts")
    eventOnIce = file.load_table("eventOnIce")
    eventStrength = file.load_table("eventStrength")
    eventTeamFlags = file.load_table("eventTeamFlags")
    return games, teams, players, pbp, shifts, eventOnIce, eventStrength, eventTeamFlags


games, teams, players, pbp, shifts, eventOnIce, eventStrength, eventTeamFlags = load_tables()

# Ensure IDs are strings (your convention)
games = ah.ensure_str(games, ["gameId", "homeTeamId", "awayTeamId"])
teams = ah.ensure_str(teams, ["teamId", "teamAbbrev"])
players = ah.ensure_str(players, ["playerId"])
pbp = ah.ensure_str(pbp, ["gameId", "eventId", "homeTeamId", "awayTeamId"])
shifts = ah.ensure_str(shifts, ["gameId", "playerId", "teamId"])
eventOnIce = ah.ensure_str(eventOnIce, ["gameId", "eventId", "playerId", "teamId"])
eventStrength = ah.ensure_str(eventStrength, ["gameId", "eventId"])
eventTeamFlags = ah.ensure_str(eventTeamFlags, ["gameId", "eventId", "teamId"])

# --------------------------------------------------------------------------------------------
# Global Filters
# --------------------------------------------------------------------------------------------

with st.sidebar:
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("# NHL Stats Dashboard")
    st.markdown("### Filters")

teamsLookup = teams.select(["teamId", "teamName", "teamAbbrev", "teamLogo"]).unique(subset=["teamId"])

# Build team dropdown off lookup
team_options = teamsLookup.select(["teamName", "teamId"]).sort("teamName")
team_names = team_options.get_column("teamName").to_list()

selectedTeamName = st.sidebar.selectbox("Choose a team:", team_names, index=0)
selectedTeamId = team_options.filter(pl.col("teamName") == selectedTeamName).select("teamId").item()

# Join first (for abbrev/logo), filter by ID (reliable)
gamesUi = (
    games
    .join(
        teamsLookup.rename({"teamId": "homeTeamId", "teamName": "homeTeamName", "teamAbbrev": "homeTeamAbbrev", "teamLogo": "homeTeamLogo"}),
        on="homeTeamId",
        how="left",
    )
    .join(
        teamsLookup.rename({"teamId": "awayTeamId", "teamName": "awayTeamName", "teamAbbrev": "awayTeamAbbrev", "teamLogo": "awayTeamLogo"}),
        on="awayTeamId",
        how="left",
    )
    .filter((pl.col("homeTeamId") == selectedTeamId) | (pl.col("awayTeamId") == selectedTeamId))
)

labelCol = "label"
if "gameDate" in gamesUi.columns:
    gamesUi = gamesUi.with_columns(
        #pl.format("{} @ {} ({})", pl.col("awayTeamAbbrev"), pl.col("homeTeamAbbrev"), pl.col("gameDate")).alias(labelCol)
        pl.format("{} @ {} ({})", pl.col("awayTeamAbbrev"), pl.col("homeTeamAbbrev"), pl.col("gameId")).alias(labelCol)
    ).sort("gameId", descending=True)  # newest first
else:
    gamesUi = gamesUi.with_columns(
        pl.format("{} @ {} ({})", pl.col("awayTeamAbbrev"), pl.col("homeTeamAbbrev"), pl.col("gameId")).alias(labelCol)
    )

game_labels = gamesUi.get_column(labelCol).to_list()
selectedGameLabel = st.sidebar.selectbox("Choose a game", game_labels, index=0)

gameId = gamesUi.filter(pl.col(labelCol) == selectedGameLabel).select("gameId").item()


# --------------------------------------------------------------------------------------------
# Filter game tables
# --------------------------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def filter_game_tables(pbp: pl.DataFrame,
                       shiftsG: pl.DataFrame,
                       eventOnIce: pl.DataFrame,
                       eventStrength: pl.DataFrame,
                       eventTeamFlags: pl.DataFrame,
                       gameId: str):
    pbpG = pbp.filter(pl.col("gameId") == gameId)
    shiftsG = shifts.filter(pl.col("gameId") == gameId)
    eventOnIceG = eventOnIce.filter(pl.col("gameId") == gameId)
    eventStrengthG = eventStrength.filter(pl.col("gameId") == gameId)
    eventTeamFlagsG = eventTeamFlags.filter(pl.col("gameId") == gameId)
    return pbpG, shiftsG, eventOnIceG, eventStrengthG, eventTeamFlagsG


pbpG, shiftsG, eventOnIceG, eventStrengthG, eventTeamFlagsG = filter_game_tables(
    pbp, shifts, eventOnIce, eventStrength, eventTeamFlags, gameId
)

# Pull home/away teamIds for header
homeTeamId = pbpG.select("homeTeamId").head(1).item() if pbpG.height > 0 and "homeTeamId" in pbpG.columns else None
awayTeamId = pbpG.select("awayTeamId").head(1).item() if pbpG.height > 0 and "awayTeamId" in pbpG.columns else None

homeInfo = teamsLookup.filter(pl.col("teamId") == homeTeamId).to_dicts()[0] if homeTeamId else {}
awayInfo = teamsLookup.filter(pl.col("teamId") == awayTeamId).to_dicts()[0] if awayTeamId else {}

away_abbrev = awayInfo.get("teamAbbrev")
home_abbrev = homeInfo.get("teamAbbrev")


# --------------------------------------------------------------------------------------------
# # Header
# --------------------------------------------------------------------------------------------

scores = (
    games
    .filter(pl.col("gameId") == gameId)
    .select([
        "homeTeamScore",
        "awayTeamScore",
        pl.col("startTimeUTC")
          .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
          .dt.replace_time_zone("UTC")
          .dt.convert_time_zone("America/Chicago")
          .dt.strftime("%m/%d/%Y")
          .alias("game_date_ct"),
        "gameOutcome",
    ])
)

home_score = scores.item(0, 0)
away_score = scores.item(0, 1)
game_date  = scores.item(0, 2)
raw_outcome = "LIVE" if scores.item(0, 3) == None else scores.item(0, 3)
outcome = "" if raw_outcome == "REG" else "/"+raw_outcome

meta_text = "LIVE" if raw_outcome == "LIVE" else f"{away_score} – {home_score}\nFINAL{outcome}"
meta_html = "<br>".join(html.escape(meta_text).splitlines())

temp_text = f"{game_date} - {gameId}"
temp_html = "<br>".join(html.escape(temp_text).splitlines())

away_logo = awayInfo.get("teamLogo", "")
home_logo = homeInfo.get("teamLogo", "")
away_name = awayInfo.get("teamName", "Away")
home_name = homeInfo.get("teamName", "Home")

st.html(f"""
<style>
  .hdr {{
    display: flex;
    align-items: center;          /* ✅ vertical centering */
    justify-content: space-between;
    gap: 24px;
    padding: 8px 0 6px 0;
  }}
  .hdr .logo {{
    width: {LOGO_WIDTH}px;
    flex: 0 0 {LOGO_WIDTH}px;
    display:flex;
    align-items:center;
    justify-content:center;
  }}
  .hdr .logo img {{
    width: 100%;
    height: auto;
    display:block;
  }}
  .hdr .team {{
    flex: 1 1 0;
    min-width: 0;                 
    font-family: 'Sora', system-ui, sans-serif;
    font-size: 30px;
    font-weight: 700;
    letter-spacing: 0.3px;
    line-height: 1.05;

    white-space: nowrap;          /* keep to one line */
    overflow: hidden;
    text-overflow: ellipsis;      /* ✅ no collision on long names */
  }}
  .hdr .team.left  {{ text-align: right; }}
  .hdr .team.right {{ text-align: left;  }}

  .hdr .mid {{
    flex: 0 0 auto;
    text-align: center;
    line-height: 1.2;
    margin: 0 8px;
  }}
  .hdr .score {{
    font-family: 'Sora', system-ui, sans-serif;
    font-size: 24px;
    font-weight: 700;
    color: {ah.THEME["text_primary"]};
  }}
  .hdr .meta {{
    font-family: 'Inter', system-ui, -apple-system, 'Segoe UI', sans-serif;
    font-size: 14px;
    font-weight: 500;
    color: {ah.THEME["text_secondary"]};
    margin-top: 4px;
  }}
</style>


<div class="hdr">
    <div class="logo">
        <img src="{away_logo}" alt="away logo"/>
    </div>

    <div class="team left">{away_name}</div>

    <div class="mid">
        <div class="score">{meta_html}</div>
        <div class="meta">{temp_html}</div>
    </div>

    <div class="team right">{home_name}</div>

    <div class="logo">
        <img src="{home_logo}" alt="home logo"/>
    </div>
</div>

""")













# --------------------------------------------------------------------------------------------
# Create tabs
# --------------------------------------------------------------------------------------------

tab_overview, tab_players, tab_lines = st.tabs([
    "Overview",
    "Player Stats",
    "Line Stats",
])



# --------------------------------------------------------------------------------------------
# Overview tab
# --------------------------------------------------------------------------------------------

# ----------------------------------------------------
with tab_overview:
    ah.section_banner("Stats Overview")

    eventOnIce_test = eventOnIce.filter(pl.col("gameId") == gameId)
    if eventOnIce_test.height == 0:
        st.write("No shift data available")
    else:
        # --- strength selectbox (replaces tabs) ---
        strength_options = ["All", "EV", "5v5", "5v4 PP", "5v4 PK"]
        
        strength_fill, space = st.columns([1,5])
        with strength_fill:
            strength_selection = st.selectbox(
                "Strength",
                strength_options,
                index=strength_options.index("5v5"),   # default
                key="overview_strength",
                #label_visibility="collapsed"
            )

        def render_overview(selection: str):
            def strength_event_ids_for_team(selection: str, team_id: str) -> pl.DataFrame:
                if selection == "All":
                    return eventStrengthG.select(["gameId", "eventId"])

                if selection == "EV":
                    return (
                        eventStrengthG
                        .filter(
                            (pl.col("is5v5") == 1) |
                            (pl.col("is4v4") == 1) |
                            (pl.col("is3v3") == 1)
                        )
                        .select(["gameId", "eventId"])
                    )

                if selection == "5v5":
                    return (
                        eventStrengthG
                        .filter(pl.col("is5v5") == 1)
                        .select(["gameId", "eventId"])
                    )

                if selection in {"5v4 PP", "5v4 PK"}:
                    # include BOTH directions of 5v4 (home5-away4 OR home4-away5)
                    s54 = eventStrengthG.filter(
                        ((pl.col("homeSkaters") == 5) & (pl.col("awaySkaters") == 4)) |
                        ((pl.col("homeSkaters") == 4) & (pl.col("awaySkaters") == 5))
                    )

                    # team side (home vs away for this game)
                    is_home_team = (pl.lit(team_id) == pl.lit(homeTeamId))

                    is_team_pp = (
                        pl.when(is_home_team)
                        .then(pl.col("homeSkaters") > pl.col("awaySkaters"))
                        .otherwise(pl.col("awaySkaters") > pl.col("homeSkaters"))
                    )

                    keep = is_team_pp if selection == "5v4 PP" else (~is_team_pp)
                    return s54.filter(keep).select(["gameId", "eventId"])

                return eventStrengthG.select(["gameId", "eventId"])

            if eventTeamFlagsG.height == 0:
                st.warning("No eventTeamFlags available for this game yet.")
                return

            away_events = strength_event_ids_for_team(selection, awayTeamId)
            home_events = strength_event_ids_for_team(selection, homeTeamId)

            away_flags = eventTeamFlagsG.join(away_events, on=["gameId", "eventId"], how="inner")
            home_flags = eventTeamFlagsG.join(home_events, on=["gameId", "eventId"], how="inner")

            away_tbl = ah.build_team_period_table(away_flags, pbpG, teamsLookup, awayTeamId)
            home_tbl = ah.build_team_period_table(home_flags, pbpG, teamsLookup, homeTeamId)

            atcol, htcol = st.columns(2, gap="large")
            with atcol:
                ah.render_team_table(away_tbl, f"{awayInfo.get('teamName','Away')} ({away_abbrev})")
            with htcol:
                ah.render_team_table(home_tbl, f"{homeInfo.get('teamName','Home')} ({home_abbrev})")

        # --- render once, based on dropdown selection ---
        render_overview(strength_selection)





    # --------------------------------------------------------------------------------------------------------
    ah.add_section_banner("Shot Map", True)

    period_filter, event_filter, space = st.columns([1,1,4])

    with period_filter:
        if raw_outcome == "REG":
            period_choice = st.selectbox(
                "Period",
                ["All", "1", "2", "3"],
                index=0,
            )
        else:
            period_choice = st.selectbox(
                "Period",
                ["All", "1", "2", "3", "OT"],
                index=0,
            )

    with event_filter:
        event_choice = st.selectbox(
        "Event",
        ["All Events", "Goals", "All Shots", "Shots on Goal", "Missed Shots", "Blocks", "Hits"],
        index=0
    )



    shots = (
        pbpG
        .filter(pl.col("typeDescKey").is_in(["shot-on-goal", "missed-shot", "goal"]))
        .filter(pl.col("xCoord").is_not_null() & pl.col("yCoord").is_not_null())
        .select([
            "eventId", "periodNumber", "timeInPeriod",
            "typeDescKey", "xCoord", "yCoord",
            "eventOwnerTeamId", "shotType"
        ])
    )

    @st.cache_data(show_spinner=False)
    def build_shots_plot_df(pbpG: pl.DataFrame,
                            homeTeamId: str,
                            awayTeamId: str,
                            teamsLookup: pl.DataFrame,
                            players: pl.DataFrame):
        shots_pl = ah.shots_for_display(pbpG, homeTeamId, awayTeamId, teamsLookup)
        shots_pl = ah.add_hover_html(shots_pl, players)
        return shots_pl.to_pandas()

    shots_plot = build_shots_plot_df(pbpG, homeTeamId, awayTeamId, teamsLookup, players)


    # --- period filter ---
    if period_choice != "All":
        if period_choice == "OT":
            shots_plot = shots_plot[shots_plot["periodNumber"] >= 4]
        else:
            shots_plot = shots_plot[shots_plot["periodNumber"] == int(period_choice)]

    # --- event filter ---
    if event_choice == "Goals":
        shots_plot = shots_plot[shots_plot["eventClass"] == "goal"]
    elif event_choice == "All Shots":
        shots_plot = shots_plot[shots_plot["isShot"] == True]
    elif event_choice == "Shots on Goal":
        shots_plot = shots_plot[shots_plot["eventClass"] == "sog"]
    elif event_choice == "Missed Shots":
        shots_plot = shots_plot[shots_plot["eventClass"] == "miss"]
    elif event_choice == "Blocks":
        shots_plot = shots_plot[shots_plot["eventClass"] == "block"]
    elif event_choice == "Hits":
        shots_plot = shots_plot[shots_plot["eventClass"] == "hit"]

    fig = go.Figure()

    home_abbrev = homeInfo.get("teamAbbrev")
    away_abbrev = awayInfo.get("teamAbbrev")

    game_colors = ah.resolve_game_team_colors(home_abbrev, away_abbrev)

    for team in shots_plot["teamAbbrev"].dropna().unique():
        df_team = shots_plot[shots_plot["teamAbbrev"] == team]

        for cls, symbol in ah.EVENT_SYMBOLS.items():
            df_tc = df_team[df_team["eventClass"] == cls]
            if df_tc.empty:
                continue

            fig.add_trace(
                go.Scatter(
                    x=df_tc["x_plot"],
                    y=df_tc["y_plot"],
                    mode="markers",
                    name=f"{team} {EVENT_LABELS.get(cls, cls)}",
                    marker=dict(
                        color=game_colors.get(team, ah.TEAM_COLORS.get(team, "#000000")),
                        symbol=symbol,
                        size=10 if cls == "hit" else 14,
                        line=dict(width=1 if cls == "hit" else 2, color="white"),
                    ),
                    hovertext=df_tc["hover_html"].fillna(""),
                    hovertemplate="%{hovertext}<extra></extra>",
                    showlegend=True,
                )
            )

            fig.update_layout(
                hoverlabel=dict(
                    font_size=14, 
                    font_family="Inter"
                )
            )

    rink_b64 = ah.svg_to_base64("assets/rink.svg")

    fig.add_layout_image(
        dict(
            source=f"data:image/svg+xml;base64,{rink_b64}",
            xref="x",
            yref="y",
            x=-100,
            y=42.5,
            sizex=200,
            sizey=85,
            sizing="stretch",
            opacity=1.0,
            layer="below",
        )
    )

    fig.update_xaxes(range=[-100, 100], visible=False)
    fig.update_yaxes(range=[-42.5, 42.5], visible=False, scaleanchor="x", scaleratio=1)

    fig.update_layout(
        height=650,
        margin=dict(l=10, r=10, t=10, b=10),
        hoverlabel=dict(font_size=14),
    )

    fig.add_annotation(
        x=-97,
        y=40,
        text=f"<b>{away_abbrev}</b>",
        showarrow=False,
        xref="x",
        yref="y",
        font=dict(
            size=20,
            family="Inter, system-ui, sans-serif",
            color=game_colors.get(away_abbrev, "#000000"),
        ),
        align="center",
    )

    fig.add_annotation(
        x=97,
        y=40,
        text=f"<b>{home_abbrev}</b>",
        showarrow=False,
        xref="x",
        yref="y",
        font=dict(
            size=20,
            family="Inter, system-ui, sans-serif",
            color=game_colors.get(home_abbrev, "#000000"),
        ),
        align="center",
    )


    st.plotly_chart(fig, width='stretch',config={"displayModeBar": False,})








# --------------------------------------------------------------------------------------------
# Players tab
# --------------------------------------------------------------------------------------------

with tab_players:
    ah.section_banner("On-Ice Player Stats")

    if eventOnIce_test.height == 0:
        st.write("No shift data available")
    else:
        strength_space, role_space, sort_space, space = st.columns([1,1,1,3])

        with strength_space:
            strength_options = ["All", "EV", "5v5", "5v4 PP", "5v4 PK"]
            strength_selection = st.selectbox(
                "Strength",
                strength_options,
                index=strength_options.index("5v5"),
                key="players_strength",
                #label_visibility="collapsed",
            )

        with role_space:
            role_options = ["All","F","D"]
            role_selection = st.selectbox(
                "Role",
                role_options,
                index=strength_options.index("All"),
                key="role",
                #label_visibility="collapsed",
            )

        with sort_space: 
            sort_map = {
                "Player": "Player",
                "CF": "CF",
                "CA": "CA",
                "CF%": "CF_pct",
                "FF": "FF",
                "FA": "FA",
                "FF%": "FF_pct",
                "GF": "GF",
                "GA": "GA",
            }

            c_sort, c_dir = st.columns([2, 1], vertical_alignment="bottom")

            with c_sort:
                sort_label = st.selectbox(
                    "Sort",
                    list(sort_map.keys()),
                    index=list(sort_map.keys()).index("CF"),
                    key="player_sort_col",
                    #label_visibility="collapsed",
                )

            with c_dir:
                sort_desc = st.toggle(
                    "Desc",
                    value=True,
                    key="player_sort_desc",
                )

            sort_col = sort_map[sort_label]




        # Filter events by strength 
        strength_events = ah.strength_events_for(strength_selection, eventStrengthG)

        # Filter player TIO by strength from shifts
        playerTOI = ah.strength_player_TOI(strength_selection, shiftsG)

        # Apply strength filter to team flags + on-ice
        flags_s = eventTeamFlagsG.join(strength_events, on=["gameId", "eventId"], how="inner")
        onice_s = eventOnIceG.join(strength_events, on=["gameId", "eventId"], how="inner")

        away_tbl = ah.build_team_player_table(flags_s, onice_s, players, playerTOI, awayTeamId)
        home_tbl = ah.build_team_player_table(flags_s, onice_s, players, playerTOI, homeTeamId)

        # Filter team tables for roles if needed
        if role_selection != "All":
            away_tbl = away_tbl.filter(pl.col("roleCode") == role_selection)
            home_tbl = home_tbl.filter(pl.col("roleCode") == role_selection)

        away_tbl = away_tbl.sort([sort_col, "CF"], descending=[sort_desc, False])
        home_tbl = home_tbl.sort([sort_col, "CF"], descending=[sort_desc, False])
        

        ah.render_player_table_html(away_tbl, f"{awayInfo.get('teamName','Away')} ({away_abbrev})")

        ah.render_player_table_html(home_tbl, f"{homeInfo.get('teamName','Home')} ({home_abbrev})")

with tab_lines:
    ah.section_banner("Line Stats")

    st.write(shiftsG)

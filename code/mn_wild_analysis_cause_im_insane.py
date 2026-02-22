import polars as pl

import database_spine as dbs
import vars
import api

#====================================================================================
# Database Spine Tables
#====================================================================================
seasonId = vars.current_season

mn_season = pl.DataFrame(
    {
        "triCode": ["MIN"],

    }
)

# #pull games for the teams and seasons
# mn_ot_games = dbs.db_games(mn_season, seasonId, 2) # 1 pre season // 2 regular season // 3 playoffs // 4 all stars

# mn_ot_games = (
#     mn_ot_games
#     .filter(pl.col("gameOutcome")=="REG")
# )
# mn_ot_games.write_excel("mn_ot_games.xlsx", autofit=True); __import__("os").startfile("mn_ot_games.xlsx")

# shifts = (
#     dbs.db_shifts(mn_ot_games)
#     .filter(pl.col("playerId") == "8482122")
# )

#shifts.write_excel("shifts.xlsx", autofit=True); __import__("os").startfile("shifts.xlsx")

players_list = api.call_players_api(seasonId) 
players_list.write_excel("_players.xlsx", autofit=True); __import__("os").startfile("_players.xlsx")

test = api.call_shift_api("2025020582")
test.write_excel("test.xlsx", autofit=True); __import__("os").startfile("test.xlsx")

# DF_PBPS = dbs.db_pbp(DF_GAMES)
# DF_PBPS.sort(["gameId","eventId"])

# DF_SHIFTS.sort(["gameId","playerId","absStart"])
# DF_EVENT_ON_ICE = dbs.db_eventOnIce(DF_PBPS, DF_SHIFTS)
# DF_EVENT_STRENGTH = dbs.db_eventStrength(DF_EVENT_ON_ICE, DF_PBPS)
# DF_EVENT_TEAM_FLAGS = dbs.db_eventTeamFlags(DF_PBPS, DF_SHIFTS)



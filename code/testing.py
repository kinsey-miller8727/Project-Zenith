import os
import polars as pl 

# DATA_DIR = "data/20252026"  

# PARQUET_TIMESTAMP = "20260203_222815" # <- last full correct pull
# # PARQUET_TIMESTAMP = "" # <- fullt team shift testing

# FILES = {
#     "games": f"games_{PARQUET_TIMESTAMP}.parquet",
#     "teams": f"teams_{PARQUET_TIMESTAMP}.parquet",
#     "players": f"players_{PARQUET_TIMESTAMP}.parquet",
#     "pbp": f"pbp_{PARQUET_TIMESTAMP}.parquet",
#     "shifts": f"shifts_{PARQUET_TIMESTAMP}.parquet",
#     "eventOnIce": f"eventOnIce_{PARQUET_TIMESTAMP}.parquet",
#     "eventStrength": f"eventStrength_{PARQUET_TIMESTAMP}.parquet",
#     "eventTeamFlags": f"eventTeamFlags_{PARQUET_TIMESTAMP}.parquet",
# }
# def load_table(name: str) -> pl.DataFrame:
#     path = os.path.join(DATA_DIR, FILES[name])
#     return pl.read_parquet(path)

# pbp = load_table("games")

# pbp = (
#     pbp
#     .filter((pl.col("gameId") == "2025020892"))
# ).unique()

# # pbp = 
# #     pbp
# #     .filter((pl.col("typeDescKey") == "shot-on-goal") | (pl.col("typeDescKey") == "missed-shot"),
# #             pl.col("gameId") == "2025020754")
# # )

# pbp.write_excel("_peek.xlsx", autofit=True); __import__("os").startfile("_peek.xlsx")


# ===================================================================================================================================================

import database_spine as dbs

game = "2025020780"
seasonId = 20252026

season_table = pl.DataFrame(
    {
        "triCode": ["MIN"],

    }
)

DF_GAMES = dbs.db_games(season_table, seasonId, 2) 
DF_GAMES = DF_GAMES.filter(pl.col("gameId") == game)

DF_TEAMS = dbs.db_teams()

DF_PLAYERS = dbs.db_players(seasonId)

DF_SHIFTS = dbs.db_shifts(DF_GAMES, DF_PLAYERS, DF_TEAMS)
DF_SHIFTS.sort(["gameId","playerId","absStart"])
DF_SHIFTS.write_excel("_DF_SHIFTS.xlsx", autofit=True); __import__("os").startfile("_DF_SHIFTS.xlsx")

DF_SHIFTS_HTML = dbs.db_shift_html(game, DF_PLAYERS, DF_TEAMS)
DF_SHIFTS_HTML.sort(["gameId","playerId","absStart"])
DF_SHIFTS_HTML.write_excel("_DF_SHIFTS_HTML.xlsx", autofit=True); __import__("os").startfile("_DF_SHIFTS_HTML.xlsx")
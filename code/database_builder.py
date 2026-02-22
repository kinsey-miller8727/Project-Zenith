import polars as pl

import database_spine as dbs
import vars
from datetime import datetime

#====================================================================================
# Database Spine Tables
#====================================================================================

def create_season_db(seasonId):
    season_table = pl.DataFrame(
        {
            "triCode": ["ANA","BOS","BUF","CAR","CBJ","CGY","CHI","COL","DAL","DET","EDM","FLA","LAK","MIN","MTL","NJD",
                         "NSH","NYI","NYR","OTT","PHI","PIT","SEA","SJS","STL","TBL","TOR","UTA","VAN","VGK","WPG","WSH"],
        }
    )

    # season_table = pl.DataFrame(
    #     {
    #         "triCode": ["MIN"],

    #     }
    # )

   # pull games for the teams and seasons
    DF_GAMES = dbs.db_games(season_table, seasonId, 2) # 1 pre season // 2 regular season // 3 playoffs // 4 all stars
    print("DF_GAMES pulled")

    DF_TEAMS = dbs.db_teams()
    print("DF_TEAMS pulled")

    DF_PLAYERS = dbs.db_players(seasonId)
    print("DF_PLAYERS pulled")

    DF_PBPS = dbs.db_pbp(DF_GAMES)
    DF_PBPS.sort(["gameId","eventId"])
    print("DF_PBPS pulled")
    
    DF_SHIFTS = dbs.db_shifts(DF_GAMES, DF_PLAYERS, DF_TEAMS)
    DF_SHIFTS.sort(["gameId","playerId","absStart"])
    print("DF_SHIFTS pulled")

    DF_EVENT_ON_ICE = dbs.db_eventOnIce(DF_PBPS, DF_SHIFTS)
    DF_EVENT_STRENGTH = dbs.db_eventStrength(DF_EVENT_ON_ICE, DF_PBPS)
    DF_EVENT_TEAM_FLAGS = dbs.db_eventTeamFlags(DF_PBPS, DF_SHIFTS)
    
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    DF_GAMES.write_parquet(f"data/{seasonId}/games_{ts}.parquet")
    DF_TEAMS.write_parquet(f"data/{seasonId}/teams_{ts}.parquet")
    DF_PLAYERS.write_parquet(f"data/{seasonId}/players_{ts}.parquet")
    DF_PBPS.write_parquet(f"data/{seasonId}/pbp_{ts}.parquet")
    DF_SHIFTS.write_parquet(f"data/{seasonId}/shifts_{ts}.parquet")
    DF_EVENT_ON_ICE.write_parquet(f"data/{seasonId}/eventOnIce_{ts}.parquet")
    DF_EVENT_STRENGTH.write_parquet(f"data/{seasonId}/eventStrength_{ts}.parquet")
    DF_EVENT_TEAM_FLAGS.write_parquet(f"data/{seasonId}/eventTeamFlags_{ts}.parquet")

    print("All database tables created. Timestamp = " + ts)

create_season_db(vars.current_season)
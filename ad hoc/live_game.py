from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse, parse_qs, urlunparse
from datetime import date

import requests
import polars as pl
import json

import vars
import api
import helpers
import database_spine as dbs

#====================================================================================
# Live Game Pull 
#====================================================================================

#pull games for the teams and seasons
live_game = helpers.pull_live_game("WSH",vars.current_season)
# live_game.write_excel("_games.xlsx", autofit=True); __import__("os").startfile("_games.xlsx")

pbp = dbs.db_pbp(live_game)
# pbp.write_excel("_pbp.xlsx", autofit=True); __import__("os").startfile("_pbp.xlsx")

shifts = dbs.db_shifts(live_game)
# shifts.write_excel("_shifts.xlsx", autofit=True); __import__("os").startfile("_shifts.xlsx")

events_on_ice = dbs.db_eventOnIce(pbp, shifts)
# events_on_ice.write_excel("_events_on_ice.xlsx", autofit=True); __import__("os").startfile("_events_on_ice.xlsx")

event_strength = dbs.db_eventStrength(events_on_ice, pbp)
# event_strength.write_excel("_event_strength.xlsx", autofit=True); __import__("os").startfile("_event_strength.xlsx")

event_team_flags = dbs.db_eventTeamFlags(pbp, shifts)
# event_team_flags.write_excel("_event_team_flags.xlsx", autofit=True); __import__("os").startfile("_event_team_flags.xlsx")

import streamlit as st
import polars as pl
import os

DATA_DIR = "data/20252026"  

PARQUET_TIMESTAMP = "20260220_095220" # <- what pull
# PARQUET_TIMESTAMP = "" # <- fullt team shift testing

FILES = {
    "games": f"games_{PARQUET_TIMESTAMP}.parquet",
    "teams": f"teams_{PARQUET_TIMESTAMP}.parquet",
    "players": f"players_{PARQUET_TIMESTAMP}.parquet",
    "pbp": f"pbp_{PARQUET_TIMESTAMP}.parquet",
    "shifts": f"shifts_{PARQUET_TIMESTAMP}.parquet",
    "eventOnIce": f"eventOnIce_{PARQUET_TIMESTAMP}.parquet",
    "eventStrength": f"eventStrength_{PARQUET_TIMESTAMP}.parquet",
    "eventTeamFlags": f"eventTeamFlags_{PARQUET_TIMESTAMP}.parquet",
}

# --------------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------------
@st.cache_data
def load_table(name: str) -> pl.DataFrame:
    path = os.path.join(DATA_DIR, FILES[name])
    return pl.read_parquet(path)



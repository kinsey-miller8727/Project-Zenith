import polars as pl
import pandas as pd
from pathlib import Path
import os

season = "2025"

# ---- Your target schema (as Polars dtypes) ----
SCHEMA: dict[str, pl.DataType] = {
    "Period": pl.Int64,
    "Time": pl.Time,
    "Strength": pl.Utf8,
    "Team": pl.Utf8,
    "Shooter": pl.Int64,
    "Shot Type": pl.Utf8,
    "A1": pl.Int64,
    "A2": pl.Int64,
    "A3": pl.Int64,
    "A1 Zone": pl.Utf8,
    "A2 Zone": pl.Utf8,
    "A3 Zone": pl.Utf8,
    "SC?": pl.Utf8,
    "SOG?": pl.Utf8,
    "Screen?": pl.Utf8,
    "Rush?": pl.Utf8,
    "Origin": pl.Utf8,
    "Context": pl.Utf8,
    "Oddman?": pl.Utf8,
    "G?": pl.Utf8,
    "State": pl.Int64,
    "Goalie": pl.Int64,
    "Game ID": pl.Utf8,
    "Date": pl.Date,
    "Entry Type": pl.Utf8,
    "Entry By": pl.Utf8,
    "Defended by": pl.Utf8,
    "Pass?": pl.Utf8,
    "Lane": pl.Utf8,
    "Dump recovered?": pl.Utf8,
    "Chance?": pl.Utf8,
    "Retrieval": pl.Utf8,
    "Result": pl.Utf8,
    "Pressure": pl.Utf8,
    "Exit": pl.Utf8,
    "Result.1": pl.Utf8,
    "Home": pl.Utf8,
    "Road": pl.Utf8,
}

# ---- Helpers ----
def _clean_for_polars(df_pd: pd.DataFrame, schema: dict[str, pl.DataType]) -> pd.DataFrame:
    """
    Pre-clean pandas columns to avoid ArrowTypeError during pl.from_pandas.
    - Numeric cols -> to_numeric(errors="coerce")
    - Date/Datetime -> to_datetime(errors="coerce")
    - Time -> coerce to string (we'll parse in Polars)
    - Strings -> pandas string dtype (keeps NA cleanly)
    """
    out = df_pd.copy()

    for col, target in schema.items():
        if col not in out.columns:
            continue

        if target in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64):
            out[col] = pd.to_numeric(out[col], errors="coerce")

        elif target in (pl.Date, pl.Datetime):
            out[col] = pd.to_datetime(out[col], errors="coerce")

        elif target == pl.Time:
            # Time cells can be datetime/time/float/string depending on Excel;
            # force to string and parse in Polars.
            out[col] = out[col].astype("string")

        else:
            out[col] = out[col].astype("string")

    return out

def _apply_schema(df_pl: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    exprs = []

    for col, target in schema.items():
        if col not in df_pl.columns:
            continue

        if target == pl.Date:
            # If it's datetime, cast handles it.
            exprs.append(
                pl.col(col)
                  .cast(pl.Date, strict=False)
                  .alias(col)
            )

        elif target == pl.Time:
            exprs.append(
                pl.coalesce([
                    pl.col(col).cast(pl.Time, strict=False),
                    pl.col(col).str.strptime(pl.Time, format="%M:%S", strict=False),
                    pl.col(col).str.strptime(pl.Time, format="%H:%M:%S", strict=False),
                    pl.col(col).str.strptime(pl.Time, format="%H:%M", strict=False),
                ]).alias(col)
            )

        else:
            exprs.append(
                pl.col(col)
                  .cast(target, strict=False)
                  .alias(col)
            )

    return df_pl.with_columns(exprs)





# ---- Main ----

base_path = Path(r"C:/Users/kvmil/OneDrive/Documents/Project Zenith/Data/AllThreeZones")
gamelogs_path = base_path / "Raw Game Logs"

gamelogs = [f for f in gamelogs_path.iterdir() if f.is_file() and f.suffix.lower() in [".xlsx", ".xlsm", ".xls"]]

dfs: list[pl.DataFrame] = []

for game_path in gamelogs:
    print(game_path.name)

    df_pd = pd.read_excel(game_path, sheet_name="Tracking", engine="openpyxl")

    # Pre-clean to avoid pyarrow conversion errors (mixed objects)
    df_pd = _clean_for_polars(df_pd, SCHEMA)

    # Convert
    df_pl = pl.from_pandas(df_pd)

    # Apply schema
    df_pl = _apply_schema(df_pl, SCHEMA)

    df_pl = df_pl.with_columns(pl.lit(game_path.name).alias("filename"))

    dfs.append(df_pl)

combined_games = pl.concat(dfs, how="diagonal_relaxed")

combined_games = combined_games.drop_nulls(subset=["Period", "Time"])

combined_games_cleaned = (
    combined_games
    .with_columns(
        pl.when(pl.col("Team") == "L.A").then(pl.lit("LAK"))
        .when(pl.col("Team") == "N.J").then(pl.lit("NJD"))
        .when(pl.col("Team") == "S.J").then(pl.lit("SJS"))
        .when(pl.col("Team") == "T.B").then(pl.lit("TBL"))
        .otherwise(pl.col("Team"))
        .alias("triCode"),


        pl.when(pl.col("SC?").str.to_uppercase() == "Y")
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("scInd"),

        pl.when(pl.col("SOG?").str.to_uppercase() == "Y")
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("sogInd"),

        pl.when(pl.col("Screen?").str.to_uppercase() == "Y")
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("screenInd"),

        pl.when(pl.col("G?").str.to_uppercase() == "Y")
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("goalInd"),


        pl.when(pl.col("Retrieval").str.len_chars() > 1)
        .then(pl.col("Retrieval").str.head(pl.col("Retrieval").str.len_chars()-3))
        .otherwise(pl.col("Retrieval"))
        .alias("retrievalSkater"),

        pl.when(pl.col("Retrieval").str.len_chars() > 1)
        .then(pl.col("Retrieval").str.tail(3))
        .otherwise(pl.col("Retrieval"))
        .alias("retrievalTeam"),
        

        pl.when(pl.col("Pressure").str.len_chars() > 1)
        .then(pl.col("Pressure").str.head(pl.col("Pressure").str.len_chars()-3))
        .otherwise(pl.col("Pressure"))
        .alias("pressureSkater"),

        pl.when(pl.col("Pressure").str.len_chars() > 1)
        .then(pl.col("Pressure").str.tail(3))
        .otherwise(pl.col("Pressure"))
        .alias("pressureTeam"),


        pl.when(pl.col("Exit").str.len_chars() > 1)
        .then(pl.col("Exit").str.head(pl.col("Exit").str.len_chars()-3))
        .otherwise(pl.col("Exit"))
        .alias("exitSkater"),

        pl.when(pl.col("Exit").str.len_chars() > 1)
        .then(pl.col("Exit").str.tail(3))
        .otherwise(pl.col("Exit"))
        .alias("exitTeam"),


        (season + "0" + pl.col("filename").str.head(5)).alias("gameId"),

        pl.col("Shot Type").str.to_lowercase().alias("shotAbbv"),
        pl.col("Rush?").str.to_uppercase().alias("rush"),
        pl.col("Origin").str.strip_chars().alias("origin"),
        pl.col("Entry Type").str.strip_chars().alias("entryType"),
    )
    .with_columns(
        pl.when(pl.col("shotAbbv") == "a").then(pl.lit("wrap-around"))
        .when(pl.col("shotAbbv") == "b").then(pl.lit("backhander"))
        .when(pl.col("shotAbbv") == "s").then(pl.lit("slapshot"))
        .when(pl.col("shotAbbv") == "w").then(pl.lit("wrist shot"))
        .when(pl.col("shotAbbv") == "o").then(pl.lit("one-timer"))
        .when(pl.col("shotAbbv") == "r").then(pl.lit("rebound"))
        .when(pl.col("shotAbbv") == "t").then(pl.lit("tip/deflection"))
        .otherwise(pl.col("shotAbbv"))
        .alias("shotType"),
    )
)

combined_games_cleaned = combined_games_cleaned.select("Period","Time","Strength","triCode","Shooter","shotType","A1","A2","A3","A1 Zone","A2 Zone","A3 Zone",
                                                    "scInd","sogInd","screenInd","rush","origin","Context","Oddman?","goalInd","State","Goalie","gameId",
                                                    "entryType","Entry By","Defended by","Pass?","Lane","Dump recovered?","Chance?",
                                                    "Retrieval","retrievalSkater","retrievalTeam","Pressure","pressureSkater","pressureTeam","Exit","exitSkater","exitTeam",
                                                    "Result","Result.1","filename",)




# Shooter, A1, A2, A3, Goalie: going to need to lookup

# Origin, Entry Type: trim "dz ", " D"
# Context: a good bit of cleaning
# Entry by, Defended by, Dump recovered?, Retrieval, Pressure, Exit,: split player/team
# Pass?: Y, N, other
# Lane: C, L, R, all the other shit
# Result, Result.1: mad cleaning



print("opening file")
combined_games_cleaned.write_excel("_peek.xlsx", autofit=True); __import__("os").startfile("_peek.xlsx")
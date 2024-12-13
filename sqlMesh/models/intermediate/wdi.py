import typing as t
from datetime import datetime

import ibis
import ibis.selectors as s
import pandas as pd
from constants import DB_PATH  # type: ignore
from sqlglot import exp
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import ModelKindName


@model(
    "intermediate.wdi",
    kind="FULL",
    columns={
        "country_id": "text",
        "indicator_id": "text",
        "year": "int",
        "value": "text",
        "indicator_label": "text",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # connect ibis to database
    con = ibis.duckdb.connect(DB_PATH)

    """Process WDI data and return the transformed Ibis table."""

    print("Starting wdi_data")
    wdi_table = context.table("wdi.csv")
    wdi_df = context.fetchdf(f"SELECT * FROM {wdi_table}")
    wdi_data = (
        ibis.memtable(wdi_df, name="wdi")
        .rename("snake_case")
        .pivot_longer(s.r["1960":], names_to="year", values_to="value")
        .cast({"year": "int64"})
        .rename(country_id="country_code", indicator_id="indicator_code")
    )
    print("Completed wdi_data")

    print("Starting wdi_label")
    wdi_label_table = context.table("wdi.series")
    wdi_label_df = context.fetchdf(f"SELECT * FROM {wdi_label_table}")
    wdi_label = (
        ibis.memtable(wdi_label_df, name="wdi_label")
        .rename("snake_case")
        .rename(indicator_id="series_code", indicator_label="indicator_name")
    )
    print("Completed wdi_label")

    print("Starting wdi_label")
    wdi = (
        wdi_data.join(
            wdi_label, wdi_data.indicator_id == wdi_label.indicator_id, how="left"
        )
        .cast({"year": "int64"})
        .select("country_id", "indicator_id", "year", "value", "indicator_label")
    )

    wdi_df = wdi.to_pandas()
    return wdi_df

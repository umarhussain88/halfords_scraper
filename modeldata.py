from pathlib import Path
import pandas as pd
import json
import numpy as np


def track_price_changes():

    df = (
        pd.concat(
            [
                pd.json_normalize(json.load(open(f, "r")))
                for f in Path(
                    r"C:\Users\umarh\OneDrive\Documents\2020\cmder\projects\scrapy_udemy\halfords"
                )
                .joinpath("raw")
                .rglob("*.json")
            ],
            keys=[
                f.stem.split("_")[0]
                for f in Path(
                    r"C:\Users\umarh\OneDrive\Documents\2020\cmder\projects\scrapy_udemy\halfords"
                )
                .joinpath("raw")
                .rglob("*.json")
            ],
        )
        .reset_index(0)
        .rename(columns={"level_0": "ingestionDate"})
    )

    df["ingestionDate"] = pd.to_datetime(
        df["ingestionDate"], format="%Y-%m-%dT%H-%M-%S"
    )

    df1 = df[
        [
            "product.gtmCategory",
            "product.productName",
            "product.categoryId",
            "product.id",
            "product.price.sales.value",
            "product.brand",
            "ingestionDate",
        ]
    ].drop_duplicates()

    df = pd.concat(
        [
            df1,
            df1["product.gtmCategory"]
            .str.split("/", expand=True)
            .rename(columns=lambda x: f"level_{x}"),
        ],
        axis=1,
    ).drop("product.gtmCategory", 1)

    """for each product, track the change in price by ingestionDate."""

    df2 = (
        df.groupby(["ingestionDate", "product.productName", "product.brand"])[
            "product.price.sales.value"
        ]
        .sum()
        .to_frame()
        .reset_index(1)
    )

    df2 = (
        df.groupby(["ingestionDate", "product.productName", "product.brand"])[
            "product.price.sales.value"
        ]
        .sum()
        .to_frame()
        .reset_index(1)
    ).drop_duplicates(["product.price.sales.value", "product.productName"], keep="last")

    df3 = (
        df2[df2.duplicated(subset=["product.productName"], keep=False)]
        .reset_index()
        .sort_values(["product.productName", "ingestionDate"])
    )

    df4 = df3.assign(
        dailyDiff=df3.groupby(["product.productName", "product.brand"])[
            "product.price.sales.value"
        ].diff(),
        pctChange=df3.groupby(["product.productName"])[
            "product.price.sales.value"
        ].pct_change()
        * 100,
    )

    df4["pctChange"] = np.where(df4["pctChange"].eq(np.inf), np.nan, df4["pctChange"])

    df5 = pd.crosstab(
        [df4["product.brand"], df4["product.productName"]],
        df4["ingestionDate"],
        df4["product.price.sales.value"],
        aggfunc="first",
    ).fillna("")

    df6 = pd.crosstab(
        [df4["product.brand"], df4["product.productName"]],
        df4["ingestionDate"],
        df4["pctChange"],
        aggfunc="first",
    ).fillna("")

    df7 = pd.crosstab(
        [df4["product.brand"], df4["product.productName"]],
        df4["ingestionDate"],
        df4["dailyDiff"],
        aggfunc="first",
    ).fillna("")

    return [df5, df6, df7]


if __name__ == "__main__":
    prices = track_price_changes()
    for df in prices:
        print(df)

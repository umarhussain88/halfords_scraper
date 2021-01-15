from pathlib import Path
import pandas as pd
import json
from import_data import import_data_to_db,engine


def get_latest_files(path=Path(r'halfords\spiders'), ext="json"):

    """Takes a source path and extension and returns the max file"""
    file_dict = {
        file: pd.Timestamp(file.stat().st_ctime, unit="s")
        for file in path.glob(f"*.{ext}")
    }

    return file_dict


def concat_dataframe(file_dictionary, max_date=True, unique_key="_", position=-1):

    """returns a dataframe according to config.
    file dictionary : dictionary of files with timestamp.

    max_date: if you want the file by max_date, 
    if false returns all files -- better to log refactor me.

    unique_key: if running multiple spiders i.e bikes, 
    cars then the file name should be the key.

    position: position of the key name after 
    splitting bikes_01012020.json < bikes is in position 0 
    """
    dfs = []
    if max_date:
        meta_df = (
            pd.DataFrame.from_dict(file_dictionary, orient="index")
            .reset_index()
            .rename(columns={"index": "name", 0: "time"})
        )

        meta_df["unique_files"] = meta_df["name"].apply(
            lambda x: x.stem.split(unique_key)[position]
        )

        for _, row in meta_df.loc[
            meta_df.groupby("unique_files")["time"].idxmax()
        ].iterrows():
            with open(row["name"], "r") as f:
                json_obj = json.load(f)
                dfs.append(
                    pd.json_normalize(json_obj).assign(ingestionDate=row["time"])
                )
    else:

        for file, date in file_dictionary.items():
            with open(file, "r") as f:
                json_ob = json.load(f)
                dfs.append(pd.json_normalize(json_ob).assign(ingestionDate=date))

        return pd.concat(dfs)


def clean_halfords_json(dataframe):

    """Takes a raw dataframe and returns a dataframe ready for modelling."""

    df1 = dataframe[
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

    return pd.concat(
        [
            df1,
            df1["product.gtmCategory"]
            .str.split("/", expand=True)
            .rename(columns=lambda x: f"level_{x}"),
        ],
        axis=1,
    ).drop("product.gtmCategory", 1)


def parse_files_into_raw_curated(
    file_paths, dataframe, ext="json", key="IngestionDate"
):
    """Moves file into a proper folder structure, mimicing structure in a datalake."""

    curated = Path.cwd().joinpath('data',"curated")
    raw = Path.cwd().joinpath('data',"raw")

    for path in [curated, raw]:
        if not path.is_dir():
            path.mkdir(parents=True, exist_ok=False)

    for file, _ in file_paths.items():
        date, key = file.stem.split("_")
        date = pd.to_datetime(date, format="%Y-%m-%dT%H-%M-%S")
        data_lake_path = (
            "product",
            key,
            f"year={date.year}",
            f"month={date.month}",
            f"day={date.day}",
            f"hour={date.hour}",
            f"minute={date.minute}",
            f"second={date.second}",
        )

        if not raw.joinpath(*data_lake_path).is_dir():
            raw.joinpath(*data_lake_path).mkdir(parents=True)

        file.rename(raw.joinpath(*data_lake_path).joinpath(file.name))



    c_path = curated.joinpath(pd.Timestamp("today").strftime("%Y_%m_%d_%H_%M"))
    if not c_path.is_dir():
        c_path.mkdir(parents=True)

    dataframe.to_csv(
        c_path.joinpath("halfords_products.csv.gz"), compression="gzip", index=False
    )

    return dataframe 
    


if __name__ == "__main__":
    file_paths = get_latest_files(path=Path(r'halfords\spiders'), ext="json")
    concat_dfs = concat_dataframe(
        file_paths, max_date=False, unique_key="_", position=-1
    )
    clean_df = clean_halfords_json(concat_dfs)
    dataframe = parse_files_into_raw_curated(file_paths=file_paths, dataframe=clean_df)
    #write to database.
    import_data_to_db(dataframe,connection=engine,schema='ext_hal',table_name='product_data')


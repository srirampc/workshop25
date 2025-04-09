import marimo

__generated_with = "0.12.6"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import duckdb
    import os.path
    import requests

    data_path = mo.notebook_location() / "public" / "gouwens.db"
    files_dir =  mo.notebook_location() / "public"
    lines_file = files_dir / "41593_2019_417_MOESM4_ESM.xlsx"
    types_file = files_dir / "41593_2019_417_MOESM5_ESM.xlsx"
    return (
        data_path,
        duckdb,
        files_dir,
        lines_file,
        mo,
        os,
        pl,
        requests,
        types_file,
    )


@app.cell
def _(duckdb, files_dir, lines_file, pl, requests, types_file):
    def fetch_file(url, out_file):
        response = requests.get(url, params={"downloadformat": "xlsx"})
        with open(out_file, mode="wb") as rfx:
            rfx.write(response.content)


    def fetch_files(out_dir):
        lines_data = "https://static-content.springer.com/esm/art%3A10.1038%2Fs41593-019-0417-0/MediaObjects/41593_2019_417_MOESM4_ESM.xlsx"
        types_data = "https://static-content.springer.com/esm/art%3A10.1038%2Fs41593-019-0417-0/MediaObjects/41593_2019_417_MOESM5_ESM.xlsx"
        fetch_file(lines_data, lines_file)
        fetch_file(types_data, types_file)


    def load_data():
        cdf = pl.read_excel(lines_file)
        rdf = cdf.slice(offset=1, length=32)
        rdf.columns = list(cdf.row(0))
        gdf = cdf.slice(offset=35, length=5)
        gdf.columns = list(cdf.row(34))
        mdf = pl.read_excel(types_file)
        return rdf, gdf, mdf


    def save_db(data_path):
        fetch_files(files_dir)
        rdf, gdf, mdf = load_data()
        with duckdb.connect(data_path) as conn:
            conn.execute(
                "CREATE OR REPLACE TABLE driver_lines AS SELECT * FROM rdf"
            )
            conn.execute(
                "CREATE OR REPLACE TABLE reporter_lines AS SELECT * FROM gdf"
            )
            conn.execute(
                "CREATE OR REPLACE TABLE me_types AS SELECT * FROM mdf"
            )
            conn.commit()

    # if not os.path.exists(data_path):
    #    save_db()
    # save_db(data_path)
    return fetch_file, fetch_files, load_data, save_db


@app.cell
def _(load_data):
    lines_df, gdf, features_df = load_data()
    return features_df, gdf, lines_df


@app.cell
def _(features_df):
    import quak
    quak.Widget(features_df)
    return (quak,)


@app.cell
def _(lines_df):
    lines_df
    return


if __name__ == "__main__":
    app.run()

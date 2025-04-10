import marimo

__generated_with = "0.12.6"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import openpyxl
    import duckdb
    import os.path
    import requests
    import altair as alt

    data_path = mo.notebook_location() / "public" / "gouwens.db"
    files_loc =  mo.notebook_location() / "public"
    lines_file = files_loc / "41593_2019_417_MOESM4_ESM.xlsx"
    types_file = files_loc / "41593_2019_417_MOESM5_ESM.xlsx"
    return (
        alt,
        data_path,
        duckdb,
        files_loc,
        lines_file,
        mo,
        openpyxl,
        os,
        pl,
        requests,
        types_file,
    )


@app.cell(hide_code=True)
def _(files_loc, mo):
    image_location = (
        str(mo.notebook_location() / "public" / "features.png")
        if str(files_loc).startswith("http") else  "/public/features.png" 
    )

    mo.md(f"""![Images]({image_location})""")
    return (image_location,)


@app.cell(hide_code=True)
def _(files_loc, mo):
    rimage_location = (
        str(mo.notebook_location() / "public" / "lines.png")
        if str(files_loc).startswith("http") else  "/public/lines.png" 
    )
    mo.md(f"""![Images]({rimage_location})""")
    return (rimage_location,)


@app.cell
def _(duckdb, files_dir, lines_file, pl, requests, types_file):
    def fetch_file(url, out_file):
        response = requests.get(
            url,
            params={"downloadformat": "xlsx"}
        )
        with open(out_file, mode="wb") as rfx:
            rfx.write(response.content)


    def fetch_files(out_dir):
        lines_data = "https://static-content.springer.com/esm/art%3A10.1038%2Fs41593-019-0417-0/MediaObjects/41593_2019_417_MOESM4_ESM.xlsx"
        types_data = "https://static-content.springer.com/esm/art%3A10.1038%2Fs41593-019-0417-0/MediaObjects/41593_2019_417_MOESM5_ESM.xlsx"
        fetch_file(lines_data, lines_file)
        fetch_file(types_data, types_file)


    def load_data(engine='openpyxl'):
        cdf = pl.read_excel(lines_file, columns=range(12), has_header=False, engine='openpyxl')
        rdf = cdf.slice(offset=2, length=33)
        rdf.columns = list(cdf.row(1))
        gdf = cdf.slice(offset=36, length=5)
        gdf.columns = list(cdf.row(35))
        mdf = pl.read_excel(types_file, engine='openpyxl')
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
def _(alt, features_df, mo):

    me_df = features_df.filter(
        features_df.get_column("me-type").is_not_null()
    )

    mo.ui.altair_chart(
        alt.Chart(me_df).mark_circle().encode(
            alt.Y("e-type"),
            alt.X("me-type"),
            size="count()"
        )
    )
    return (me_df,)


@app.cell
def _(alt, me_df, mo):
    mo.ui.altair_chart(
        alt.Chart(me_df).mark_circle().encode(
            alt.Y("m-type"),
            alt.X("me-type"),
            size="count()"
        )
    )
    return


@app.cell
def _(lines_df):
    lines_df
    return


if __name__ == "__main__":
    app.run()

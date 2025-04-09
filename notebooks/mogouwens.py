import marimo

__generated_with = "0.12.6"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import duckdb
    import os.path

    data_path = mo.notebook_location() / "public" / "gouwens.db"
    return data_path, duckdb, mo, os, pl


@app.cell
def _(duckdb, pl):
    def fetch_data():
        lines_data = "https://static-content.springer.com/esm/art%3A10.1038%2Fs41593-019-0417-0/MediaObjects/41593_2019_417_MOESM4_ESM.xlsx",
        types_data = "https://static-content.springer.com/esm/art%3A10.1038%2Fs41593-019-0417-0/MediaObjects/41593_2019_417_MOESM5_ESM.xlsx"
        cdf = pl.read_excel(lines_data)
        rdf = cdf.slice(offset=1, length=32)
        rdf.columns = list(cdf.row(0))
        gdf = cdf.slice(offset=35, length=5)
        gdf.columns = list(cdf.row(34))
        mdf = pl.read_excel(types_data)
        return rdf, gdf, mdf

    def save_db(data_path):
        rdf, gdf, mdf = fetch_data()
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

    # if not os.path.exists("gouwens.db"):
    #    save_db()
    return fetch_data, save_db


@app.cell
def _(data_path, duckdb):

    db_conn = duckdb.connect(data_path, read_only=True)
    return (db_conn,)


@app.cell
def _(db_conn, me_types, mo):
    import quak
    mdf = mo.sql(
        f"""
        SELECT * FROM me_types
        """,
        engine=db_conn,
    ) 
    quak.Widget(mdf)
    return mdf, quak


@app.cell
def _(db_conn, driver_lines, mo):
    drdf = mo.sql(
        f"""
        SELECT * FROM driver_lines
        """,
        engine=db_conn,
    )
    drdf
    return (drdf,)


if __name__ == "__main__":
    app.run()

import marimo

__generated_with = "0.11.22"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    dbPath = "/Users/vhe/Code/DevoxxFr2025/data/bike.db"
    _df = mo.sql(
        f"""
        ATTACH IF NOT EXISTS '{dbPath}'
        """
    )
    return dbPath, null


@app.cell
def _(bike, mo, trips_view):
    _df = mo.sql(
        f"""
        SELECT * FROM bike.trips_view LIMIT 10
        """
    )
    return


@app.cell
def _(bike, mo, trips_view):
    _df = mo.sql(
        f"""
        -- All stations
        SELECT DISTINCT start_station_name
        FROM bike.trips_view
        """
    )
    return


@app.cell
def _(bike, mo, trips_view):
    _df = mo.sql(
        f"""
        -- All stations locations
        SELECT DISTINCT start_station_name, start_lat, start_lng
        FROM bike.trips_view
        WHERE start_lat IS NOT NULL AND start_lng IS NOT NULL;
        """
    )
    return


@app.cell
def _(bike, mo, trips_view):
    _df = mo.sql(
        f"""
        -- Hotpspots departures
        SELECT start_station_name, COUNT(*) AS nb_departures
        FROM bike.trips_view
        GROUP BY start_station_name
        ORDER BY nb_departures DESC
        LIMIT 10;
        """
    )
    return


@app.cell
def _(bike, mo, trips_view):
    _df = mo.sql(
        f"""
        SELECT end_station_name, COUNT(*) AS nb_arrivals
        FROM bike.trips_view
        GROUP BY end_station_name
        ORDER BY nb_arrivals DESC
        LIMIT 10;
        """
    )
    return


@app.cell
def _(bike, mo, trips_view):
    _df = mo.sql(
        f"""
        -- Popular trajets
        SELECT * FROM (
        SELECT start_station_name, end_station_name, COUNT(*) AS trajets
        FROM bike.trips_view
        GROUP BY start_station_name, end_station_name
        )
        WHERE start_station_name != end_station_name
        ORDER BY trajets DESC
        LIMIT 20;
        """
    )
    return


@app.cell
def _(bike, mo, trips_view):
    _df = mo.sql(
        f"""
        -- Tourists rides
        SELECT * FROM (
        SELECT start_station_name, end_station_name, COUNT(*) AS trajets
        FROM bike.trips_view
        GROUP BY start_station_name, end_station_name
        )
        WHERE start_station_name == end_station_name
        ORDER BY trajets DESC
        LIMIT 20;
        """
    )
    return


@app.cell
def _(bike, mo, trips_view):
    _df = mo.sql(
        f"""
        SELECT start_station_name, end_station_name,
            6371 * 2 * ASIN(SQRT(
                POWER(SIN(RADIANS((end_lat - start_lat) / 2)), 2) +
                COS(RADIANS(start_lat)) * COS(RADIANS(end_lat)) *
                POWER(SIN(RADIANS((end_lng - start_lng) / 2)), 2)
            )) AS approx_km
        FROM bike.trips_view
        WHERE start_lat IS NOT NULL AND end_lat IS NOT NULL

        ;
        """
    )
    return


@app.cell
def _(bike, mo, trips_view):
    bins_df = mo.sql(
        f"""
        SELECT 
            ROUND(start_lat, 3) AS lat_bin,
            ROUND(start_lng, 3) AS lng_bin,
            COUNT(*) AS nb_rides
        FROM bike.trips_view
        WHERE start_lat IS NOT NULL
        GROUP BY lat_bin, lng_bin
        ORDER BY nb_rides DESC
        """
    )
    return (bins_df,)


@app.cell
def _(bike, mo, trips_view):
    _df = mo.sql(
        f"""
        SELECT DISTINCT(start_station_name)
        FROM bike.trips_view
        WHERE ROUND(start_lat, 2) = 40.73 AND ROUND(start_lng, 2) = -73.99;
        """
    )
    return


@app.cell
def _():
    import plotly.express as px
    return (px,)


@app.cell
def _(bins_df, px):
    # Carte interactive
    fig = px.scatter_mapbox(
        bins_df,
        lat="lat_bin",
        lon="lng_bin",
        size="nb_rides",
        color="nb_rides",
        size_max=30,
        zoom=10,
        mapbox_style="open-street-map",
        title="Heatmap des trajets Citi Bike par zone (~1km²)"
    )

    fig.show()
    return (fig,)


@app.cell
def _(bike, mo, trips_view):
    _df = mo.sql(
        f"""
        SELECT COUNT(*) FROM bike.trips_view
        """
    )
    return


if __name__ == "__main__":
    app.run()

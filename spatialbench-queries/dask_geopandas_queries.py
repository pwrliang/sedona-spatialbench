"""
Dask-GeoPandas implementation of SpatialBench queries (Q1-Q12).
Evaluates out-of-core and chunked spatial operations over partitioned datasets.
"""

from typing import cast
import dask
import dask.dataframe as dd
import dask_geopandas as dgpd
import geopandas as gpd
import numpy as np
import pandas as pd
from pandas import DataFrame
from shapely import wkb
from shapely.geometry import LineString, MultiPoint, Point, Polygon

# Ensure raw binary WKB bytes columns are not coerced to pyarrow strings
dask.config.set({"dataframe.convert-string": False})


def _to_gdf_point(df, loc_col: str, geom_col: str = "geometry", crs="EPSG:4326") -> gpd.GeoDataFrame:
    geoms = gpd.GeoSeries.from_wkb(df[loc_col], crs=crs)
    gdf = gpd.GeoDataFrame(df, geometry=geoms, crs=crs)
    if geom_col != "geometry":
        gdf = gdf.rename_geometry(geom_col)
    return gdf


def _to_gdf_poly(df, poly_col: str, geom_col: str = "geometry", crs="EPSG:4326") -> gpd.GeoDataFrame:
    geoms = gpd.GeoSeries.from_wkb(df[poly_col], crs=crs)
    gdf = gpd.GeoDataFrame(df, geometry=geoms, crs=crs)
    if geom_col != "geometry":
        gdf = gdf.rename_geometry(geom_col)
    return gdf


def q1(data_paths: dict[str, str]) -> DataFrame:
    """Q1 (Dask-GeoPandas): Trips starting within 50km of Sedona city center."""
    ddf = dd.read_parquet(
        data_paths["trip"],
        columns=["t_tripkey", "t_pickuploc", "t_pickuptime"]
    )
    center = Point(-111.7610, 34.8697)

    def process_partition(df):
        gdf = _to_gdf_point(df, "t_pickuploc", "pickup_geom")
        gdf["pickup_lon"] = gdf.geometry.x
        gdf["pickup_lat"] = gdf.geometry.y
        gdf["distance_to_center"] = gdf.geometry.distance(center)
        filtered = gdf[gdf["distance_to_center"].notna() & (gdf["distance_to_center"] <= 0.45)]
        return filtered[["t_tripkey", "pickup_lon", "pickup_lat", "t_pickuptime", "distance_to_center"]]

    meta = pd.DataFrame({
        "t_tripkey": pd.Series(dtype="int64"),
        "pickup_lon": pd.Series(dtype="float64"),
        "pickup_lat": pd.Series(dtype="float64"),
        "t_pickuptime": pd.Series(dtype="datetime64[ns]"),
        "distance_to_center": pd.Series(dtype="float64"),
    })
    filtered_ddf = ddf.map_partitions(process_partition, meta=meta)
    res = filtered_ddf.compute()
    return res.sort_values(
        ["distance_to_center", "t_tripkey"], ascending=[True, True]
    ).head(100).reset_index(drop=True)


def q2(data_paths: dict[str, str]) -> DataFrame:
    """Q2 (Dask-GeoPandas): Count trips starting within Coconino County zone."""
    zone_df = pd.read_parquet(data_paths["zone"])
    target = zone_df[zone_df["z_name"] == "Coconino County"].head(1)
    if target.empty:
        return pd.DataFrame({"trip_count_in_coconino_county": [0]})
    poly = wkb.loads(target.iloc[0]["z_boundary"])

    ddf = dd.read_parquet(data_paths["trip"], columns=["t_pickuploc"])

    def count_intersects(df):
        geoms = gpd.GeoSeries.from_wkb(df["t_pickuploc"], crs="EPSG:4326")
        return pd.Series([int(geoms.intersects(poly).sum())])

    counts_ddf = ddf.map_partitions(count_intersects, meta=pd.Series(dtype="int64"))
    total_count = int(counts_ddf.sum().compute())
    return pd.DataFrame({"trip_count_in_coconino_county": [total_count]})


def q3(data_paths: dict[str, str]) -> DataFrame:
    """Q3 (Dask-GeoPandas): Monthly trip stats for a buffered box around Sedona center."""
    ddf = dd.read_parquet(
        data_paths["trip"],
        columns=["t_tripkey", "t_pickuploc", "t_distance", "t_pickuptime", "t_dropofftime", "t_fare"]
    )
    base_poly = Polygon([
        (-111.9060, 34.7347),
        (-111.6160, 34.7347),
        (-111.6160, 35.0047),
        (-111.9060, 35.0047),
        (-111.9060, 34.7347),
    ])
    buffered_box = base_poly.buffer(0.045)

    def process_partition(df):
        geoms = gpd.GeoSeries.from_wkb(df["t_pickuploc"], crs="EPSG:4326")
        mask = geoms.intersects(buffered_box)
        sub = df[mask].copy()
        if sub.empty:
            return pd.DataFrame(columns=["pickup_month", "t_tripkey", "t_distance", "duration_seconds", "t_fare"])
        sub["pickup_month"] = sub["t_pickuptime"].dt.to_period("M").dt.to_timestamp()
        sub["duration_seconds"] = (sub["t_dropofftime"] - sub["t_pickuptime"]).dt.total_seconds()
        return sub[["pickup_month", "t_tripkey", "t_distance", "duration_seconds", "t_fare"]]

    meta = pd.DataFrame({
        "pickup_month": pd.Series(dtype="datetime64[ns]"),
        "t_tripkey": pd.Series(dtype="int64"),
        "t_distance": pd.Series(dtype="float64"),
        "duration_seconds": pd.Series(dtype="float64"),
        "t_fare": pd.Series(dtype="float64"),
    })
    sub_ddf = ddf.map_partitions(process_partition, meta=meta)
    grouped = sub_ddf.groupby("pickup_month").agg(
        total_trips=("t_tripkey", "count"),
        avg_distance=("t_distance", "mean"),
        avg_duration=("duration_seconds", "mean"),
        avg_fare=("t_fare", "mean"),
    ).reset_index().compute()
    return grouped.sort_values("pickup_month").reset_index(drop=True)


def q4(data_paths: dict[str, str]) -> DataFrame:
    """Q4 (Dask-GeoPandas): Zone distribution of top 1000 trips by tip amount."""
    ddf = dd.read_parquet(data_paths["trip"], columns=["t_tripkey", "t_pickuploc", "t_tip"])
    
    def process_partition(df):
        sub = df.copy()
        sub["t_tip"] = sub["t_tip"].astype(float)
        return sub.sort_values(["t_tip", "t_tripkey"], ascending=[False, True]).head(1000)

    meta = pd.DataFrame({
        "t_tripkey": pd.Series(dtype="int64"),
        "t_pickuploc": pd.Series(dtype="object"),
        "t_tip": pd.Series(dtype="float64"),
    })
    sub_ddf = ddf.map_partitions(process_partition, meta=meta)
    top_trips = sub_ddf.compute().sort_values(["t_tip", "t_tripkey"], ascending=[False, True]).head(1000)
    if top_trips.empty:
        return pd.DataFrame(columns=["z_zonekey", "z_name", "trip_count"])

    top_trips["pickup_geom"] = gpd.GeoSeries.from_wkb(top_trips["t_pickuploc"], crs="EPSG:4326")
    top_gdf = gpd.GeoDataFrame(top_trips, geometry="pickup_geom", crs="EPSG:4326")

    zone_df = pd.read_parquet(data_paths["zone"])[["z_zonekey", "z_name", "z_boundary"]]
    zone_df["zone_geom"] = gpd.GeoSeries.from_wkb(zone_df["z_boundary"], crs="EPSG:4326")
    zones_gdf = gpd.GeoDataFrame(zone_df, geometry="zone_geom", crs="EPSG:4326")[["z_zonekey", "z_name", "zone_geom"]]

    result = (
        gpd.sjoin(top_gdf, zones_gdf, how="inner", predicate="within")
        .groupby(["z_zonekey", "z_name"], as_index=False)
        .size()
        .rename(columns={"size": "trip_count"})
        .sort_values(["trip_count", "z_zonekey"], ascending=[False, True])
        .reset_index(drop=True)
    )
    return result


def q5(data_paths: dict[str, str]) -> DataFrame:
    """Q5 (Dask-GeoPandas): Monthly travel patterns for repeat customers."""
    cust_df = pd.read_parquet(data_paths["customer"])[["c_custkey", "c_name"]]
    ddf = dd.read_parquet(
        data_paths["trip"],
        columns=["t_tripkey", "t_custkey", "t_dropoffloc", "t_pickuptime"]
    )

    def process_partition(df):
        sub = df.dropna(subset=["t_custkey", "t_dropoffloc"]).copy()
        sub["pickup_month"] = sub["t_pickuptime"].dt.to_period("M").dt.to_timestamp()
        sub["dropoff_geom"] = gpd.GeoSeries.from_wkb(sub["t_dropoffloc"], crs="EPSG:4326")
        return sub[["t_tripkey", "t_custkey", "pickup_month", "dropoff_geom"]]

    meta = pd.DataFrame({
        "t_tripkey": pd.Series(dtype="int64"),
        "t_custkey": pd.Series(dtype="int64"),
        "pickup_month": pd.Series(dtype="datetime64[ns]"),
        "dropoff_geom": gpd.GeoSeries([], crs="EPSG:4326"),
    })
    sub_ddf = ddf.map_partitions(process_partition, meta=meta)
    
    # Compute counts per customer and month first to filter repeat customers
    counts = sub_ddf.groupby(["t_custkey", "pickup_month"]).size().reset_index()
    counts.columns = ["t_custkey", "pickup_month", "trip_count"]
    repeat = counts[counts["trip_count"] > 5].compute()
    if repeat.empty:
        return pd.DataFrame(columns=["c_custkey", "customer_name", "pickup_month", "monthly_travel_hull_area", "dropoff_count"])

    # Pull only repeat records
    repeat_set = set(zip(repeat["t_custkey"], repeat["pickup_month"]))
    
    def filter_repeat(df):
        mask = [pair in repeat_set for pair in zip(df["t_custkey"], df["pickup_month"])]
        return df[mask]
        
    filtered_ddf = sub_ddf.map_partitions(filter_repeat, meta=meta)
    collected = filtered_ddf.compute()
    
    joined = collected.merge(cust_df, left_on="t_custkey", right_on="c_custkey", how="inner")
    grouped = (
        joined.groupby(["c_custkey", "c_name", "pickup_month"], as_index=False)
        .agg(
            trip_count=("t_tripkey", "count"),
            dropoff_points=("dropoff_geom", lambda x: list(x)),
        )
    )
    grouped["monthly_travel_hull_area"] = gpd.GeoSeries(
        grouped["dropoff_points"].map(MultiPoint), crs="EPSG:4326"
    ).convex_hull.area

    result = (
        grouped.sort_values(
            ["monthly_travel_hull_area", "c_custkey", "pickup_month"],
            ascending=[False, True, True],
        )[["c_custkey", "c_name", "pickup_month", "monthly_travel_hull_area", "trip_count"]]
        .rename(columns={"c_name": "customer_name", "trip_count": "dropoff_count"})
        .head(100)
        .reset_index(drop=True)
    )
    return result


def q6(data_paths: dict[str, str]) -> DataFrame:
    """Q6 (Dask-GeoPandas): Zone statistics for trips intersecting a bounding box."""
    zone_df = pd.read_parquet(data_paths["zone"])
    zone_df["zone_geom"] = gpd.GeoSeries.from_wkb(zone_df["z_boundary"], crs="EPSG:4326")
    zones_gdf = gpd.GeoDataFrame(zone_df, geometry="zone_geom", crs="EPSG:4326")[["z_zonekey", "z_name", "zone_geom"]]

    bbox_poly = Polygon([
        (-112.2110, 34.4197),
        (-111.3110, 34.4197),
        (-111.3110, 35.3197),
        (-112.2110, 35.3197),
        (-112.2110, 34.4197),
    ])
    candidate_zones = zones_gdf[
        zones_gdf["zone_geom"].notna() & zones_gdf["zone_geom"].intersects(bbox_poly)
    ].reset_index(drop=True)

    ddf = dd.read_parquet(
        data_paths["trip"],
        columns=["t_tripkey", "t_pickuploc", "t_distance", "t_pickuptime", "t_dropofftime"]
    )

    def process_partition(df):
        gdf = _to_gdf_point(df, "t_pickuploc", "pickup_geom")
        joined = gpd.sjoin(gdf, candidate_zones, how="inner", predicate="within")
        if joined.empty:
            return pd.DataFrame(columns=["z_zonekey", "z_name", "t_tripkey", "t_distance", "duration_seconds"])
        joined["duration_seconds"] = (joined["t_dropofftime"] - joined["t_pickuptime"]).dt.total_seconds()
        return joined[["z_zonekey", "z_name", "t_tripkey", "t_distance", "duration_seconds"]]

    meta = pd.DataFrame({
        "z_zonekey": pd.Series(dtype="int64"),
        "z_name": pd.Series(dtype="object"),
        "t_tripkey": pd.Series(dtype="int64"),
        "t_distance": pd.Series(dtype="float64"),
        "duration_seconds": pd.Series(dtype="float64"),
    })
    joined_ddf = ddf.map_partitions(process_partition, meta=meta)
    res = (
        joined_ddf.groupby(["z_zonekey", "z_name"])
        .agg(
            total_pickups=("t_tripkey", "count"),
            avg_distance=("t_distance", "mean"),
            avg_duration=("duration_seconds", "mean"),
        )
        .reset_index()
        .compute()
    )
    return res.sort_values(["total_pickups", "z_zonekey"], ascending=[False, True]).reset_index(drop=True)


def q7(data_paths: dict[str, str]) -> DataFrame:
    """Q7 (Dask-GeoPandas): Detect route detours by comparing reported vs geometric distances."""
    ddf = dd.read_parquet(
        data_paths["trip"],
        columns=["t_tripkey", "t_pickuploc", "t_dropoffloc", "t_distance"]
    )

    def process_partition(df):
        sub = df[df["t_distance"] > 0].copy()
        if sub.empty:
            return pd.DataFrame(columns=["t_tripkey", "reported_distance_m", "line_distance_m", "detour_ratio"])
        p_geoms = gpd.GeoSeries.from_wkb(sub["t_pickuploc"], crs="EPSG:4326").to_numpy()
        d_geoms = gpd.GeoSeries.from_wkb(sub["t_dropoffloc"], crs="EPSG:4326").to_numpy()
        sub["reported_distance_m"] = sub["t_distance"].astype(float)
        line_lengths = np.fromiter(
            (
                LineString([pg, dg]).length / 0.000009
                if (pg is not None and dg is not None)
                else np.nan
                for pg, dg in zip(p_geoms, d_geoms, strict=False)
            ),
            dtype=float,
            count=len(sub),
        )
        sub["line_distance_m"] = line_lengths
        sub["detour_ratio"] = np.divide(
            sub["reported_distance_m"].to_numpy(dtype=float, copy=False),
            line_lengths,
            out=np.full_like(sub["reported_distance_m"].to_numpy(dtype=float, copy=False), np.nan),
            where=(line_lengths != 0.0),
        )
        return sub[["t_tripkey", "reported_distance_m", "line_distance_m", "detour_ratio"]].nlargest(
            100, ["detour_ratio", "reported_distance_m", "t_tripkey"]
        )

    meta = pd.DataFrame({
        "t_tripkey": pd.Series(dtype="int64"),
        "reported_distance_m": pd.Series(dtype="float64"),
        "line_distance_m": pd.Series(dtype="float64"),
        "detour_ratio": pd.Series(dtype="float64"),
    })
    sub_ddf = ddf.map_partitions(process_partition, meta=meta)
    res = sub_ddf.compute()
    return (
        res.sort_values(
            ["detour_ratio", "reported_distance_m", "t_tripkey"],
            ascending=[False, False, True],
            na_position="last",
        )
        .head(100)
        .reset_index(drop=True)
    )


def q8(data_paths: dict[str, str]) -> DataFrame:
    """Q8 (Dask-GeoPandas): Count nearby pickups for each building within ~500m."""
    buildings_df = pd.read_parquet(data_paths["building"])[["b_buildingkey", "b_name", "b_boundary"]]
    buildings_df["boundary_geom"] = gpd.GeoSeries.from_wkb(buildings_df["b_boundary"], crs="EPSG:4326")
    buildings_gdf = gpd.GeoDataFrame(buildings_df, geometry="boundary_geom", crs="EPSG:4326")[
        ["b_buildingkey", "b_name", "boundary_geom"]
    ]

    ddf = dd.read_parquet(data_paths["trip"], columns=["t_tripkey", "t_pickuploc"])
    threshold = 0.0045  # ~500m

    def process_partition(df):
        gdf = _to_gdf_point(df, "t_pickuploc", "pickup_geom")
        joined = buildings_gdf.sjoin(gdf, predicate="dwithin", distance=threshold)
        if joined.empty:
            return pd.DataFrame(columns=["b_buildingkey", "b_name", "pickup_count"])
        return (
            joined.groupby(["b_buildingkey", "b_name"], as_index=False)
            .size()
            .rename(columns={"size": "pickup_count"})
        )

    meta = pd.DataFrame({
        "b_buildingkey": pd.Series(dtype="int64"),
        "b_name": pd.Series(dtype="object"),
        "pickup_count": pd.Series(dtype="int64"),
    })
    joined_ddf = ddf.map_partitions(process_partition, meta=meta)
    res = (
        joined_ddf.groupby(["b_buildingkey", "b_name"])["pickup_count"]
        .sum()
        .reset_index()
        .rename(columns={"pickup_count": "nearby_pickup_count"})
        .compute()
    )
    return (
        res.sort_values(["nearby_pickup_count", "b_buildingkey"], ascending=[False, True])
        .head(100)
        .reset_index(drop=True)
    )


def q9(data_paths: dict[str, str]) -> DataFrame:
    """Q9 (Dask-GeoPandas): Building conflation via IoU detection."""
    buildings_df = pd.read_parquet(data_paths["building"])
    buildings_df["boundary_geom"] = gpd.GeoSeries.from_wkb(buildings_df["b_boundary"], crs="EPSG:4326")
    bdf = gpd.GeoDataFrame(buildings_df, geometry="boundary_geom", crs="EPSG:4326")[
        ["b_buildingkey", "boundary_geom"]
    ].rename(columns={"b_buildingkey": "building_key"})

    pairs = gpd.sjoin(bdf, bdf, how="inner", predicate="intersects")
    left_key_candidates = ["building_key_left", "building_key_1", "building_key"]
    right_key_candidates = ["building_key_right", "building_key_2"]
    left_key_col = next(c for c in left_key_candidates if c in pairs.columns)
    right_key_col = next((c for c in right_key_candidates if c in pairs.columns), None)
    if right_key_col is None:
        pairs["_building_key_right_temp"] = bdf.loc[pairs["index_right"], "building_key"].to_numpy()
        right_key_col = "_building_key_right_temp"

    pairs = pairs.rename(columns={left_key_col: "building_1", right_key_col: "building_2"}).rename_geometry("boundary_geom_1")
    pairs["boundary_geom_2"] = bdf.loc[pairs["index_right"], "boundary_geom"].to_numpy()
    pairs = pairs[pairs["building_1"] < pairs["building_2"]]

    boundary_geom_1_gs = gpd.GeoSeries(pairs["boundary_geom_1"], crs=pairs.crs)
    boundary_geom_2_gs = gpd.GeoSeries(pairs["boundary_geom_2"], crs=pairs.crs)
    pairs["area1"] = boundary_geom_1_gs.area
    pairs["area2"] = boundary_geom_2_gs.area
    intersection = boundary_geom_1_gs.intersection(boundary_geom_2_gs)
    pairs["overlap_area"] = intersection.area
    overlap = pairs["overlap_area"].to_numpy(dtype=float, copy=False)
    area1 = pairs["area1"].to_numpy(dtype=float, copy=False)
    area2 = pairs["area2"].to_numpy(dtype=float, copy=False)
    union = area1 + area2 - overlap
    iou = np.divide(overlap, union, out=np.zeros_like(overlap), where=union != 0.0)
    mask_union_zero = (union == 0.0) & (overlap > 0.0)
    if mask_union_zero.any():
        iou[mask_union_zero] = 1.0
    pairs["iou"] = iou
    result = (
        pairs[["building_1", "building_2", "area1", "area2", "overlap_area", "iou"]]
        .sort_values(["iou", "building_1", "building_2"], ascending=[False, True, True])
        .head(100)
        .reset_index(drop=True)
    )
    return cast(DataFrame, result)


def q10(data_paths: dict[str, str]) -> DataFrame:
    """Q10 (Dask-GeoPandas): Zone stats for trips starting within each zone."""
    zone_df = pd.read_parquet(data_paths["zone"])
    zone_df["zone_geom"] = gpd.GeoSeries.from_wkb(zone_df["z_boundary"], crs="EPSG:4326")
    zones_gdf = gpd.GeoDataFrame(zone_df, geometry="zone_geom", crs="EPSG:4326")[["z_zonekey", "z_name", "zone_geom"]]

    ddf = dd.read_parquet(
        data_paths["trip"],
        columns=["t_tripkey", "t_pickuploc", "t_distance", "t_pickuptime", "t_dropofftime"]
    )

    def process_partition(df):
        gdf = _to_gdf_point(df, "t_pickuploc", "pickup_geom")
        joined = gpd.sjoin(gdf, zones_gdf, how="inner", predicate="within")
        if joined.empty:
            return pd.DataFrame(columns=["z_zonekey", "z_name", "t_tripkey", "t_distance", "duration_seconds"])
        joined["duration_seconds"] = (joined["t_dropofftime"] - joined["t_pickuptime"]).dt.total_seconds()
        return joined[["z_zonekey", "z_name", "t_tripkey", "t_distance", "duration_seconds"]]

    meta = pd.DataFrame({
        "z_zonekey": pd.Series(dtype="int64"),
        "z_name": pd.Series(dtype="object"),
        "t_tripkey": pd.Series(dtype="int64"),
        "t_distance": pd.Series(dtype="float64"),
        "duration_seconds": pd.Series(dtype="float64"),
    })
    joined_ddf = ddf.map_partitions(process_partition, meta=meta)
    stats = (
        joined_ddf.groupby(["z_zonekey", "z_name"])
        .agg(
            num_trips=("t_tripkey", "count"),
            avg_distance=("t_distance", "mean"),
            avg_duration=("duration_seconds", "mean"),
        )
        .reset_index()
        .compute()
    )

    # Right join / merge with all zones to retain zones with 0 trips
    full_zones = zone_df[["z_zonekey", "z_name"]].rename(columns={"z_name": "pickup_zone"})
    merged = full_zones.merge(stats.rename(columns={"z_name": "pickup_zone"}), on=["z_zonekey", "pickup_zone"], how="left")
    merged["num_trips"] = merged["num_trips"].fillna(0).astype(int)
    return (
        merged.sort_values(["avg_duration", "z_zonekey"], ascending=[False, True], na_position="last")
        .head(100)
        .reset_index(drop=True)
    )


def q11(data_paths: dict[str, str]) -> DataFrame:
    """Q11 (Dask-GeoPandas): Count trips that cross between different zones."""
    zone_df = pd.read_parquet(data_paths["zone"])
    zone_df["zone_geom"] = gpd.GeoSeries.from_wkb(zone_df["z_boundary"], crs="EPSG:4326")
    zones_gdf = gpd.GeoDataFrame(zone_df, geometry="zone_geom", crs="EPSG:4326")[["z_zonekey", "zone_geom"]]

    ddf = dd.read_parquet(
        data_paths["trip"],
        columns=["t_tripkey", "t_pickuploc", "t_dropoffloc"]
    )

    def process_partition(df):
        p_gdf = _to_gdf_point(df[["t_tripkey", "t_pickuploc"]], "t_pickuploc", "pickup_geom")
        d_gdf = _to_gdf_point(df[["t_tripkey", "t_dropoffloc"]], "t_dropoffloc", "dropoff_geom")

        pj = gpd.sjoin(p_gdf, zones_gdf, how="inner", predicate="within").rename(columns={"z_zonekey": "z_pickup"})
        dj = gpd.sjoin(d_gdf, zones_gdf, how="inner", predicate="within").rename(columns={"z_zonekey": "z_dropoff"})

        m = pj[["t_tripkey", "z_pickup"]].merge(dj[["t_tripkey", "z_dropoff"]], on="t_tripkey", how="inner")
        cross = m[m["z_pickup"] != m["z_dropoff"]]
        return pd.Series([len(cross)])

    counts = ddf.map_partitions(process_partition, meta=pd.Series(dtype="int64")).sum().compute()
    return pd.DataFrame({"cross_zone_trip_count": [int(counts)]})


def q12(data_paths: dict[str, str]) -> DataFrame:
    """Q12 (Dask-GeoPandas): Rank trip pickups by average distance to their 5 nearest buildings."""
    buildings_df = pd.read_parquet(data_paths["building"])
    buildings_df["boundary_geom"] = gpd.GeoSeries.from_wkb(buildings_df["b_boundary"], crs="EPSG:4326")
    buildings_gdf = gpd.GeoDataFrame(buildings_df, geometry="boundary_geom", crs="EPSG:4326")
    building_geoms = buildings_gdf["boundary_geom"].to_list()
    building_keys = buildings_gdf["b_buildingkey"].to_numpy()

    ddf = dd.read_parquet(data_paths["trip"], columns=["t_tripkey", "t_pickuploc"])

    def process_partition(df):
        p_geoms = gpd.GeoSeries.from_wkb(df["t_pickuploc"], crs="EPSG:4326").to_list()
        trip_keys = df["t_tripkey"].to_numpy()
        results = []
        for i, pt in enumerate(p_geoms):
            dists = [pt.distance(geom) for geom in building_geoms]
            nearest_idx = np.lexsort((building_keys, dists))[:5]
            avg_distance = float(np.mean([dists[idx] for idx in nearest_idx]))
            results.append({"t_tripkey": trip_keys[i], "avg_distance_to_5_nearest": avg_distance})
        sub = pd.DataFrame(results)
        if sub.empty:
            return pd.DataFrame(columns=["t_tripkey", "avg_distance_to_5_nearest"])
        return sub.nlargest(100, ["avg_distance_to_5_nearest", "t_tripkey"])

    meta = pd.DataFrame({
        "t_tripkey": pd.Series(dtype="int64"),
        "avg_distance_to_5_nearest": pd.Series(dtype="float64"),
    })
    sub_ddf = ddf.map_partitions(process_partition, meta=meta)
    res = sub_ddf.compute()
    return (
        res.sort_values(["avg_distance_to_5_nearest", "t_tripkey"], ascending=[False, True])
        .head(100)
        .reset_index(drop=True)
    )

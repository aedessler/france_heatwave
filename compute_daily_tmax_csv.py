#!/usr/bin/env python3
"""Compute daily France-averaged TMax for every model/dataset and write to CSV."""

import argparse
import glob
import os
import gc
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from shapely.geometry import box, Point
from shapely.prepared import prep

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ALL_DATASETS = {
    "NEX-GDDP": os.path.join(BASE_DIR, "NEX-GDDP", "tmax_france_results"),
    "CIL-GDPCIR": os.path.join(BASE_DIR, "CIL-GDPCIR", "tmax_france_results"),
    "EURO-CORDEX": os.path.join(BASE_DIR, "EURO-CORDEX", "tmax_france_cordex_results"),
}

parser = argparse.ArgumentParser(description="Compute daily France-averaged TMax CSV.")
parser.add_argument("--nex", action="store_true", help="Include NEX-GDDP")
parser.add_argument("--cil", action="store_true", help="Include CIL-GDPCIR")
parser.add_argument("--euro", action="store_true", help="Include EURO-CORDEX")
parser.add_argument("--all", action="store_true", dest="use_all", help="Include all (default)")
args = parser.parse_args()

if not (args.nex or args.cil or args.euro):
    args.use_all = True

FLAG_MAP = {"NEX-GDDP": args.nex, "CIL-GDPCIR": args.cil, "EURO-CORDEX": args.euro}
DATASETS = {k: v for k, v in ALL_DATASETS.items() if args.use_all or FLAG_MAP[k]}


def load_france_geom():
    print("Loading France shapefile...")
    world = gpd.read_file(
        "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_0_countries.zip"
    )
    france_shape = world[world["NAME"] == "France"].clip(box(-10, 40, 12, 53))
    return france_shape.geometry.values[0]


def build_mask(lon2d, lat2d, france_prep):
    return np.array(
        [france_prep.contains(Point(lo, la))
         for lo, la in zip(lon2d.ravel(), lat2d.ravel())]
    ).reshape(lon2d.shape)


def extract_model_name(fname):
    name = fname.replace("tasmax_france_", "").replace(".nc", "")
    for suffix in ["_ssp245_2000_2080", "_rcp45_2006_2080"]:
        name = name.replace(suffix, "")
    return name


def daily_weighted_mean(fpath, mask_np, weights_np):
    """Return (dates, daily_france_tmax_celsius) for June days only, year by year."""
    w_sum = np.float64(weights_np.sum())
    ds = xr.open_dataset(fpath)
    time_vals = ds["time"].values
    if hasattr(time_vals[0], "year"):
        years_all = np.array([t.year for t in time_vals])
        months_all = np.array([t.month for t in time_vals])
    else:
        dt_index = pd.DatetimeIndex(time_vals)
        years_all = dt_index.year.values
        months_all = dt_index.month.values

    all_dates = []
    all_vals = []
    for yr in np.unique(years_all):
        idx = np.where((years_all == yr) & (months_all == 6))[0]
        if len(idx) == 0:
            continue
        chunk = ds["tasmax"].isel(time=idx).values
        masked = np.where(mask_np, chunk, np.nan)
        daily = np.nansum(masked * weights_np, axis=tuple(range(1, masked.ndim))) / w_sum
        daily = daily.astype(np.float64) - 273.15

        t_slice = time_vals[idx]
        if hasattr(t_slice[0], "isoformat"):
            dates = [t.isoformat()[:10] for t in t_slice]
        else:
            dates = [str(t)[:10] for t in pd.DatetimeIndex(t_slice)]
        all_dates.extend(dates)
        all_vals.extend(daily.tolist())
        del chunk, masked, daily
    ds.close()
    gc.collect()
    return all_dates, all_vals


france_geom = load_france_geom()
france_prep = prep(france_geom)

all_series = {}
reg_mask_cache = {}

for ds_name, results_dir in DATASETS.items():
    files = sorted(glob.glob(os.path.join(results_dir, "tasmax_france_*.nc")))
    if not files:
        print(f"No files for {ds_name}, skipping.")
        continue
    print(f"\n{ds_name}: {len(files)} files")

    for fpath in files:
        model = extract_model_name(os.path.basename(fpath))
        col_name = f"{ds_name}__{model}"
        print(f"  {model}...")

        ds = xr.open_dataset(fpath)
        lat = ds.lat.values
        lon = ds.lon.values
        ds.close()

        if lat.ndim == 1:
            cache_key = (ds_name, lat.shape[0], lon.shape[0])
            if cache_key not in reg_mask_cache:
                lon2d, lat2d = np.meshgrid(lon, lat)
                mask_np = build_mask(lon2d, lat2d, france_prep)
                weights_np = (np.cos(np.deg2rad(lat2d)) * mask_np).astype(np.float64)
                reg_mask_cache[cache_key] = (mask_np, weights_np)
                print(f"    mask: {int(mask_np.sum())} of {mask_np.size} cells")
            mask_np, weights_np = reg_mask_cache[cache_key]
        else:
            mask_np = build_mask(lon, lat, france_prep)
            weights_np = (np.cos(np.deg2rad(lat)) * mask_np).astype(np.float64)

        dates, vals = daily_weighted_mean(fpath, mask_np, weights_np)
        all_series[col_name] = pd.Series(vals, index=pd.Index(dates, name="date"))

print("\nBuilding DataFrame...")
df = pd.DataFrame(all_series)
df.index.name = "date"
df = df.sort_index()

outpath = os.path.join(BASE_DIR, "france_daily_tmax.csv")
df.to_csv(outpath, float_format="%.4f")
print(f"Saved {outpath}  ({df.shape[0]} rows x {df.shape[1]} columns)")

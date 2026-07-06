#!/usr/bin/env python3
"""Plot France-averaged TMax time series from the precomputed CSV."""

import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "france_daily_tmax.csv")
ERA5_CSV_PATH = os.path.join(BASE_DIR, "ERA5_france_june_tmax_2000-2026.csv")

ALL_COLORS = {"NEX-GDDP": "tab:blue", "CIL-GDPCIR": "tab:orange", "EURO-CORDEX": "tab:green"}

parser = argparse.ArgumentParser(description="Plot France TMax time series from CSV.")
parser.add_argument("--nex", action="store_true", help="Include NEX-GDDP")
parser.add_argument("--cil", action="store_true", help="Include CIL-GDPCIR")
parser.add_argument("--euro", action="store_true", help="Include EURO-CORDEX")
parser.add_argument("--all", action="store_true", dest="use_all", help="Include all (default)")
parser.add_argument("--mean", action="store_true", help="Add ensemble mean lines (dotted) to plot 2")
parser.add_argument("--members", action="store_true", help="Add individual member lines (light gray) to plot 2")
parser.add_argument("--ERA5", action="store_true", help="Add observed ERA5 annual max line (black)")
args = parser.parse_args()

if not (args.nex or args.cil or args.euro):
    args.use_all = True

FLAG_MAP = {"NEX-GDDP": args.nex, "CIL-GDPCIR": args.cil, "EURO-CORDEX": args.euro}
COLORS = {k: v for k, v in ALL_COLORS.items() if args.use_all or FLAG_MAP[k]}

df = pd.read_csv(CSV_PATH, index_col="date")
df["year"] = df.index.str[:4].astype(int)

era5 = None
if args.ERA5:
    era5 = pd.read_csv(ERA5_CSV_PATH)


def add_era5(ax):
    if era5 is not None:
        ax.plot(era5["year"], era5["tx"], linewidth=2.0, color="black",
                label="ERA5 (observed)")

fig, ax = plt.subplots(figsize=(14, 7))
legend_handles = []

for ds_name, color in COLORS.items():
    cols = [c for c in df.columns if c.startswith(ds_name + "__")]
    if not cols:
        continue
    for col in cols:
        annual = df[col].groupby(df["year"]).max().dropna()
        ax.plot(annual.index, annual.values, linewidth=0.6, alpha=0.5, color=color)
    legend_handles.append(plt.Line2D([0], [0], color=color, linewidth=1.5, label=ds_name))
    if args.mean:
        ensemble_mean = df[cols].groupby(df["year"]).max().mean(axis=1)
        ax.plot(ensemble_mean.index, ensemble_mean.values, linewidth=2, color=color,
                linestyle="dashed", label=f"{ds_name} mean")

if args.mean:
    legend_handles.append(plt.Line2D([0], [0], color="black", linewidth=2, linestyle="dashed",
                                     label="ensemble mean"))

add_era5(ax)
if args.ERA5:
    legend_handles.append(plt.Line2D([0], [0], color="black", linewidth=2.0, label="ERA5 (observed)"))

ax.set_xlabel("Year")
ax.set_ylabel("Annual max of France-mean daily TMax (°C)")
# ax.set_title("France-averaged maximum daily high temperature for June of each year")
ax.legend(handles=legend_handles, fontsize=9, loc="upper left")
ax.grid(True, alpha=0.3)
fig.tight_layout()

outpath = os.path.join(BASE_DIR, "france_tmax_timeseries.png")
fig.savefig(outpath, dpi=600)
print(f"Saved {outpath}")
plt.close()

# --- Plot 2: ensemble max per year (one line per dataset) ---
fig2, ax2 = plt.subplots(figsize=(14, 7))

for ds_name, color in COLORS.items():
    cols = [c for c in df.columns if c.startswith(ds_name + "__")]
    if not cols:
        continue
    if args.members:
        for i, col in enumerate(cols):
            annual = df[col].groupby(df["year"]).max().dropna()
            ax2.plot(annual.index, annual.values, linewidth=0.6, alpha=0.5, color="gray",
                     label="individual members" if i == 0 and ds_name == list(COLORS)[0] else None)
    ensemble_max = df[cols].max(axis=1).groupby(df["year"]).max()
    ax2.plot(ensemble_max.index, ensemble_max.values, linewidth=1.5, color=color, label=f"{ds_name} max")
    if args.mean:
        ensemble_mean = df[cols].groupby(df["year"]).max().mean(axis=1)
        ax2.plot(ensemble_mean.index, ensemble_mean.values, linewidth=2, color=color,
                 linestyle="dashed", label=f"{ds_name} mean")

add_era5(ax2)

ax2.set_xlabel("Year")
ax2.set_ylabel("Annual max of France-mean daily TMax (°C)")
ax2.set_title("France-averaged daily maximum temperature — ensemble max")
ax2.legend(fontsize=9, loc="upper left")
ax2.grid(True, alpha=0.3)
fig2.tight_layout()

outpath2 = os.path.join(BASE_DIR, "france_tmax_ensemble_max.png")
#fig2.savefig(outpath2, dpi=200)
#print(f"Saved {outpath2}")
plt.close()

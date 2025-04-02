import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from sklearn.cluster import DBSCAN
import contextily as ctx
import os

# Load data
df = pd.read_csv("quake_exports/csv/combined/earthquakes_combined.csv")
df["time_utc"] = pd.to_datetime(df["time_utc"])
df["year"] = df["time_utc"].dt.year

# Prepare output directory
os.makedirs("cluster_maps", exist_ok=True)

# Load fault lines
faults = gpd.read_file("fault_lines.json").to_crs(epsg=3857)

# Function to plot clusters on basemap
def plot_clusters(gdf, title, filename):
    fig, ax = plt.subplots(figsize=(10, 10))
    
    # Clustered points
    clustered = gdf[gdf["cluster"] != -1]
    clustered.plot(ax=ax, column="cluster", cmap="tab20", markersize=20, alpha=0.7, legend=True)
    
    # Noise points
    gdf[gdf["cluster"] == -1].plot(ax=ax, color="lightgrey", markersize=5, label="Noise", alpha=0.4)
    
    # Fault lines
    faults.plot(ax=ax, color="purple", linewidth=1, label="Fault Lines")
    
    # Map
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom=6)
    ax.set_title(title, fontsize=15)
    ax.set_axis_off()
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"cluster_maps/{filename}", dpi=300)
    plt.close()

# --- Global clustering with tuned parameters ---
coords_all = df[["latitude", "longitude"]]
db_all = DBSCAN(eps=0.3, min_samples=10).fit(coords_all)
df["cluster"] = db_all.labels_

# GeoDataFrame
gdf_all = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326").to_crs(epsg=3857)

# Plot full dataset
plot_clusters(
    gdf_all,
    title="Earthquake Clusters in Myanmar (DBSCAN, eps=0.3, min_samples=10)",
    filename="clusters_all.png"
)

# --- Temporal Clustering by Decade ---
for start_year in range(1950, 2030, 10):
    end_year = start_year + 10
    df_slice = df[(df["year"] >= start_year) & (df["year"] < end_year)].copy()

    if len(df_slice) < 50:
        print(f"Skipping {start_year}s: not enough data.")
        continue

    coords = df_slice[["latitude", "longitude"]]
    db = DBSCAN(eps=0.3, min_samples=8).fit(coords)
    df_slice["cluster"] = db.labels_

    gdf = gpd.GeoDataFrame(df_slice, geometry=gpd.points_from_xy(df_slice.longitude, df_slice.latitude), crs="EPSG:4326").to_crs(epsg=3857)

    plot_clusters(
        gdf,
        title=f"Earthquake Clusters in Myanmar ({start_year}s)",
        filename=f"clusters_{start_year}s.png"
    )

print("✅ All clustering visualizations completed. Check the 'cluster_maps/' folder.")

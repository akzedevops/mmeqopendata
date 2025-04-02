import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import DBSCAN
import geopandas as gpd
import contextily as ctx

# Load earthquake data
df = pd.read_csv("quake_exports/csv/combined/earthquakes_combined.csv")
df["time_utc"] = pd.to_datetime(df["time_utc"])

# 1. Magnitude Distribution Histogram and KDE
plt.figure(figsize=(10, 6))
sns.histplot(df["mag"], bins=30, kde=True, color='skyblue')
plt.title('Magnitude Distribution of Earthquakes (Myanmar)')
plt.xlabel('Magnitude')
plt.ylabel('Frequency')
plt.grid(True)
plt.tight_layout()
plt.savefig("magnitude_distribution.png")
plt.close()

# 2. Earthquake Frequency Trends
df_monthly = df.set_index("time_utc").resample("ME")["mag"].count()

plt.figure(figsize=(12, 6))
df_monthly.plot(color='teal')
plt.title('Monthly Frequency of Earthquakes')
plt.xlabel('Year')
plt.ylabel('Number of Earthquakes')
plt.grid(True)
plt.tight_layout()
plt.savefig("monthly_frequency_trends.png")
plt.close()

# 3. Magnitude vs. Depth Scatterplot
plt.figure(figsize=(10, 6))
sns.scatterplot(x='depth', y='mag', data=df, alpha=0.6)
plt.title('Magnitude vs. Depth of Earthquakes')
plt.xlabel('Depth (km)')
plt.ylabel('Magnitude')
plt.grid(True)
plt.tight_layout()
plt.savefig("mag_vs_depth_relationship.png")
plt.close()

# 4. Spatial Clustering (DBSCAN) over Map
coords = df[['latitude', 'longitude']]

# Run DBSCAN clustering
db = DBSCAN(eps=0.3, min_samples=10).fit(coords)
df['cluster'] = db.labels_

# Create GeoDataFrame for mapping
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")

# Convert to Web Mercator for contextily
gdf = gdf.to_crs(epsg=3857)

fig, ax = plt.subplots(figsize=(12, 12))

# Plot clusters (exclude noise points)
gdf[gdf['cluster'] != -1].plot(
    ax=ax,
    column='cluster',
    cmap='tab20',
    markersize=20,
    alpha=0.8,
    legend=True,
    legend_kwds={'label': "Clusters"}
)

# Plot noise points (unclustered)
gdf[gdf['cluster'] == -1].plot(
    ax=ax,
    color='grey',
    markersize=5,
    alpha=0.4,
    label='Noise'
)

# Add basemap (background tiles)
ctx.add_basemap(
    ax, 
    source=ctx.providers.OpenStreetMap.Mapnik, 
    zoom=7
)

# Adjust map extent and title
ax.set_axis_off()
ax.set_title('Earthquake Clusters in Myanmar (DBSCAN)', fontsize=16)
plt.legend()
plt.tight_layout()
plt.savefig("earthquake_clusters.png", dpi=300, bbox_inches='tight')
plt.close()

# Print clustering summary
num_clusters = len(set(db.labels_)) - (1 if -1 in db.labels_ else 0)
num_noise = list(db.labels_).count(-1)

print("✅ Advanced Analysis Completed")
print(f"📊 Total Clusters found: {num_clusters}")
print(f"❗ Noise points (unclustered): {num_noise}")
print("Files generated:")
print(" - magnitude_distribution.png")
print(" - monthly_frequency_trends.png")
print(" - mag_vs_depth_relationship.png")
print(" - earthquake_clusters.png")

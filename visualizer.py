import pandas as pd
import folium
from folium.plugins import HeatMap
from datetime import datetime

# Load earthquake data
df = pd.read_csv("quake_exports/csv/combined/earthquakes_combined.csv")
df["time_utc"] = pd.to_datetime(df["time_utc"])

# Dynamic date-range info
start_date = df["time_utc"].min().strftime("%B %Y")
end_date = df["time_utc"].max().strftime("%B %Y")

# Create a base map (reliable tiles)
quake_map = folium.Map(location=[21.0, 96.0], zoom_start=6, tiles='OpenStreetMap')

# Earthquake markers
marker_layer = folium.FeatureGroup(name="Earthquake Markers")
for _, row in df.iterrows():
    color = "green" if row["mag"] < 3 else "orange" if row["mag"] < 5 else "red"
    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=4,
        color=color,
        fill=True,
        fill_opacity=0.7,
        popup=f"<b>Magnitude:</b> {row['mag']}<br><b>Depth:</b> {row['depth']} km<br><b>Time (MMT):</b> {row['time_mmt']}"
    ).add_to(marker_layer)
marker_layer.add_to(quake_map)

# Fault lines layer
fault_lines_layer = folium.FeatureGroup(name="Fault Lines")
folium.GeoJson(
    "fault_lines.json",
    style_function=lambda x: {"color": "purple", "weight": 2, "opacity": 0.7}
).add_to(fault_lines_layer)
fault_lines_layer.add_to(quake_map)

# Heatmap layer
heatmap_layer = folium.FeatureGroup(name="Earthquake Heatmap")
heat_data = [[row['latitude'], row['longitude']] for _, row in df.iterrows()]
HeatMap(heat_data, radius=10, blur=15, max_zoom=10).add_to(heatmap_layer)
heatmap_layer.add_to(quake_map)

# Layer controls
folium.LayerControl().add_to(quake_map)

# Custom legend with clear marker colors and explanations
legend_html = f"""
<div style="position: fixed; bottom: 50px; left: 50px; width: 210px; z-index:9999; 
            font-size:14px; background-color: white; opacity: 0.9; padding: 10px; border-radius: 5px;">
    <b>Earthquake Magnitude Legend</b><br>
    <i style="background: green; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></i> Magnitude &lt; 3<br>
    <i style="background: orange; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></i> 3 ≤ Magnitude &lt; 5<br>
    <i style="background: red; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></i> Magnitude ≥ 5<br>
    <i style="background: purple; width: 15px; height: 3px; display: inline-block;"></i> Fault Lines<br><br>
    <b>Data Range:</b><br>{start_date} – {end_date}
</div>
"""
quake_map.get_root().html.add_child(folium.Element(legend_html))

# Dynamic title at the top center
title_html = f'''
<h3 align="center" style="font-size:20px">
<b>Myanmar Earthquake Visualization<br>({start_date} – {end_date})</b></h3>
'''
quake_map.get_root().html.add_child(folium.Element(title_html))

# Save map
quake_map.save("enhanced_earthquake_map.html")
print("✅ Enhanced earthquake map saved: enhanced_earthquake_map.html")

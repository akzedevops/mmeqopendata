import os
import json
import math
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable

plt.rcParams.update({
    "font.family": "serif",
    "font.size": 11,
    "axes.labelsize": 12,
    "axes.titlesize": 13,
    "legend.fontsize": 9,
    "figure.dpi": 300,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.1,
})

OUT = "paper/figures"
os.makedirs(OUT, exist_ok=True)

df = pd.read_csv("quake_exports/csv/combined/earthquakes_combined.csv", on_bad_lines="skip")
df["time_utc"] = pd.to_datetime(df["time_utc"], errors="coerce")
df = df.dropna(subset=["time_utc", "mag", "latitude", "longitude", "depth"])

risk = pd.read_csv("report/dam_risk_scores.csv")

with open("myanmar_dams.geojson") as f:
    dams_geo = json.load(f)
dams_df = pd.DataFrame([
    {"name": feat["properties"]["name"], "lat": feat["geometry"]["coordinates"][1],
     "lon": feat["geometry"]["coordinates"][0],
     "status": feat["properties"]["status"], "function": feat["properties"]["function"],
     "capacity_mw": feat["properties"].get("capacity_mw", "")}
    for feat in dams_geo["features"]
])

faults_clipped = None
if os.path.exists("fault_lines.json"):
    import geopandas as gpd
    faults = gpd.read_file("fault_lines.json")
    faults_clipped = faults.cx[92:102, 9:29]

# ========== Figure 1: Study area map ==========
fig, ax = plt.subplots(figsize=(10, 12))

ax.scatter(df["longitude"], df["latitude"], s=df["mag"].clip(1)**1.8 * 2,
           c=df["depth"], cmap="RdYlBu_r", alpha=0.4, edgecolors="none",
           norm=Normalize(vmin=0, vmax=150), zorder=2)

dam_colors = {"Complete": "#2196F3", "Proposed": "#90CAF9", "Under Construction": "#FF9800",
              "Planned": "#90CAF9", "Identified Sites": "#90CAF9"}
for _, d in dams_df.iterrows():
    c = dam_colors.get(d["status"], "#888")
    ax.scatter(d["lon"], d["lat"], s=20, c=c, marker="^", edgecolors="k",
               linewidths=0.3, zorder=3)

m77 = df[df["mag"] >= 7.5]
for _, eq in m77.iterrows():
    ax.plot(eq["longitude"], eq["latitude"], "*", color="red", markersize=18,
            markeredgecolor="k", markeredgewidth=0.8, zorder=5)
    ax.annotate(f"M{eq['mag']:.1f}\n{eq['time_utc'].strftime('%Y')}",
                (eq["longitude"], eq["latitude"]),
                textcoords="offset points", xytext=(8, 5), fontsize=8, fontweight="bold")

if faults_clipped is not None:
    faults_clipped.plot(ax=ax, color="purple", linewidth=1.5, alpha=0.6, zorder=1)

ax.set_xlim(92, 102)
ax.set_ylim(9, 29)
ax.set_xlabel("Longitude (\u00b0E)")
ax.set_ylabel("Latitude (\u00b0N)")
ax.set_title("Myanmar Seismicity and Dam Infrastructure")

handles = [
    Line2D([0], [0], marker="o", color="w", markerfacecolor="#d62728", markersize=8, label="Shallow (<30 km)"),
    Line2D([0], [0], marker="o", color="w", markerfacecolor="#ff7f0e", markersize=8, label="Intermediate (30\u201370 km)"),
    Line2D([0], [0], marker="o", color="w", markerfacecolor="#1f77b4", markersize=8, label="Deep (>70 km)"),
    Line2D([0], [0], marker="^", color="w", markerfacecolor="#2196F3", markersize=8, markeredgecolor="k", label="Dam (Complete)"),
    Line2D([0], [0], marker="^", color="w", markerfacecolor="#90CAF9", markersize=8, markeredgecolor="k", label="Dam (Proposed/Planned)"),
    Line2D([0], [0], marker="*", color="w", markerfacecolor="red", markersize=14, markeredgecolor="k", label="M \u2265 7.5"),
    Line2D([0], [0], color="purple", linewidth=2, label="Fault Lines"),
]
ax.legend(handles=handles, loc="lower left", framealpha=0.95, edgecolor="k")
ax.set_aspect(1.0)
fig.savefig(f"{OUT}/fig1_study_area.pdf")
fig.savefig(f"{OUT}/fig1_study_area.png")
plt.close()
print("Figure 1: Study area map")

# ========== Figure 2: Frequency-Magnitude Distribution (CORRECTED) ==========
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

mags = df["mag"].values
mag_bins = np.arange(max(0, mags.min()), mags.max() + 0.1, 0.1)
counts, edges = np.histogram(mags, bins=mag_bins)
centers = (edges[:-1] + edges[1:]) / 2
cumulative_counts = np.cumsum(counts[::-1])[::-1]

# Panel (a): Full FMD with corrected b-value at Mc=4.0
ax1.bar(centers, counts, width=0.1, color="steelblue", edgecolor="none", alpha=0.7, label="Non-cumulative")
ax1_2 = ax1.twinx()
mask_cum = cumulative_counts > 0
ax1_2.plot(centers[mask_cum], cumulative_counts[mask_cum], "k.", markersize=2, alpha=0.5)

mc_corrected = 4.0
mask = (centers >= mc_corrected) & (counts > 0)
if mask.sum() > 2:
    log_N_cum = np.log10(np.maximum(cumulative_counts[mask], 1))
    coeffs = np.polyfit(centers[mask], log_N_cum, 1)
    b_val = -coeffs[0]
    a_val = coeffs[1]
    x_fit = np.linspace(mc_corrected, mags.max(), 100)
    y_fit = 10 ** (a_val - b_val * x_fit)
    ax1_2.plot(x_fit, y_fit, "r-", linewidth=2,
               label=f"GR fit (b={b_val:.2f})")
    ax1_2.set_yscale("log")
    ax1_2.set_ylabel("Cumulative frequency (N)", color="red")

ax1.axvline(mc_corrected, color="gray", linestyle="--", linewidth=1.5, label=f"$M_c$ = {mc_corrected}")
ax1.set_xlabel("Magnitude")
ax1.set_ylabel("Non-cumulative count")
ax1.set_title("(a) Frequency-Magnitude Distribution")
ax1.set_yscale("log")
ax1.set_xlim(0, 8)
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax1_2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=8)
ax1_2.set_ylim(bottom=0.5)

# Panel (b): b-value stability analysis
mc_tests = np.arange(2.0, 6.1, 0.5)
b_values = []
mc_labels = []
for mc_t in mc_tests:
    m_above = mags[mags >= mc_t]
    if len(m_above) < 20:
        continue
    mean_m = m_above.mean()
    b = np.log10(np.e) / (mean_m - (mc_t - 0.05))
    b_values.append(b)
    mc_labels.append(mc_t)

ax2.plot(mc_labels, b_values, "o-", color="steelblue", linewidth=2, markersize=8)
ax2.axhline(1.0, color="red", linestyle="--", alpha=0.5, label="b = 1.0 (typical)")
ax2.axvline(mc_corrected, color="gray", linestyle="--", alpha=0.7, label=f"$M_c$ = {mc_corrected}")
ax2.set_xlabel("Minimum magnitude ($M_c$)")
ax2.set_ylabel("b-value")
ax2.set_title("(b) b-value Stability Analysis")
ax2.legend()
ax2.set_xlim(1.5, 6.5)
ax2.set_ylim(0, 1.5)
ax2.grid(True, alpha=0.3)

fig.savefig(f"{OUT}/fig2_fmd.pdf")
fig.savefig(f"{OUT}/fig2_fmd.png")
plt.close()
print("Figure 2: FMD (corrected)")

# ========== Figure 3: Depth distribution ==========
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

ax1.hist(df["depth"], bins=50, color="steelblue", edgecolor="white", alpha=0.8)
ax1.set_xlabel("Depth (km)")
ax1.set_ylabel("Number of events")
ax1.set_title("Depth Distribution")
ax1.axvline(30, color="red", linestyle="--", alpha=0.5, label="30 km")
ax1.axvline(70, color="orange", linestyle="--", alpha=0.5, label="70 km")
ax1.legend()

ax2.scatter(df["depth"], df["mag"], s=3, c="steelblue", alpha=0.3, edgecolors="none")
ax2.set_xlabel("Depth (km)")
ax2.set_ylabel("Magnitude")
ax2.set_title("Magnitude vs Depth")
ax2.axhline(5.0, color="red", linestyle="--", alpha=0.5, label="M5.0")
ax2.axvline(30, color="gray", linestyle="--", alpha=0.3)
ax2.legend()

fig.savefig(f"{OUT}/fig3_depth.pdf")
fig.savefig(f"{OUT}/fig3_depth.png")
plt.close()
print("Figure 3: Depth distribution")

# ========== Figure 4: Dam risk map ==========
fig, ax = plt.subplots(figsize=(10, 12))

grade_colors = {"Critical": "#d62728", "High": "#ff7f0e", "Moderate": "#ffdd57", "Low": "#2ca02c"}
for grade, color in grade_colors.items():
    sub = risk[risk["risk_grade"] == grade]
    if len(sub) == 0:
        continue
    ax.scatter(sub["longitude"], sub["latitude"], s=40, c=color, marker="^",
               edgecolors="k", linewidths=0.3, zorder=3, label=f"{grade} ({len(sub)})")

ax.scatter(df["longitude"], df["latitude"], s=df["mag"].clip(1)**1.5, c="lightgray",
           alpha=0.2, edgecolors="none", zorder=1)

if faults_clipped is not None:
    faults_clipped.plot(ax=ax, color="purple", linewidth=1.5, alpha=0.5, zorder=2)

ax.set_xlim(92, 102)
ax.set_ylim(9, 29)
ax.set_xlabel("Longitude (\u00b0E)")
ax.set_ylabel("Latitude (\u00b0N)")
ax.set_title("Dam Seismic Risk Assessment \u2014 Myanmar")
ax.legend(loc="lower left", framealpha=0.95, title="Risk Grade", edgecolor="k")
ax.set_aspect(1.0)
fig.savefig(f"{OUT}/fig4_dam_risk_map.pdf")
fig.savefig(f"{OUT}/fig4_dam_risk_map.png")
plt.close()
print("Figure 4: Dam risk map")

# ========== Figure 5: PGA attenuation curves (ASK08) ==========
fig, ax = plt.subplots(figsize=(8, 6))

def pga_ask08(mag, rjb_km):
    rjb = max(rjb_km, 0.0)
    c1_val = 6.2
    c4 = 5.6
    a1 = -0.526
    a2 = -1.60
    a3 = 0.143
    a4 = 0.0
    a10 = -0.135
    a13 = -0.015
    h = c4
    r = math.sqrt(rjb**2 + h**2)
    if mag <= c1_val:
        f_mag = a1 + a4 * (mag - c1_val) + a13 * (8.5 - mag)**2
    else:
        f_mag = a1 + a10 * (mag - c1_val) + a13 * (8.5 - mag)**2
    f_dis = (a2 + a3 * mag) * math.log(r)
    return max(math.exp(f_mag + f_dis), 0.0001)

dists = np.logspace(0, 2.8, 200)
for mag, ls in [(7.7, "-"), (7.0, "--"), (6.5, "-."), (6.0, ":")]:
    pgas = [pga_ask08(mag, d) for d in dists]
    ax.loglog(dists, pgas, ls, linewidth=2, label=f"M{mag:.1f}")

# Plot dam PGA vs distance to fault
pga_dams = risk["pga_g"].values
dist_dams = risk["dist_to_fault_km"].values
mask_valid = ~np.isnan(dist_dams)
ax.scatter(dist_dams[mask_valid], pga_dams[mask_valid], s=15, c="gray", alpha=0.5,
           edgecolors="none", label=f"Dams (N={mask_valid.sum()})")

# Add recorded values from 2025 event
ax.scatter([5], [1.07], s=80, c="red", marker="D", edgecolors="k", zorder=5, label="Recorded 1.07g (GFZ)")
ax.scatter([5], [0.62], s=80, c="orange", marker="D", edgecolors="k", zorder=5, label="Recorded 0.62g (USGS)")

ax.set_xlabel("Joyner-Boore Distance, $R_{jb}$ (km)")
ax.set_ylabel("Peak Ground Acceleration (g)")
ax.set_title("Ground Motion Attenuation (Abrahamson & Silva, 2008)")
ax.legend(loc="upper right", fontsize=8)
ax.set_xlim(1, 600)
ax.set_ylim(1e-4, 2)
ax.grid(True, which="both", alpha=0.3)
fig.savefig(f"{OUT}/fig5_pga_attenuation.pdf")
fig.savefig(f"{OUT}/fig5_pga_attenuation.png")
plt.close()
print("Figure 5: PGA attenuation (ASK08)")

# ========== Figure 6: Temporal evolution ==========
fig, ax = plt.subplots(figsize=(12, 5))

df_yr = df.set_index("time_utc").resample("YE").agg(
    count=("mag", "size"),
    max_mag=("mag", "max"),
    mean_mag=("mag", "mean"),
)
ax.bar(df_yr.index.year, df_yr["count"], color="steelblue", alpha=0.7, label="Annual count")

ax2 = ax.twinx()
ax2.plot(df_yr.index.year, df_yr["max_mag"], "r-o", markersize=3, linewidth=1, label="Max magnitude")
ax2.set_ylabel("Maximum Magnitude", color="red")
ax2.set_ylim(0, 9)
ax2.tick_params(axis="y", labelcolor="red")

ax.set_xlabel("Year")
ax.set_ylabel("Number of Events")
ax.set_title("Temporal Evolution of Myanmar Seismicity (1970\u20132026)")
lines1, labels1 = ax.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
fig.savefig(f"{OUT}/fig6_temporal.pdf")
fig.savefig(f"{OUT}/fig6_temporal.png")
plt.close()
print("Figure 6: Temporal evolution")

# ========== Figure 7: Risk score distribution + sensitivity ==========
fig, axes = plt.subplots(1, 3, figsize=(16, 5))

axes[0].hist(risk["composite_risk"], bins=20, color="steelblue", edgecolor="white")
axes[0].set_xlabel("Composite Risk Score")
axes[0].set_ylabel("Number of Dams")
axes[0].set_title("(a) Risk Score Distribution")

grade_counts = risk["risk_grade"].value_counts()
colors = [grade_colors.get(g, "#888") for g in grade_counts.index]
axes[1].bar(grade_counts.index, grade_counts.values, color=colors, edgecolor="k")
axes[1].set_ylabel("Number of Dams")
axes[1].set_title("(b) Dams by Risk Grade")
for i, (g, v) in enumerate(zip(grade_counts.index, grade_counts.values)):
    axes[1].text(i, v + 3, str(v), ha="center", fontweight="bold")

# Sensitivity analysis boxplot
sens = pd.read_csv("report/sensitivity_analysis.csv")
grade_data = [sens["critical"], sens["high"], sens["moderate"], sens["low"]]
bp = axes[2].boxplot(grade_data, labels=["Critical", "High", "Moderate", "Low"],
                      patch_artist=True)
for patch, color in zip(bp["boxes"], ["#d62728", "#ff7f0e", "#ffdd57", "#2ca02c"]):
    patch.set_facecolor(color)
    patch.set_alpha(0.6)
axes[2].set_ylabel("Number of Dams")
axes[2].set_title("(c) Sensitivity Analysis\n(100 Monte Carlo weight samples)")

fig.savefig(f"{OUT}/fig7_risk_distribution.pdf")
fig.savefig(f"{OUT}/fig7_risk_distribution.png")
plt.close()
print("Figure 7: Risk distribution + sensitivity")

# ========== Figure 8: Dam status and function ==========
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

status_counts = dams_df["status"].value_counts()
ax1.barh(status_counts.index, status_counts.values, color=["#2196F3","#90CAF9","#FF9800","#B0BEC5","#B0BEC5","#EF5350","#B0BEC5"])
ax1.set_xlabel("Number of Dams")
ax1.set_title("Dam Status")
for i, v in enumerate(status_counts.values):
    ax1.text(v + 1, i, str(v), va="center")

func_counts = dams_df["function"].value_counts()
ax2.barh(func_counts.index, func_counts.values, color=["#4CAF50","#2196F3","#FF9800","#9C27B0","#B0BEC5"])
ax2.set_xlabel("Number of Dams")
ax2.set_title("Dam Function")
for i, v in enumerate(func_counts.values):
    ax2.text(v + 1, i, str(v), va="center")

fig.savefig(f"{OUT}/fig8_dam_types.pdf")
fig.savefig(f"{OUT}/fig8_dam_types.png")
plt.close()
print("Figure 8: Dam types")

# ========== Figure 9: Vs30 map ==========
fig, ax = plt.subplots(figsize=(10, 12))

vs30_data = pd.read_csv("report/dam_vs30.csv")
vs30_colors_map = {290: "#d62728", 360: "#ff7f0e", 460: "#ffdd57", 600: "#98df8a",
                   760: "#2ca02c", 900: "#1f77b4", 1100: "#08306b"}
vs30_labels = {290: "NEHRP E (290)", 360: "NEHRP D (360)", 460: "NEHRP D (460)",
               600: "NEHRP C (600)", 760: "NEHRP C (760)", 900: "NEHRP B (900)", 1100: "NEHRP B (1100)"}

for vs30_val in sorted(vs30_data["vs30"].unique()):
    sub = vs30_data[vs30_data["vs30"] == vs30_val]
    color = vs30_colors_map.get(vs30_val, "#888")
    label = vs30_labels.get(vs30_val, f"Vs30={vs30_val}")
    ax.scatter(sub["lon"], sub["lat"], s=30, c=color, marker="^",
               edgecolors="k", linewidths=0.3, zorder=3, label=f"{label} ({len(sub)})")

if faults_clipped is not None:
    faults_clipped.plot(ax=ax, color="purple", linewidth=1.5, alpha=0.5, zorder=2)

ax.set_xlim(92, 102)
ax.set_ylim(9, 29)
ax.set_xlabel("Longitude (\u00b0E)")
ax.set_ylabel("Latitude (\u00b0N)")
ax.set_title("Site-Specific Vs30 at Dam Sites (Wald & Allen, 2007)")
ax.legend(loc="lower left", framealpha=0.95, title="Vs30 (m/s)", edgecolor="k", fontsize=8)
ax.set_aspect(1.0)
fig.savefig(f"{OUT}/fig9_vs30_map.pdf")
fig.savefig(f"{OUT}/fig9_vs30_map.png")
plt.close()
print("Figure 9: Vs30 map")

# ========== Figure 10: Hazard curves for representative dams ==========
from src.mmeq.analysis.dam_risk import compute_hazard_curve
from src.mmeq.analysis.seismology import b_value as calc_bval

b_val_fig, a_val_fig, mc_fig = calc_bval(df["mag"], min_mag=4.0)

fig, ax = plt.subplots(figsize=(9, 7))

representative_dams = [
    ("Kun Chaung", 1.0, 1100),
    ("Taungnyo", 1.3, 900),
    ("Dam near 21.77N 95.89E", 2.9, 760),
    ("Ta Nai Hka", 4.3, 760),
    ("Meikhtila", 8.8, 760),
    ("Dam near 20.85N 96.03E", 8.9, 900),
]

line_styles = ["-", "--", "-.", ":", "-", "--"]
colors_hc = ["#d62728", "#ff7f0e", "#2ca02c", "#1f77b4", "#9467bd", "#8c564b"]

for i, (name, rjb, vs30) in enumerate(representative_dams):
    hc = compute_hazard_curve(
        rjb_km=rjb, vs30=vs30, b_val=b_val_fig, a_val=a_val_fig, mc=mc_fig,
        pga_levels=np.logspace(-2.5, 0.3, 80),
    )
    valid = hc[hc["return_period_yr"] < 1e5]
    if len(valid) > 2:
        ax.plot(valid["pga_g"], valid["return_period_yr"], line_styles[i % len(line_styles)],
                linewidth=2, color=colors_hc[i], label=f"{name} (Rjb={rjb:.0f}km, Vs30={vs30})")

ax.axhline(475, color="gray", linestyle="--", alpha=0.5, label="475 yr (10% in 50 yr)")
ax.axhline(2475, color="gray", linestyle=":", alpha=0.5, label="2475 yr (2% in 50 yr)")
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("Peak Ground Acceleration (g)")
ax.set_ylabel("Return Period (years)")
ax.set_title("Seismic Hazard Curves at Representative Dam Sites")
ax.legend(loc="lower right", fontsize=8)
ax.set_xlim(0.01, 3)
ax.set_ylim(1, 1e4)
ax.grid(True, which="both", alpha=0.3)
fig.savefig(f"{OUT}/fig10_hazard_curves.pdf")
fig.savefig(f"{OUT}/fig10_hazard_curves.png")
plt.close()
print("Figure 10: Hazard curves")

print("\nAll figures saved to", OUT)

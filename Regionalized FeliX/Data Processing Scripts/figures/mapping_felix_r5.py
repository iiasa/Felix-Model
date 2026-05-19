import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# -----------------------------
# Your data
# -----------------------------
path_data = "C:/Users/yequanliang/OneDrive - IIASA/research_project/(0 Processing) dietary felix-r5/"
path_data_output = path_data + "presentations/"
df = pd.read_csv(path_data + "test_countries.csv")
df = df.dropna()


# -----------------------------
# Load Natural Earth directly
# -----------------------------
url = (
    "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
)
world = gpd.read_file(url)

# Use correct column name
world = world.rename(columns={"NAME": "country"})

# -----------------------------
# Fix name mismatches
# -----------------------------
# name_map = {
#     "Venezuela": "Venezuela (Bolivarian Republic of)",
#     "United States of America": "United States",
# }
# world["country"] = world["country"].replace(name_map)

# -----------------------------
# Merge
# -----------------------------
merged = world.merge(df, on="country", how="left")


# -----------------------------
# Plot
# -----------------------------


region_colors = {
    "Africa": "#1f77b4",
    "AsiaPacific": "#ff7f0e",
    "EastEu": "#2ca02c",
    "LAC": "#d62728",
    "WestEu_Dev": "#9467bd",
}
merged["color"] = merged["felix_region_abb"].map(region_colors)

fig, ax = plt.subplots(figsize=(16, 8))

merged.plot(
    column="felix_region_abb",
    categorical=True,
    legend=True,
    edgecolor="black",
    linewidth=0.3,
    # color=merged["color"],
    missing_kwds={"color": "lightgrey", "label": "Not classified"},
    ax=ax,
    legend_kwds={
        "loc": "center left",
        "facecolor": "none",
        "edgecolor": "none",
        "fontsize": 20,
    },
)

# ax.set_title("Regional Classification Map")
ax.axis("off")

# plt.show()

plt.tight_layout()
plt.savefig(
    path_data_output + "mapping_felix_r5.svg",
    dpi=1200,
    # transparent=True,
    bbox_inches="tight",
    pad_inches=0.01,
    format="svg",
)

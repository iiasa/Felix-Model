import pandas as pd
import matplotlib.pyplot as plt

# Load your data (adjust filename if needed)
path_data = "C:/Users/yequanliang/OneDrive - IIASA/research_iiasa/choices/2025.11 diet_scenario_felix_r5/felix_r5/sensitivity_analysis_20260506"
df = pd.read_excel(path_data + "/ALL.xlsx")

# Rename columns for clarity (optional but recommended)
df.columns = ["scenario", "var0", "var1", "var2", "var3"]

# Unique scenarios and colors
scenarios = df["scenario"].unique()
colors = ["red", "blue", "green", "orange"]

# Variables list
vars_ = ["var0", "var1", "var2", "var3"]

# Create 4x4 subplot grid
fig, axes = plt.subplots(4, 4, figsize=(12, 12))

for i, var_y in enumerate(vars_):
    for j, var_x in enumerate(vars_):
        ax = axes[i, j]

        # Skip diagonal plots
        if i == j:
            ax.axis("off")
            continue

        # Scatter for each scenario
        for scen, color in zip(scenarios, colors):
            subset = df[df["scenario"] == scen]
            ax.scatter(
                subset[var_x],
                subset[var_y],
                label=scen if (i == 0 and j == 1) else "",
                alpha=0.6,
                s=10,
                color=color,
            )

        # Labels only on outer plots (cleaner look)
        if i == 3:
            ax.set_xlabel(var_x)
        if j == 0:
            ax.set_ylabel(var_y)

# Add legend only once
handles, labels = axes[0, 1].get_legend_handles_labels()
fig.legend(handles, labels, loc="upper right")

plt.tight_layout()
plt.show()

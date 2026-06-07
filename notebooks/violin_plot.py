import matplotlib.pyplot as plt
import numpy as np

plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['text.color'] = 'white'
plt.rcParams['axes.facecolor'] = '#1A1A1A'
plt.rcParams['figure.facecolor'] = '#1A1A1A'
plt.rcParams['axes.edgecolor'] = '#1A1A1A'
plt.rcParams['xtick.color'] = 'white'
plt.rcParams['ytick.color'] = 'white'
plt.rcParams['grid.color'] = '#5A5A5A'

# Load data from the fact_orders table
df = spark.table("workspace.dbt_dev_gold.fact_orders")

# Get category and final_price columns as pandas DataFrame
data = df.select("category", "final_price").toPandas()

# Convert Decimal to float and drop nulls
data['final_price'] = data['final_price'].apply(lambda x: float(x) if x is not None else np.nan)
data = data.dropna()

# Prepare data for violin plot - group by category
categories = sorted(data['category'].unique())
data_by_category = [data[data['category'] == cat]['final_price'].astype(float).values for cat in categories]

# Create the violin plot with increased dpi
fig, ax = plt.subplots(figsize=(14, 4), dpi=150)
parts = ax.violinplot(data_by_category, positions=range(len(categories)), 
                       showmeans=False, showmedians=True, widths=0.7)

# Customize colors
for pc in parts['bodies']:
    pc.set_facecolor('#9B59B6')
    pc.set_alpha(1.0)

# Style the lines with reduced thickness
parts['cmedians'].set_color('white')
parts['cmedians'].set_linewidth(1.2)
parts['cbars'].set_color('white')
parts['cbars'].set_linewidth(1.2)
parts['cmins'].set_color('white')
parts['cmins'].set_linewidth(1.2)
parts['cmaxes'].set_color('white')
parts['cmaxes'].set_linewidth(1.2)

# Set labels
ax.tick_params(axis='both', length=0)
ax.set_xticks(range(len(categories)))
ax.set_xticklabels([cat.capitalize() for cat in categories], rotation=0, ha='center', fontsize=9)
ax.set_xlabel('')
ax.set_ylabel('')
ax.set_title('Order Value Distribution | Category', fontsize=15, color='white', loc='left')

# Format y-axis as currency and set font properties
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
for label in ax.get_yticklabels():
    label.set_fontsize(9)
    #label.set_fontweight('bold')

# Add grid for readability with increased visibility
ax.grid(True, alpha=0.25, axis='y', linestyle='--', linewidth=1.2)

plt.tight_layout(pad=2.0)
plt.show()

# Print summary statistics
print("\nSummary Statistics by Category:")
print("=" * 70)
for cat in categories:
    cat_data = data[data['category'] == cat]['final_price']
    print(f"{cat.capitalize():15} - Mean: ${cat_data.mean():>10,.2f}  Median: ${cat_data.median():>10,.2f}  Std: ${cat_data.std():>10,.2f}")
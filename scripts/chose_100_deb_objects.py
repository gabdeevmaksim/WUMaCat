import pandas as pd
from astropy.table import Table

# Read the ecsv file
# Read the ecsv file
df = Table.read('./data/debcat_cross_with_gaia.ecsv', format='ascii.ecsv').to_pandas()

# Sort by Vmag in descending order and by Pday in ascending order
df_sorted = df.sort_values(by=['Vmag', 'Pday'], ascending=[False, True])

# Select the required columns
df_selected = df_sorted[['name', 'SOURCE_ID', 'ra', 'dec']]

# Write to a new file
df_selected.to_csv('./data/deb_sorted_for_TESS.csv', index=False)
import pandas as pd
from astropy.time import Time
import os
from astropy.table import Table

input_dir = '/Users/wera/Max_astro/Slovakia/tess_curves/deb_tess_curves'
output_dir = '/Users/wera/Max_astro/Slovakia/tess_curves/output_files'

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for filename in os.listdir(input_dir):
    if filename.endswith('.csv'):
        input_file = os.path.join(input_dir, filename)
        output_file = os.path.join(output_dir, filename)
        
        df = pd.read_csv(input_file)
        time_astropy = Time(df["time"].astype(str).tolist(), format='iso', scale='tdb')
        df["jd"] = time_astropy.jd
        
        df = df[['jd', 'flux']]
        table = Table.from_pandas(df)
        table.write(output_file.replace('.csv', '.ecsv'), format='ascii.ecsv', overwrite=True)
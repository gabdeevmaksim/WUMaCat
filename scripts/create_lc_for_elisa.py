import os
from astropy.table import Table, Column
import numpy as np
import lightkurve as lk
import re

def fold_and_normalize_lightcurves(input_table_filename, lightcurve_dir, period_column='P'):
    """
    Folds and normalizes TESS light curves based on periods and JDs from an input table.

    Args:
        input_table_filename (str): Path to the input ECSV table file.
        lightcurve_dir (str): Path to the directory containing the TESS light curve files.
        period_column (str, optional): Name of the column containing the periods. Defaults to 'P'.

    Returns:
        list: A list of processed object names or None if there is an error.
    """
    try:
        input_table = Table.read(input_table_filename, format='ascii.ecsv')

        name_col_exists = any(col.lower() == 'name' for col in input_table.colnames)
        if not name_col_exists:
            print("Error: Input table must contain a column named 'name' (case-insensitive).")
            return None

        name_col = next(col for col in input_table.colnames if col.lower() == 'name')
        if name_col.lower() != 'name':
            input_table.rename_column(name_col, 'name')

        if period_column not in input_table.colnames:
            print(f"Error: Input table must contain a column named '{period_column}'.")
            return None

        # Remove duplicate rows based on 'name'
        unique_names = np.unique(input_table['name'])

        processed_objects = []

        for name in unique_names:
            mask = input_table['name'] == name
            row = input_table[mask][0]
            period = row[period_column]
            jd_min = row.get('jd_min')

            try:
                lightcurve_files = [f for f in os.listdir(lightcurve_dir) if re.search(rf"{re.escape(name)}", f)]
                if not lightcurve_files:
                    print(f"Warning: No light curve file found for '{name}'.")
                    continue
                elif len(lightcurve_files) > 1:
                    print(f"Warning: More than one light curve file found for '{name}'. Using first one.")

                lightcurve_file = os.path.join(lightcurve_dir, lightcurve_files[0])

                try:
                    lc_table = Table.read(lightcurve_file, format="ascii.ecsv")
                except Exception as e:
                    print(f"Error reading light curve file for '{name}': {e}")
                    continue

                if period is None or np.isnan(period):
                    print(f"Warning: No valid period provided for '{name}'. Skipping folding.")
                    continue

                if jd_min is None:
                    jd_min = lc_table['jd'][np.argmin(lc_table['flux'])]
                    print(f"Warning: No 'jd_min' provided for '{name}'. Using minimum flux JD: {jd_min}")
                    output_filename = f"tess_lc_{name}_tess_jdmin.csv"
                else:
                    output_filename = f"tess_lc_{name}_vsx_jdmin.csv"
                
                try:
                    jd = lc_table['jd']
                    flux = lc_table['flux']
                    phase = (jd - jd_min) % period
                    phase[phase < 0] = 1 + phase[phase<0]
                    normalized_flux = (flux - np.min(flux)) / (np.max(flux) - np.min(flux))
                    output_table = Table([phase, normalized_flux], names=['phase', 'normalized_flux'])
                    output_table.meta['jd_min'] = jd_min
                    output_table.meta['period'] = period
                    output_table.sort('phase')
                    output_table.write(output_filename, format='csv', overwrite=True)
                    print(f"Normalized light curve for '{name}' saved to {output_filename}")
                    processed_objects.append(name)
                except Exception as e:
                    print(f"Error processing light curve for '{name}': {e}")
                    continue

            except FileNotFoundError:
                print(f"Error: Light curve directory not found: {lightcurve_dir}")
                return None

        return processed_objects

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

if __name__ == "__main__":
    lightcurve_dir = "../tess_curves/"
    processed_objects = fold_and_normalize_lightcurves("./data/sp_final_with_jd_min.ecsv", lightcurve_dir)
    if processed_objects is not None:
        print(f"Processed objects: {processed_objects}")
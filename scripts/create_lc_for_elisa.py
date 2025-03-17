import os
from astropy.table import Table, Column
import numpy as np
import lightkurve as lk
import re
from scipy.optimize import curve_fit

def gaussian(x, a, x0, sigma):
    return a * np.exp(-(x - x0)**2 / (2 * sigma**2))

def find_jdmin(table, period):
    """
    Finds the JDmin, which is the center of an eclipse, from the light curve data by fitting a Gaussian model.

    Args:
        table (astropy.table.Table): The light curve data table.
        period (float): The orbital period of the light curve.

    Returns:
        float: The Julian Date (JD) corresponding to the center of the eclipse.
    """
    x = table['jd']
    y = table['flux'].max() - table['flux']

    # Initial guess for the parameters
    initial_guess = [max(y), x[np.argmax(y)], 0.2 * period]

    # Take points around the initial guess within 0.2 of the period
    mask = (x > initial_guess[1] - 0.2 * period) & (x < initial_guess[1] + 0.2 * period)
    x_fit = x[mask]
    y_fit = y[mask]

    try:
        # Fit the Gaussian model to the data
        popt, _ = curve_fit(gaussian, x_fit, y_fit, p0=initial_guess)

        # The center of the eclipse is the mean of the Gaussian
        jdmin = popt[1]
        return jdmin
    except RuntimeError:
        print("Gaussian fit did not converge.")
        return None

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
        if input_table_filename.endswith('.csv'):
            input_table = Table.read(input_table_filename, format='csv')
        elif input_table_filename.endswith('.ecsv'):
            input_table = Table.read(input_table_filename, format='ascii.ecsv')
        else:
            print("Error: Input table file must be in CSV or ECSV format.")
            return None

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

            try:
                lightcurve_files = [f for f in os.listdir(lightcurve_dir) if re.search(rf"{re.escape(name)}", f)]
                if not lightcurve_files:
                    print(f"Warning: No light curve file found for '{name}'.")
                    continue
                elif len(lightcurve_files) > 1:
                    print(f"Warning: More than one light curve file found for '{name}'. Using first one.")

                lightcurve_file = os.path.join(lightcurve_dir, lightcurve_files[0])

                try:
                    if lightcurve_file.endswith('.csv'):
                        lc_table = Table.read(lightcurve_file, format="csv")
                    elif lightcurve_file.endswith('.ecsv'):
                        lc_table = Table.read(lightcurve_file, format="ascii.ecsv")
                    else:
                        print(f"Error: Light curve file '{lightcurve_file}' must be in CSV or ECSV format.")
                        continue
                except Exception as e:
                    print(f"Error reading light curve file for '{name}': {e}")
                    continue

                if period is None or np.isnan(period):
                    print(f"Warning: No valid period provided for '{name}'. Skipping folding.")
                    continue

                jd_min = find_jdmin(lc_table, period)
                if jd_min is None:
                    print(f"Warning: Could not determine 'jd_min' for '{name}'. Skipping.")
                    continue
                print(f"Calculated 'jd_min' for '{name}': {jd_min}")
                output_filename = f"./data/deb_lcs/tess_lc_{name}_tess_jdmin.csv"
                
                try:
                    jd = lc_table['jd']
                    flux = lc_table['flux']
                    phase = ((jd - jd_min) / period) % 1
                    phase[phase < 0] = 1 + phase[phase < 0]
                    normalized_flux = flux / np.max(flux)
                    output_table = Table([jd, flux, phase, normalized_flux], names=['jd', 'flux', 'phase', 'normalized_flux'])
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
    lightcurve_dir = "../tess_curves/output_files/"
    processed_objects = fold_and_normalize_lightcurves("./data/debcat_with_coord.csv", lightcurve_dir, period_column='Pday')
    if processed_objects is not None:
        print(f"Processed objects: {processed_objects}")
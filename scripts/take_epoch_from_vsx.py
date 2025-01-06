from astroquery.vizier import Vizier
from astropy.table import Table
import astropy.units as u
import time

def take_epoch_from_vsx(input_table, output_filename, catalog="B/vsx/vsx"):
    """
    Adds 'jd_min' column from Vizier to an input Astropy Table based on object name.

    Args:
        input_table (astropy.table.Table or str): Input Astropy Table or path to CSV file.
        output_filename (str): Name of the output ECSV file.
        catalog (str, optional): Vizier catalog to query. Defaults to "B/vsx/vsx".

    Returns:
        astropy.table.Table: The final table with 'jd_min' column (or None if there's an error).

    # Example usage (create a dummy input file first)
        data = {'name': ['V* V1010 Oph', 'V* V1011 Oph', 'Wrong Name Object'], 'ra': [10.0, 20.0, 30.0], 'dec': [40.0, 50.0, 60.0]}
        input_table = Table(data)
        input_table.write("my_objects.csv", format='csv', overwrite=True)
    """

    try:
        if isinstance(input_table, str):
            try:
                input_table = Table.read(input_table, format='ascii.ecsv')
            except Exception as e:
                print(f"Error reading input table: {e}")
                return None
        elif not isinstance(input_table, Table):
            print("Error: Input must be an Astropy Table or a filename.")
            return None

        # Ensure 'name' column exists (case-insensitive)
        name_col_exists = any(col.lower() == 'name' for col in input_table.colnames)
        if not name_col_exists:
            print("Error: Input table must contain a column named 'name' (case-insensitive).")
            return None

        name_col = next(col for col in input_table.colnames if col.lower() == 'name')
        if name_col.lower() != 'name':
            input_table.rename_column(name_col, 'name')

        # Add jd_min column
        input_table['jd_min'] = None

        v = Vizier(columns=['all'])  # Getting only needed columns

        for i in range(len(input_table)):  # Iterate by index
            name = input_table['name'][i]
            start_time = time.perf_counter()
            try:
                result = v.query_object(name, radius=1 * u.arcsec, catalog=catalog)
                if result and len(result[0]) > 0:
                    jd_min = result[0]['_tab1_15'][0]
                    input_table['jd_min'] = jd_min - 2450000  # Assign directly to the row
                    print(f" '_tab1_15' found for '{name}' in Vizier.")
                else:
                    print(f"Warning: No data or '_tab1_15' found for '{name}' in Vizier.")
            except Exception as e:
                print(f"Warning: Error querying Vizier for '{name}': {e}")
                continue
            end_time = time.perf_counter() # end timer for one object
            elapsed_time = end_time - start_time
            print(f"Time for object '{name}': {elapsed_time:.4f} seconds")

        # Save the final table as ECSV
        input_table.write(output_filename, format='ascii.ecsv', overwrite=True)
        print(f"Final table saved to '{output_filename}'.")

        return input_table

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

if __name__ == "__main__":
    input_file = "./data/SP_cross_with_gaia.ecsv"
    output_file = "./data/sp_final_with_jd_min.ecsv"
    processed_table = take_epoch_from_vsx(input_file, output_file)

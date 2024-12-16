import pandas as pd
import os
import time
from astropy.table import Table
from astropy.coordinates import SkyCoord
import astropy.units as u
from astroquery.gaia import Gaia

def gaia_cross_match(input_data, 
                    table_name="my_table", 
                    gaia_table="gaiadr3.gaia_source", 
                    radius=1,
                    output_filename=None):
    """
    Cross-matches objects from a CSV file or Pandas DataFrame with the Gaia archive.

    Args:
        input_data (str or pd.DataFrame): Path to a CSV file or a Pandas DataFrame 
                                         containing object names, RAs, and Decs.
                                         Must have columns 'name', 'ra', and 'dec'.
                                         'ra' and 'dec' can be in string format (HH:MM:SS, DD:MM:SS) or decimal degrees.
        table_name (str, optional): Name for the uploaded table. Defaults to "my_table".
        gaia_table (str, optional): Name of the Gaia table to crossmatch with. Defaults to "gaiadr3.gaia_source".
        radius (int, optional): Crossmatch radius in arcseconds. Defaults to 1.
        output_filename (str, optional): Name of the output file. If provided, the results are saved to this file.

    Returns:
        astropy.table.Table: Crossmatch results, or None if there's an error.

    # Example usage with a Pandas DataFrame
        data = {'name': ['Object1', 'Object2', 'Object3'], # changed here
                'ra': ['10:00:00', '12 30 00', 187.5],  # Example with mixed formats
                'dec': ['+20:00:00', '+30 00 00', 25.0]}
        df = pd.DataFrame(data)

        results = gaia_cross_match(df, output_filename="my_crossmatch_results.ecsv")

     # Example usage with a CSV file
        df.to_csv("my_objects.csv", index=False) # creating the file

        results_from_file = gaia_cross_match("my_objects.csv", radius=5)
        
    """

    username = os.environ.get('GAIA_USERNAME')
    password = os.environ.get('GAIA_PASSWORD')
    full_table_name = f"user_{username}.{table_name}"
    xmatch_table_name = f"xmatch_{table_name}"
    max_attempts = 3 # Maximum number of attempts to check for the table
    wait_time = 3 # Time to wait between attempts in seconds

    try:
        if isinstance(input_data, str):  # Input is a filename
            try:
                df = pd.read_csv(input_data)
            except FileNotFoundError:
                print(f"Error: File not found at {input_data}")
                return None
            except pd.errors.ParserError:
                print(f"Error: Could not parse CSV file at {input_data}. Check file format.")
                return None
        elif isinstance(input_data, pd.DataFrame):  # Input is a DataFrame
            df = input_data.copy() # Create a copy to avoid modifying the original DataFrame
        else:
            print("Error: Input data must be a filename (string) or a Pandas DataFrame.")
            return None

        if not all(col in df.columns for col in ['name', 'ra', 'dec']): # changed here
            print("Error: DataFrame must contain columns 'name', 'ra', and 'dec'.") # changed here
            return None

        Gaia.login(user=username, password=password)

        # Check and delete existing table
        tables = Gaia.load_tables(only_names=True)
        if tables:
            for table in tables:
                if f"user_{username}.{full_table_name}" == table.get_qualified_name():
                    print(f"Table '{full_table_name}' already exists. Deleting...")
                    Gaia.delete_user_table(table_name=table_name)
                    break

        # Create astropy table from DataFrame
        source_table = Table.from_pandas(df)

        # Convert 'ra' and 'dec' to SkyCoord if they are strings
        try:
            coords = SkyCoord(source_table['ra'], source_table['dec'], unit=(u.hourangle, u.deg))
            source_table['ra'] = coords.ra.deg
            source_table['dec'] = coords.dec.deg
        except u.UnitsError:
            try:
                coords = SkyCoord(source_table['ra'], source_table['dec'], unit=(u.deg, u.deg))
                source_table['ra'] = coords.ra.deg
                source_table['dec'] = coords.dec.deg
            except u.UnitsError:
                print("Error: Could not parse RA and Dec. Check the format (HH:MM:SS or decimal degrees).")
                return None
        except TypeError:
            print("Error: Could not parse RA and Dec. Check the format (HH:MM:SS or decimal degrees).")
            return None

        # Upload table
        try:
            Gaia.upload_table(upload_resource=source_table, table_name=table_name, format="votable")
        except Exception as e:
            print(f"Error uploading table: {e}")
            return None

        # Update column flags
        try:
            Gaia.update_user_table(table_name=full_table_name,
                                  list_of_changes=[["ra","flags","Ra"], 
                                                   ["dec","flags","Dec"],
                                                   ["name","flags","Pk"]]) # changed here
        except Exception as e:
            print(f"Error updating table flags: {e}")
            return None

        # Perform crossmatch
        try:
            job = Gaia.cross_match(full_qualified_table_name_a=full_table_name,
                                  full_qualified_table_name_b=gaia_table,
                                  results_table_name=xmatch_table_name,
                                  radius=radius, background=True, verbose=True)
        except Exception as e:
            print(f"Error performing crossmatch: {e}")
            return None

        # Check for the existence of the crossmatch table with time lag
        for attempt in range(max_attempts):
            time.sleep(wait_time)
            try:
                tables = Gaia.load_tables(only_names=True) # Correct way to list tables
                for table in tables:
                    if f"user_{username}.user_{username}.{xmatch_table_name}" == table.get_qualified_name():
                        print(f"Crossmatch table '{xmatch_table_name}' found after {attempt + 1} attempts.")
                        break
                else:
                    continue # if table isn't found continue the loop
                break # if table is found break the loop
            except Exception as e:
                print(f"Error listing tables: {e}")
                if attempt < max_attempts -1:
                    print("Retrying...")
                continue
        else:
            print(f"Error: Crossmatch table '{xmatch_table_name}' not found after {max_attempts} attempts. Something went wrong with crossmatch.")
            return None
        
        # Retrieve results
        try:
            query = (f"SELECT c.separation*3600 AS separation_arcsec, a.*, b.* "
                     f"FROM gaiadr3.gaia_source_lite AS a, {full_table_name} AS b, "
                     f"user_{username}.{xmatch_table_name} AS c "
                     f"WHERE c.gaia_source_source_id = a.source_id AND "
                     f"c.{table_name}_{table_name}_oid = b.{table_name}_oid")
            job = Gaia.launch_job(query=query)
            results = job.get_results()

            if output_filename:
                results.write(output_filename, overwrite=True)
                print(f"Crossmatch results saved to {output_filename}")

            return results
        except Exception as e:
            print(f"Error retrieving or saving results: {e}")
            return None

    except Exception as e:
        print(f"Error: {e}")
        return None

    finally:
        Gaia.logout()

if __name__ == "__main__":
    results_from_file = gaia_cross_match("../data/SP_objects_with_coord.csv", radius=1, output_filename="../data/SP_cross_with_gaia.ecsv")
    
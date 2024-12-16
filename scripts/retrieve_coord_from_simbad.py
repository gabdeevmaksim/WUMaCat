import numpy as np
import pandas as pd
from astroquery.simbad import Simbad
import os

def retrieve_coord_from_simbad(input_data, output_filename=None):
    """
    Retrieves RA and Dec coordinates from SIMBAD for a list of object names.

    Args:
        input_data (str or pd.DataFrame): Path to a CSV file or a Pandas DataFrame 
                                         containing object names. Must have a column named 'name' (case-insensitive).
        output_filename (str, optional): Name of the output file. If provided, the results are saved to this file.

    Returns:
        pd.DataFrame: A Pandas DataFrame with 'ra' and 'dec' columns, or None if there's an error.
    """

    simbad = Simbad()
    simbad.ROW_LIMIT = 0

    try:
        if isinstance(input_data, str):  # Input is a filename
            try:
                filepath = input_data
                df = pd.read_csv(input_data)
            except FileNotFoundError:
                print(f"Error: File not found at {input_data}")
                return None
            except pd.errors.ParserError:
                print(f"Error: Could not parse CSV file at {input_data}. Check file format.")
                return None
        elif isinstance(input_data, pd.DataFrame):  # Input is a DataFrame
            df = input_data.copy()
            filepath = None
        else:
            print("Error: Input data must be a filename (string) or a Pandas DataFrame.")
            return None

        name_col_exists = any(col.lower() == 'name' for col in df.columns)
        if not name_col_exists:
            print("Error: DataFrame must contain a column named 'name' (case-insensitive).")
            return None

        name_col = next(col for col in df.columns if col.lower() == 'name')
        df = df.rename(columns={name_col: 'name'})

        object_names = df['name'].tolist()
        try:
            query_result = simbad.query_objects(object_names)
        except Exception as e:
            print(f"Error querying SIMBAD: {e}")
            return None

        if query_result is None:
            print("Error: SIMBAD query returned None. Check your input names or network connection.")
            return None

        simbad_df = query_result.to_pandas()

        # Ensure SCRIPT_NUMBER_ID is an integer for proper joining
        simbad_df['SCRIPT_NUMBER_ID'] = simbad_df['SCRIPT_NUMBER_ID'].astype(int)

        # Create a new DataFrame with the index shifted by 1
        df_with_index = df.reset_index().rename(columns={'index': 'SCRIPT_NUMBER_ID'})
        df_with_index['SCRIPT_NUMBER_ID'] += 1

        # Merge the DataFrames
        merged_df = pd.merge(df_with_index, simbad_df[['SCRIPT_NUMBER_ID', 'RA', 'DEC']], on='SCRIPT_NUMBER_ID', how='left')

        merged_df = merged_df.drop(columns=['SCRIPT_NUMBER_ID'])

        # Update original ra and dec columns or create them if they don't exist
        df['ra'] = merged_df['RA'].rename('ra')
        df['dec'] = merged_df['DEC'].rename('dec')

        if filepath:
            try:
                df.to_csv(output_filename if output_filename else filepath, index=False)
                print(f"Data saved to {output_filename if output_filename else filepath}")
            except Exception as e:
                print(f"Error saving to file: {e}")
        return df

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

if __name__ == "__main__":
    df_from_csv_with_coords_no_overwrite = retrieve_coord_from_simbad("../data/only_SP_objects.csv", output_filename="../data/SP_objects_with_coord.csv")

    
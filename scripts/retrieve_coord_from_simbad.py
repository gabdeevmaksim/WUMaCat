import numpy as np
import pandas as pd
from astroquery.simbad import Simbad
import os

def retrieve_coord_from_simbad(input_data, overwrite=True):
    """
    Retrieves RA and Dec coordinates from SIMBAD for a list of object names and saves the result.

    Args:
        input_data (str or pd.DataFrame): Path to a CSV file or a Pandas DataFrame 
                                         containing object names.
                                         Must have a column named 'name' (case-insensitive).
        overwrite (bool): If True, overwrites the input file with the new data. If False, saves to a new file.

    Returns:
        pd.DataFrame: A Pandas DataFrame with added 'ra' and 'dec' columns, or None if there's an error.
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

        ra_col_exists = 'ra' in df.columns
        dec_col_exists = 'dec' in df.columns

        # Check if all RA and Dec values are NaN
        if ra_col_exists and dec_col_exists:
            ra_dec_empty = df[['ra', 'dec']].isnull().all().all()
        else:
            ra_dec_empty = True

        if ra_col_exists and dec_col_exists and not ra_dec_empty:
             print("RA and Dec columns already exist and are not all empty. Skipping SIMBAD query for filled rows.")
             objects_to_query = df[df[['ra', 'dec']].isnull().any(axis=1)]
             if objects_to_query.empty:
                 return df
             else:
                 df_to_query = objects_to_query[['name']]
                 df_with_coords = retrieve_coord_from_simbad(df_to_query, overwrite=False)
                 df.update(df_with_coords)
                 if filepath:
                     try:
                         if overwrite:
                             df.to_csv(filepath, index=False)
                             print(f"Data saved to {filepath}")
                         else:
                             base, ext = os.path.splitext(filepath)
                             new_filepath = f"{base}_with_coords{ext}"
                             df.to_csv(new_filepath, index=False)
                             print(f"Data saved to {new_filepath}")
                     except Exception as e:
                         print(f"Error saving to file: {e}")
                 return df

        df['ra'] = np.nan
        df['dec'] = np.nan
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
        merged_df = pd.merge(df, simbad_df[['MAIN_ID', 'RA', 'DEC']], left_on='name', right_on='MAIN_ID', how='left')

        merged_df = merged_df.drop(columns=['MAIN_ID'])
        merged_df = merged_df.rename(columns={'RA': 'ra', 'DEC': 'dec'})

        if filepath:
            try:
                if overwrite:
                    merged_df.to_csv(filepath, index=False)
                    print(f"Data saved to {filepath}")
                else:
                    base, ext = os.path.splitext(filepath)
                    new_filepath = f"{base}_with_coords{ext}"
                    merged_df.to_csv(new_filepath, index=False)
                    print(f"Data saved to {new_filepath}")
            except Exception as e:
                print(f"Error saving to file: {e}")
        return merged_df

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

if __name__ == "__main__":
    df_from_csv_with_coords_no_overwrite = retrieve_coord_from_simbad("../data/only_SP_objects.csv", overwrite=False)

    
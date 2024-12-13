import pandas as pd
import os

def filter_dataframe(csv_filepath, filter_dict, output_filepath=None):
    """
    Reads a CSV file, filters rows based on a dictionary, and optionally saves the result to a new file.

    Args:
        csv_filepath: Path to the input CSV file.
        filter_dict: A dictionary defining the filters (see previous examples).
        output_filepath (optional): Path to save the filtered DataFrame to a new CSV file. If None, the result is not saved.

    Returns:
        A Pandas DataFrame containing the filtered rows, or None if there's an error.
        Prints informative messages to the console.

    # Example usage:
        filepath = "my_data.csv"

        filter_dictionary = {
            "city": ["New York", "London"],
            "age": (25, 40),
            "value": (30, 60)
        }

        output_file = "filtered_data/output.csv" 

        filtered_df = filter_dataframe(filepath, filter_dictionary, output_file)
    """
    try:
        df = pd.read_csv(csv_filepath)
    except FileNotFoundError:
        print(f"Error: File not found at {csv_filepath}")
        return None
    except pd.errors.ParserError:
        print(f"Error: Could not parse CSV file at {csv_filepath}. Check file format.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while reading the file: {e}")
        return None

    filtered_rows = pd.Series([True] * len(df))

    for column, filter_value in filter_dict.items():
        if column not in df.columns:
            print(f"Warning: Column '{column}' not found in the CSV file. Skipping this filter.")
            continue

        if isinstance(filter_value, list):
            filtered_rows &= df[column].isin(filter_value)
        elif isinstance(filter_value, tuple) and len(filter_value) == 2:
            try:
                min_val, max_val = filter_value
                df[column] = pd.to_numeric(df[column], errors='coerce')
                filtered_rows &= (df[column] >= min_val) & (df[column] <= max_val)
            except (TypeError, ValueError):
                print(f"Warning: Cannot compare column '{column}' with range {filter_value}. Check data type. Skipping this filter.")
                continue
        else:
            print(f"Warning: Invalid filter value for column '{column}'. Must be a list or a 2-tuple. Skipping this filter.")

    filtered_df = df[filtered_rows]

    if output_filepath:
        try:
            # Create the directory if it doesn't exist
            os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
            filtered_df.to_csv(output_filepath, index=False)  # index=False prevents writing the DataFrame index
            print(f"Filtered data saved to {output_filepath}")
        except Exception as e:
            print(f"Error saving to file: {e}")
    
    return filtered_df

if __name__ == "__main__":
    filepath = "../data/WUMaCat.csv"

    filter_dictionary = {
        "QT": ["SP"]
    }

    output_file = "../data/only_SP_objects.csv"

    filtered_df = filter_dataframe(filepath, filter_dictionary, output_file)

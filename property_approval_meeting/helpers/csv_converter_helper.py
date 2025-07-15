# my_app/utils/csv_converter_helper.py
import pandas as pd
import os

def convert_csv_to_excel_preserve_data(csv_file_path, excel_file_path, columns_to_force_text=None, logger_func=None, style_obj=None):
    """
    Converts a CSV file to an Excel (.xlsx) file, preserving all data
    as text. For specific columns (e.g., bank account numbers), it explicitly
    forces them to remain text by prepending a SPACE, preventing Excel's
    auto-conversion on cell interaction.

    Args:
        csv_file_path (str): The path to the input CSV file.
        excel_file_path (str): The desired path for the output Excel file.
        columns_to_force_text (list): Optional list of column names (strings)
                                      to prepend with a SPACE in Excel.
                                      Use this for fields like bank account numbers,
                                      long IDs, or ZIP codes.
        logger_func (callable): A function to use for logging messages (e.g., self.stdout.write).
        style_obj (object): An object with style methods (e.g., self.style).
    Returns:
        tuple: (bool, str) - True on success, False on failure, and a message.
    """
    # Set up basic logging if not provided by Django's command
    if not logger_func:
        logger_func = print

    def _log(message, style_method=None):
        if style_obj and style_method and hasattr(style_obj, style_method):
            # Use Django's colored output if style_obj is provided
            logger_func(getattr(style_obj, style_method)(message))
        else:
            # Fallback to plain print
            logger_func(message)

    if not os.path.exists(csv_file_path):
        return False, f"CSV file not found at '{csv_file_path}'"

    try:
        # Read the CSV, forcing all columns to be read as string type.
        # This is crucial for initial preservation.
        # keep_default_na=False ensures empty strings remain empty, not pandas' NaN.
        # You can add 'encoding=' argument if you know your CSV's encoding (e.g., 'utf-8', 'latin1')
        df = pd.read_csv(csv_file_path, dtype=str, keep_default_na=False)

        # MODIFIED LOGIC: Prepend a SPACE to specified columns
        if columns_to_force_text:
            for col_name in columns_to_force_text:
                if col_name in df.columns:
                    # Apply a lambda function to each cell: add a leading space only if not empty
                    # and not already starting with a space (to prevent multiple leading spaces)
                    df[col_name] = df[col_name].apply(
                        lambda x: " " + str(x) if pd.notna(x) and str(x).strip() != '' and not str(x).startswith(" ") else x
                    )
                else:
                    _log(f"Warning: Column '{col_name}' not found in CSV. Skipping force text formatting.", style_method='WARNING')

        # Ensure the directory for the output file exists
        output_dir = os.path.dirname(excel_file_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Write the DataFrame to an Excel file.
        # index=False prevents writing the DataFrame index as a column in Excel.
        # engine='openpyxl' ensures it creates an .xlsx file.
        df.to_excel(excel_file_path, index=False, engine='openpyxl')

        return True, f"Successfully converted '{csv_file_path}' to '{excel_file_path}'. All data preserved as text."

    except ImportError:
        return False, "Required library 'openpyxl' not found. Please install it: pip install openpyxl"
    except Exception as e:
        return False, f"An unexpected error occurred during conversion: {e}"
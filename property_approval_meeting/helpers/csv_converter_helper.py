import pandas as pd
import os

def convert_csv_to_excel_preserve_data(csv_file_path, excel_file_path, columns_to_force_text=None, logger_func=None, style_obj=None):
    if not logger_func:
        logger_func = print

    def _log(message, style_method=None):
        if style_obj and style_method and hasattr(style_obj, style_method):
            logger_func(getattr(style_obj, style_method)(message))
        else:
            logger_func(message)

    if not os.path.exists(csv_file_path):
        return False, f"CSV file not found at '{csv_file_path}'"

    try:
        df = pd.read_csv(csv_file_path, dtype=str, keep_default_na=False)
        if columns_to_force_text:
            for col_name in columns_to_force_text:
                if col_name in df.columns:
                    df[col_name] = df[col_name].apply(
                        lambda x: " " + str(x) if pd.notna(x) and str(x).strip() != '' and not str(x).startswith(" ") else x
                    )
                else:
                    _log(f"Warning: Column '{col_name}' not found in CSV. Skipping force text formatting.", style_method='WARNING')

        output_dir = os.path.dirname(excel_file_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        df.to_excel(excel_file_path, index=False, engine='openpyxl')

        return True, f"Successfully converted '{csv_file_path}' to '{excel_file_path}'. All data preserved as text."

    except ImportError:
        return False, "Required library 'openpyxl' not found. Please install it: pip install openpyxl"
    except Exception as e:
        return False, f"An unexpected error occurred during conversion: {e}"
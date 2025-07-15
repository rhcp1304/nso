import os
from django.core.management.base import BaseCommand, CommandError
from ...helpers.csv_converter_helper import convert_csv_to_excel_preserve_data

class Command(BaseCommand):
    help = 'Converts a CSV file to an Excel (.xlsx) file, preserving all data as text.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--csv_file_path',
            type=str,
            help='The absolute or relative path to the input CSV file.'
        )
        parser.add_argument(
            '--output_excel_path',
            type=str,
            help='Optional: The path for the output Excel file. If not provided, '
                 'it will be created in the same directory as the CSV with a .xlsx extension '
                 'and the same base name.',
            required=False
        )
        parser.add_argument(
            '--force_text_columns',
            type=str,
            help='Optional: Comma-separated list of column names that should always '
                 'be treated as text in Excel (e.g., "BankAccount,ProductID"). '
                 'An apostrophe will be prepended to values in these columns to force Excel to render them as text.',
            required=False
        )

    def handle(self, *args, **options):
        csv_file_path = options['csv_file_path']
        output_excel_path = options['output_excel_path']
        force_text_columns_str = options['force_text_columns']
        columns_to_force_text = [col.strip() for col in force_text_columns_str.split(',')] if force_text_columns_str else None
        csv_file_path_abs = os.path.abspath(csv_file_path)
        if not output_excel_path:
            csv_dir = os.path.dirname(csv_file_path_abs)
            csv_name_without_ext = os.path.splitext(os.path.basename(csv_file_path_abs))[0]
            output_excel_path_abs = os.path.join(csv_dir, f"{csv_name_without_ext}.xlsx")
        else:
            output_excel_path_abs = os.path.abspath(output_excel_path)
            if not output_excel_path_abs.lower().endswith('.xlsx'):
                output_excel_path_abs += '.xlsx'

        self.stdout.write(f"Attempting to convert CSV: '{csv_file_path_abs}'")
        self.stdout.write(f"Output will be saved to: '{output_excel_path_abs}'")
        if columns_to_force_text:
            self.stdout.write(f"Applying forced text formatting (prepending apostrophe) to columns: {', '.join(columns_to_force_text)}")

        success, message = convert_csv_to_excel_preserve_data(
            csv_file_path_abs,
            output_excel_path_abs,
            columns_to_force_text=columns_to_force_text,
            logger_func=self.stdout.write,
            style_obj=self.style
        )

        if success:
            self.stdout.write(self.style.SUCCESS(message))
        else:
            raise CommandError(self.style.ERROR(message))
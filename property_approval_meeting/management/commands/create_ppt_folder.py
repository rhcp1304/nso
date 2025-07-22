import os
from django.core.management.base import BaseCommand, CommandError
from ...helpers.create_ppt_folder_helper import process_ppt_and_create_folder, CREDENTIALS_FILE


class Command(BaseCommand):
    help = 'Processes a PowerPoint file from Google Drive to extract market name and create a folder using OAuth 2.0. Credentials are loaded from a fixed path.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--ppt-file-id',
            type=str,
            help='Google Drive File ID of the PowerPoint presentation.',
            required=True,
        )
        parser.add_argument(
            '--parent-folder-id',
            type=str,
            help='Google Drive File ID of the parent folder where the new market name folder will be created.',
            required=True,
        )

    def handle(self, *args, **options):
        ppt_file_id = options['ppt_file_id']
        parent_folder_id = options['parent_folder_id']
        if not os.path.exists(CREDENTIALS_FILE):
            raise CommandError(
                f"Error: OAuth client secrets file not found at '{CREDENTIALS_FILE}'. "
                "Please ensure 'client_secret.json' is placed in the same directory as 'ppt_processor_helpers.py'."
            )

        self.stdout.write(self.style.SUCCESS("Starting PPT processing and folder creation using OAuth 2.0..."))

        try:
            process_ppt_and_create_folder(ppt_file_id, parent_folder_id)
            self.stdout.write(self.style.SUCCESS("Process completed successfully."))
        except Exception as e:
            raise CommandError(f"An error occurred during processing: {e}")


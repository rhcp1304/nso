import os
from django.core.management.base import BaseCommand, CommandError

from ...helpers.drive_uploader_helper import get_authenticated_service, find_drive_folder, upload_file_to_drive_folder

class Command(BaseCommand):
    help = 'Uploads a local file to a specified shared Google Drive folder.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--file_path',
            type=str,
            required=True,
            help='The full path to the local file you want to upload (e.g., "/home/user/my_video.mp4" or "C:\\Users\\user\\Documents\\report.pdf").'
        )
        parser.add_argument(
            '--folder_name',
            type=str,
            required=True,
            help='The name of the target shared Google Drive folder where the file will be uploaded.'
        )

    def handle(self, *args, **options):
        local_file_path = options['file_path']
        folder_name = options['folder_name']

        if not os.path.exists(local_file_path):
            raise CommandError(f"Local file not found at: '{local_file_path}'")

        self.stdout.write(f"Attempting to upload '{local_file_path}' to Google Drive folder '{folder_name}'...")

        service = get_authenticated_service()
        if not service:
            raise CommandError("Failed to authenticate with Google Drive API. Please check your credentials.json and internet connection.")

        folder_id = find_drive_folder(service, folder_name)
        if not folder_id:
            raise CommandError(f"Could not find shared Google Drive folder named '{folder_name}'. Ensure the folder exists and you have access.")

        uploaded_file_id = upload_file_to_drive_folder(service, local_file_path, folder_id)

        if uploaded_file_id:
            self.stdout.write(self.style.SUCCESS(f"\nSuccessfully uploaded file with ID: {uploaded_file_id}"))
            self.stdout.write(self.style.SUCCESS(f"You can view it here: https://drive.google.com/file/d/{uploaded_file_id}/view"))
        else:
            raise CommandError("\nFile upload to Google Drive failed.")


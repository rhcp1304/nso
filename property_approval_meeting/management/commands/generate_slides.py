import os
from django.core.management.base import BaseCommand, CommandError
from property_approval_meeting.helpers.google_slides_helper import create_slides_from_folder

class Command(BaseCommand):
    help = 'Generates a Google Slides presentation from a Google Drive folder containing a PPT and videos using user credentials.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--folder_id',
            type=str,
            help='The Google Drive folder ID containing the PPT and video files.'
        )

    def handle(self, *args, **options):
        folder_id = options['folder_id']
        client_secret_path = r'C:\Users\Ankit.Anand\PycharmProjects\nso\property_approval_meeting\helpers\credentials.json'
        token_path = os.path.join(os.path.dirname(client_secret_path), 'token.json')
        if not os.path.exists(client_secret_path):
            raise CommandError(f"Client secret file not found at: {client_secret_path}")
        self.stdout.write(f"Starting slide generation for folder ID: {folder_id}")
        self.stdout.write(f"Using client secret from: {client_secret_path}")
        self.stdout.write(f"Token will be stored/loaded from: {token_path}")
        presentation_id = create_slides_from_folder(folder_id, client_secret_path, token_path)

        if presentation_id:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Successfully created/updated Google Slides presentation. ID: {presentation_id}"
                )
            )
            self.stdout.write(
                self.style.SUCCESS(
                    f"You can view it at: https://docs.google.com/presentation/d/{presentation_id}/edit"
                )
            )
        else:
            raise CommandError("Failed to generate Google Slides presentation.")


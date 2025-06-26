import os
from django.core.management.base import BaseCommand, CommandError

from ...helpers import extract_video_links_from_ppt_helper as helper

class Command(BaseCommand):
    help = 'Uploads a local file to a specified shared Google Drive folder.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--ppt_file_path',
            type=str,
            required=True,
            help='The full path to the local ppt file you want to extract videos links from").'
        )

    def handle(self, *args, **options):
        local_file_path = options['ppt_file_path']
        if not os.path.exists(local_file_path):
            raise CommandError(f"Local file not found at: '{local_file_path}'")
        print(f"Attempting to extract video links from the last slide of: {local_file_path}")
        links = helper.extract_all_potential_links_from_last_slide(local_file_path)
        if links:
            print("\nExtracted video links from the last slide:")
            for link in links:
                print(f"- {link}")
        else:
            print("\nNo video links found on the last slide, or an error occurred.")
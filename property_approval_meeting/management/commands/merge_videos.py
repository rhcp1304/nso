import os
from django.core.management.base import BaseCommand, CommandError

# Import the helper function from your video_merger_helper.py
# Assuming video_merger_helper.py is in the project root.
# Adjust the import path if you placed it elsewhere (e.g., 'my_app.helpers.video_merger_helper')
from ...helpers.video_merger_helper import merge_videos_in_folder


class Command(BaseCommand):
    help = 'Merges all video files in a specified local folder into a single video file.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--input_folder',
            type=str,
            required=True,
            help="Path to the folder containing the video files to merge (e.g., 'C:\\videos\\parts')."
        )
        parser.add_argument(
            '--output_name',
            type=str,
            default='merged_video.mp4',
            help="Name of the output merged video file (e.g., 'my_final_video.mp4'). "
                 "If a full path is not provided, it will be saved in the current working directory."
        )

    def handle(self, *args, **options):
        input_folder = options['input_folder']
        output_name = options['output_name']

        self.stdout.write(f"Starting video merge process for folder: {input_folder}")
        self.stdout.write(f"Output video will be named: {output_name}")

        if merge_videos_in_folder(input_folder, output_name):
            self.stdout.write(self.style.SUCCESS("\nVideo merging completed successfully!"))
        else:
            raise CommandError("\nVideo merging failed. Please check the error messages above.")


from django.core.management.base import BaseCommand, CommandError
from ...helpers import add_timestamp_to_youtube_video_helper as yh


class Command(BaseCommand):
    help = 'Adds timestamps to a YouTube video description using OAuth 2.0.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--video_id',
            type=str,
            help='The ID of the YouTube video to update.'
        )
        parser.add_argument(
            '--timestamps',
            type=str,
            nargs='+',
            help='A list of timestamp strings to add (e.g., "0:00 - Intro" "1:30 - Chapter 2").'
        )

    def handle(self, *args, **options):
        video_id = options['video_id']
        timestamps = options['timestamps']

        if not video_id:
            raise CommandError("A video ID is required.")
        if not timestamps:
            raise CommandError("At least one timestamp string is required.")

        self.stdout.write(f"Attempting to add timestamps to video ID: {video_id}")

        if yh.add_timestamps_to_video(video_id, timestamps):
            self.stdout.write(self.style.SUCCESS("Timestamps added successfully."))
        else:
            self.stdout.write(self.style.ERROR("Failed to add timestamps. See details above."))
            raise CommandError("Timestamp update failed.")
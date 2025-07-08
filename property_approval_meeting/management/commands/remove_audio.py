from django.core.management.base import BaseCommand, CommandError
from ...helpers import remove_audio_helper as helper

class Command(BaseCommand):
    help = 'Removes audio from a specified video file using FFmpeg.'

    def add_arguments(self, parser):
        parser.add_argument(
            'input_path',
            type=str,
            help='The path to the input video file (e.g., "path/to/my_video.mp4").'
        )
        parser.add_argument(
            'output_path',
            type=str,
            help='The path where the new video file (without audio) will be saved (e.g., "path/to/output_video_no_audio.mp4").'
        )

    def handle(self, *args, **options):
        input_video_path = options['input_path']
        output_video_path = options['output_path']

        self.stdout.write(f"Attempting to remove audio from '{input_video_path}'...")

        # Call the helper function
        success = helper.remove_audio_from_video(input_video_path, output_video_path)

        if success:
            self.stdout.write(self.style.SUCCESS(f"Successfully removed audio. Output saved to: {output_video_path}"))
        else:
            self.stderr.write(self.style.ERROR(f"Failed to remove audio from '{input_video_path}'."))
            raise CommandError("Audio removal process failed.")


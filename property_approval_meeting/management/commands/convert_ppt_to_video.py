from django.core.management.base import BaseCommand, CommandError
from ...helpers.convert_ppt_to_video_helper import convert_pptx_to_video, ConversionError # <-- IMPORTANT: Replace 'my_django_app' with your actual Django app name!

class Command(BaseCommand):
    help = 'Exports a PowerPoint presentation (.pptx) to a video file (.mp4) using portable LibreOffice, PyMuPDF, and FFmpeg.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--ppt_path',
            type=str,
            required=True,
            help='Full path to the input PowerPoint presentation file (.pptx).'
        )
        parser.add_argument(
            '--output_video_path',
            type=str,
            required=True,
            help='Full path for the output video file (.mp4).'
        )
        parser.add_argument(
            '--slide_duration_seconds',
            type=int,
            default=5,
            help='Duration (in seconds) each slide will be displayed in the video.'
        )
        parser.add_argument(
            '--resolution',
            type=str,
            default='1920x1080',
            help='Resolution of the output video (e.g., "1920x1080").'
        )
        parser.add_argument(
            '--frame_rate',
            type=int,
            default=30,
            help='Frame rate of the output video (frames per second).'
        )

    def handle(self, *args, **options):
        ppt_path = options['ppt_path']
        output_video_path = options['output_video_path']
        slide_duration_seconds = options['slide_duration_seconds']
        resolution = options['resolution']
        frame_rate = options['frame_rate']

        try:
            convert_pptx_to_video(
                ppt_path=ppt_path,
                output_video_path=output_video_path,
                slide_duration_seconds=slide_duration_seconds,
                resolution=resolution,
                frame_rate=frame_rate,
                stdout=self.stdout,
                style=self.style
            )
            self.stdout.write(self.style.SUCCESS("Overall video conversion process completed successfully."))
        except ConversionError as e:
            raise CommandError(f"Video conversion failed: {e}")
        except Exception as e:
            raise CommandError(f"An unexpected error occurred during command execution: {e}")
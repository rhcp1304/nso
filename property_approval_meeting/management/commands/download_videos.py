import os
from django.core.management.base import BaseCommand, CommandError
from urllib.parse import urlparse  # Import urlparse for parsing URLs
from ...helpers import video_downloader_helper
from ...helpers.video_downloader_helper import get_gdrive_authenticated_service,download_youtube_vimeo_etc,download_google_drive_video,download_generic_video # This imports the module by its name


class Command(BaseCommand):
    help = 'Downloads videos from a list of URLs provided in a text file.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--url_list_file',
            type=str,
            required=True,
            help="Path to the text file containing one URL per line."
        )
        parser.add_argument(
            '--output_dir',
            type=str,
            default='downloads',
            help="Directory to save downloaded videos. Will be created if it doesn't exist."
        )

    def handle(self, *args, **options):
        url_list_file = options['url_list_file']
        output_dir = options['output_dir']
        abs_output_dir = os.path.abspath(output_dir)
        os.makedirs(abs_output_dir, exist_ok=True)
        self.stdout.write(f"Saving videos to: {abs_output_dir}")
        gdrive_service = None
        if os.path.exists(video_downloader_helper.GDRIVE_CREDENTIALS_FILE):  # Corrected access
            self.stdout.write("\nAttempting to authenticate for Google Drive...")
            gdrive_service = get_gdrive_authenticated_service()
            if not gdrive_service:
                self.stdout.write(self.style.WARNING(
                    "Google Drive API authentication failed. Google Drive links will not be downloaded."))
            else:
                self.stdout.write(self.style.SUCCESS("Google Drive API service ready."))
        else:
            self.stdout.write(self.style.WARNING(
                "No 'credentials.json' found for Google Drive API. Google Drive links will not be downloaded."))
        urls_to_download = []
        try:
            with open(url_list_file, 'r', encoding='utf-8') as f:
                urls_to_download = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            raise CommandError(f"Error: URL list file not found at '{url_list_file}'")
        except Exception as e:
            raise CommandError(f"Error reading URL list file: {e}")

        if not urls_to_download:
            self.stdout.write("No URLs found in the provided file.")
            return

        self.stdout.write(f"\nFound {len(urls_to_download)} URLs to process.")
        successful_downloads = 0

        for i, url in enumerate(urls_to_download):
            self.stdout.write(f"\n--- Processing URL {i + 1}/{len(urls_to_download)}: {url} ---")
            downloaded = False
            parsed_url = urlparse(url)
            if "youtube.com" in parsed_url.netloc or "youtu.be" in parsed_url.netloc or \
                    "vimeo.com" in parsed_url.netloc or "dailymotion.com" in parsed_url.netloc:
                downloaded = download_youtube_vimeo_etc(url, abs_output_dir)
            elif "drive.google.com" in parsed_url.netloc:
                if gdrive_service:
                    downloaded = download_google_drive_video(gdrive_service, url, abs_output_dir)
                else:
                    self.stdout.write(
                        self.style.WARNING("  Skipping Google Drive link: Google Drive API not authenticated."))
            else:
                if any(ext in parsed_url.path.lower() for ext in ['.mp4', '.mov', '.webm', '.avi', '.mkv', '.flv']):
                    downloaded = download_generic_video(url, abs_output_dir)
                else:
                    self.stdout.write(self.style.WARNING(
                        f"  Warning: URL not recognized as a supported video platform or direct video link. Skipping: {url}"))

            if downloaded:
                successful_downloads += 1

        self.stdout.write(self.style.SUCCESS(f"\n--- Download Summary ---"))
        self.stdout.write(self.style.SUCCESS(f"Total URLs processed: {len(urls_to_download)}"))
        self.stdout.write(self.style.SUCCESS(f"Successful downloads: {successful_downloads}"))
        self.stdout.write(self.style.WARNING(f"Failed downloads: {len(urls_to_download) - successful_downloads}"))
        self.stdout.write(self.style.SUCCESS("------------------------"))

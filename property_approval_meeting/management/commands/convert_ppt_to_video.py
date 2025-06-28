import os
import subprocess
import shutil
import platform
import re
import tempfile
import fitz  # <--- NEW: Import for PyMuPDF
from django.core.management.base import BaseCommand, CommandError

# --- Configuration for Portable LibreOffice and FFmpeg ---
# IMPORTANT: You MUST update these paths to match the exact locations on YOUR system.
# If these paths are incorrect, the script will try to use system-wide 'soffice'/'ffmpeg'
# which might not exist or be the correct versions.

# Example Path (replace with your actual path, e.g., C:\LibreOfficePortable\App\libreoffice\program\soffice.exe)
PORTABLE_SOFFICE_PATH = r"C:\Users\Ankit.Anand\Downloads\LibreOfficePortable\App\libreoffice\program\soffice.exe"

# Example Path (replace with your actual path, e.g., C:\ffmpeg\bin\ffmpeg.exe)
# Remember that the 'gyan.dev' builds often have the executable inside a 'bin' folder
PORTABLE_FFMPEG_PATH = r"C:\Users\Ankit.Anand\Downloads\ffmpeg\ffmpeg\bin\ffmpeg.exe"

# Fallback to system PATH if portable paths are not set or don't exist
SOFFICE_EXEC = PORTABLE_SOFFICE_PATH if os.path.exists(PORTABLE_SOFFICE_PATH) else "soffice"
FFMPEG_EXEC = PORTABLE_FFMPEG_PATH if os.path.exists(PORTABLE_FFMPEG_PATH) else "ffmpeg"


# --------------------------------------------------------

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

        self.stdout.write(f"Starting PPT to Video conversion for '{ppt_path}'...")
        self.stdout.write(f"Using LibreOffice portable at: {SOFFICE_EXEC}")
        self.stdout.write(f"Using FFmpeg portable at: {FFMPEG_EXEC} (for final video creation)")

        if not os.path.exists(ppt_path):
            raise CommandError(f"PowerPoint file not found at '{ppt_path}'")
        if not os.path.isfile(ppt_path):
            raise CommandError(f"Path '{ppt_path}' is not a file.")

        output_video_dir = os.path.dirname(output_video_path)
        if output_video_dir and not os.path.exists(output_video_dir):
            os.makedirs(output_video_dir)

        try:
            with tempfile.TemporaryDirectory(dir=os.path.dirname(output_video_path),
                                             prefix="ppt_video_temp_") as temp_dir:
                self.stdout.write(f"Created temporary directory: {temp_dir}")

                # --- Step 1: Convert PPTX to PDF using LibreOffice ---
                self.stdout.write("Converting PPTX to PDF using LibreOffice...")
                ppt_base_name = os.path.splitext(os.path.basename(ppt_path))[0]
                output_pdf_path = os.path.join(temp_dir, f"{ppt_base_name}.pdf")

                libreoffice_pdf_command = [
                    SOFFICE_EXEC,
                    "--headless",
                    "--convert-to", "pdf",
                    "--outdir", temp_dir,
                    ppt_path
                ]

                try:
                    self.stdout.write(f"LibreOffice command: {' '.join(libreoffice_pdf_command)}")
                    subprocess.run(libreoffice_pdf_command, check=True, capture_output=True, text=True, timeout=600)
                    self.stdout.write(self.style.SUCCESS(f"PPTX converted to PDF: {output_pdf_path}"))
                    if not os.path.exists(output_pdf_path):
                        generated_files = [f for f in os.listdir(temp_dir) if f.lower().endswith('.pdf')]
                        if generated_files:
                            output_pdf_path = os.path.join(temp_dir, generated_files[0])
                            self.stdout.write(self.style.WARNING(f"PDF found at alternative path: {output_pdf_path}"))
                        else:
                            raise CommandError(
                                f"LibreOffice did not generate a PDF file in {temp_dir}. Check permissions or LibreOffice output behavior.")

                except FileNotFoundError:
                    raise CommandError(
                        f"Error: '{SOFFICE_EXEC}' command not found. Please ensure LibreOffice Portable path is correct or installed.")
                except subprocess.TimeoutExpired:
                    raise CommandError(
                        f"Error: LibreOffice PDF conversion timed out after 600 seconds. Increase timeout if presentation is very large.")
                except subprocess.CalledProcessError as e:
                    raise CommandError(
                        f"Error during LibreOffice PDF conversion:\nStderr: {e.stderr}\nStdout: {e.stdout}\nEnsure LibreOffice can open the PPTX file.")
                except Exception as e:
                    raise CommandError(f"An unexpected error occurred during LibreOffice PDF processing: {e}")

                # --- Step 2: Convert PDF to PNG images using PyMuPDF ---
                self.stdout.write("Converting PDF pages to PNG images using PyMuPDF...")
                generated_pngs = []
                try:
                    doc = fitz.open(output_pdf_path)
                    for i, page in enumerate(doc):
                        # You can adjust resolution here if needed. 'dpi' parameter.
                        # For 1920x1080 resolution, assuming 96 DPI, a zoom of ~2 is 192 DPI.
                        # Calculate zoom to roughly match desired width if possible
                        output_width = int(resolution.split('x')[0])
                        zoom_factor = output_width / page.rect.width  # Calculate zoom based on desired output width

                        pix = page.get_pixmap(
                            matrix=fitz.Matrix(zoom_factor, zoom_factor))  # Render page with calculated zoom
                        output_image_path = os.path.join(temp_dir, f"slide_{i:03d}.png")
                        pix.save(output_image_path)
                        generated_pngs.append(output_image_path)
                    doc.close()
                    self.stdout.write(self.style.SUCCESS(
                        f"PDF conversion to {len(generated_pngs)} PNG images complete using PyMuPDF."))

                except FileNotFoundError:
                    raise CommandError(f"Error: PDF file not found at '{output_pdf_path}' for PyMuPDF processing.")
                except Exception as e:
                    raise CommandError(f"Error during PyMuPDF PDF to PNG conversion: {e}\n"
                                       f"Ensure PyMuPDF (fitz) is installed: `pip install pymupdf`.")

                if not generated_pngs:
                    raise CommandError(f"Error: PyMuPDF did not generate any PNG images from the PDF in '{temp_dir}'.")
                self.stdout.write(
                    f"Found {len(generated_pngs)} PNG images (e.g., {os.path.basename(generated_pngs[0])}) for video creation.")

                # --- Step 3: Stitch PNG images into video using FFmpeg ---
                # FFmpeg is still used here, as it's the standard for video encoding.
                # If you continue to have issues, ensure this FFmpeg build is stable.
                self.stdout.write(
                    f"Stitching images into video using FFmpeg (each slide for {slide_duration_seconds}s)...")
                input_video_image_pattern = os.path.join(temp_dir, "slide_%03d.png")
                input_framerate = 1 / slide_duration_seconds

                # Split resolution into width and height for FFmpeg filter
                res_width, res_height = resolution.split('x')  # <--- NEW LINE: Split '1920x1080' into '1920' and '1080'

                ffmpeg_video_command = [
                    FFMPEG_EXEC,
                    "-y",
                    "-framerate", str(input_framerate),
                    "-i", input_video_image_pattern,
                    "-c:v", "libx264",
                    "-pix_fmt", "yuv420p",
                    # CORRECTED -vf filter: Use res_width:res_height for the pad filter's dimensions
                    "-vf",
                    f"scale={resolution}:force_original_aspect_ratio=decrease,pad={res_width}:{res_height}:(ow-iw)/2:(oh-ih)/2",
                    "-r", str(frame_rate),
                    output_video_path
                ]

                try:
                    self.stdout.write(f"FFmpeg video creation command: {' '.join(ffmpeg_video_command)}")
                    subprocess.run(ffmpeg_video_command, check=True, capture_output=True, text=True, timeout=600)
                    self.stdout.write(self.style.SUCCESS(f"Video created successfully: {output_video_path}"))
                except FileNotFoundError:
                    raise CommandError(
                        f"Error: '{FFMPEG_EXEC}' command not found. Please ensure FFmpeg Portable path is correct or installed.")
                except subprocess.TimeoutExpired:
                    raise CommandError(
                        f"Error: FFmpeg video creation timed out after 600 seconds. Increase timeout if video is very long.")
                except subprocess.CalledProcessError as e:
                    raise CommandError(
                        f"Error during FFmpeg video creation:\nStderr: {e.stderr}\nStdout: {e.stdout}\nEnsure FFmpeg arguments are correct and images are valid.")
                except Exception as e:
                    raise CommandError(f"An unexpected error occurred during FFmpeg video processing: {e}")

                self.stdout.write(f"Temporary directory {temp_dir} automatically cleaned up.")

        except Exception as e:
            raise CommandError(f"An overall error occurred during the conversion process: {e}")
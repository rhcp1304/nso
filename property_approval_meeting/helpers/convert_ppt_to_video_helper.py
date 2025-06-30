import os
import subprocess
import tempfile
import fitz

PORTABLE_SOFFICE_PATH = r"C:\Users\Ankit.Anand\Downloads\LibreOfficePortable\App\libreoffice\program\soffice.exe"
PORTABLE_FFMPEG_PATH = r"C:\Users\Ankit.Anand\Downloads\ffmpeg\ffmpeg\bin\ffmpeg.exe"

SOFFICE_EXEC = PORTABLE_SOFFICE_PATH if os.path.exists(PORTABLE_SOFFICE_PATH) else "soffice"
FFMPEG_EXEC = PORTABLE_FFMPEG_PATH if os.path.exists(PORTABLE_FFMPEG_PATH) else "ffmpeg"

class ConversionError(Exception):
    pass

def convert_pptx_to_video(
    ppt_path: str,
    output_video_path: str,
    slide_duration_seconds: int = 5,
    resolution: str = '1920x1080',
    frame_rate: int = 30,
    stdout=None,
    style=None
):

    class _DummyStdout:
        def write(self, msg, ending='\n'):
            print(msg, end=ending)
    _stdout = stdout if stdout is not None else _DummyStdout()
    _style = style if style is not None else type('DummyStyle', (object,), {'SUCCESS': lambda x:x, 'WARNING': lambda x:x, 'ERROR': lambda x:x})()
    _stdout.write(f"Starting PPT to Video conversion for '{ppt_path}'...")
    _stdout.write(f"Using LibreOffice portable at: {SOFFICE_EXEC}")
    _stdout.write(f"Using FFmpeg portable at: {FFMPEG_EXEC} (for final video creation)")

    if not os.path.exists(ppt_path):
        raise ConversionError(f"PowerPoint file not found at '{ppt_path}'")
    if not os.path.isfile(ppt_path):
        raise ConversionError(f"Path '{ppt_path}' is not a file.")

    output_video_dir = os.path.dirname(output_video_path)
    if output_video_dir and not os.path.exists(output_video_dir):
        os.makedirs(output_video_dir)

    try:
        with tempfile.TemporaryDirectory(dir=os.path.dirname(output_video_path),
                                         prefix="ppt_video_temp_") as temp_dir:
            _stdout.write(f"Created temporary directory: {temp_dir}")
            _stdout.write("Converting PPTX to PDF using LibreOffice...")
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
                _stdout.write(f"LibreOffice command: {' '.join(libreoffice_pdf_command)}")
                subprocess.run(libreoffice_pdf_command, check=True, capture_output=True, text=True, timeout=600)
                print("-------------------------------")
                print(output_video_path)
                _stdout.write(_style.SUCCESS(f"PPTX converted to PDF: {output_pdf_path}"))
                if not os.path.exists(output_pdf_path):
                    generated_files = [f for f in os.listdir(temp_dir) if f.lower().endswith('.pdf')]
                    if generated_files:
                        output_pdf_path = os.path.join(temp_dir, generated_files[0])
                        _stdout.write(_style.WARNING(f"PDF found at alternative path: {output_pdf_path}"))
                    else:
                        raise ConversionError(
                            f"LibreOffice did not generate a PDF file in {temp_dir}. Check permissions or LibreOffice output behavior.")

            except FileNotFoundError:
                raise ConversionError(
                    f"Error: '{SOFFICE_EXEC}' command not found. Please ensure LibreOffice Portable path is correct or installed.")
            except subprocess.TimeoutExpired:
                raise ConversionError(
                    f"Error: LibreOffice PDF conversion timed out after 600 seconds. Increase timeout if presentation is very large.")
            except subprocess.CalledProcessError as e:
                raise ConversionError(
                    f"Error during LibreOffice PDF conversion:\nStderr: {e.stderr}\nStdout: {e.stdout}\nEnsure LibreOffice can open the PPTX file.")
            except Exception as e:
                raise ConversionError(f"An unexpected error occurred during LibreOffice PDF processing: {e}")

            _stdout.write("Converting PDF pages to PNG images using PyMuPDF...")
            generated_pngs = []
            try:
                doc = fitz.open(output_pdf_path)
                for i, page in enumerate(doc):
                    output_width = int(resolution.split('x')[0])
                    zoom_factor = output_width / page.rect.width

                    pix = page.get_pixmap(matrix=fitz.Matrix(zoom_factor, zoom_factor))
                    output_image_path = os.path.join(temp_dir, f"slide_{i:03d}.png")
                    pix.save(output_image_path)
                    generated_pngs.append(output_image_path)
                doc.close()
                _stdout.write(_style.SUCCESS(
                    f"PDF conversion to {len(generated_pngs)} PNG images complete using PyMuPDF."))

            except FileNotFoundError:
                raise ConversionError(f"Error: PDF file not found at '{output_pdf_path}' for PyMuPDF processing.")
            except Exception as e:
                raise ConversionError(f"Error during PyMuPDF PDF to PNG conversion: {e}\n"
                                   f"Ensure PyMuPDF (fitz) is installed: `pip install pymupdf`.")


            if not generated_pngs:
                raise ConversionError(f"Error: PyMuPDF did not generate any PNG images from the PDF in '{temp_dir}'.")
            _stdout.write(
                f"Found {len(generated_pngs)} PNG images (e.g., {os.path.basename(generated_pngs[0])}) for video creation.")
            _stdout.write(
                f"Stitching images into video using FFmpeg (each slide for {slide_duration_seconds}s)...")
            input_video_image_pattern = os.path.join(temp_dir, "slide_%03d.png")
            input_framerate = 1 / slide_duration_seconds

            res_width, res_height = resolution.split('x')

            ffmpeg_video_command = [
                FFMPEG_EXEC,
                "-y",
                "-framerate", str(input_framerate),
                "-i", input_video_image_pattern,
                "-c:v", "libx264",
                "-pix_fmt", "yuv420p",
                "-vf",
                f"scale={resolution}:force_original_aspect_ratio=decrease,pad={res_width}:{res_height}:(ow-iw)/2:(oh-ih)/2",
                "-r", str(frame_rate),
                output_video_path
            ]

            try:
                _stdout.write(f"FFmpeg video creation command: {' '.join(ffmpeg_video_command)}")
                subprocess.run(ffmpeg_video_command, check=True, capture_output=True, text=True, timeout=600)
                _stdout.write(_style.SUCCESS(f"Video created successfully: {output_video_path}"))
            except FileNotFoundError:
                raise ConversionError(
                    f"Error: '{FFMPEG_EXEC}' command not found. Please ensure FFmpeg Portable path is correct or installed.")
            except subprocess.TimeoutExpired:
                raise ConversionError(
                    f"Error: FFmpeg video creation timed out after 600 seconds. Increase timeout if video is very long.")
            except subprocess.CalledProcessError as e:
                raise ConversionError(
                    f"Error during FFmpeg video creation:\nStderr: {e.stderr}\nStdout: {e.stdout}\nEnsure FFmpeg arguments are correct and images are valid.")
            except Exception as e:
                raise ConversionError(f"An unexpected error occurred during FFmpeg video processing: {e}")

            _stdout.write(f"Temporary directory {temp_dir} automatically cleaned up.")

    except ConversionError as e:
        raise e
    except Exception as e:
        raise ConversionError(f"An overall unexpected error occurred during the conversion process: {e}")
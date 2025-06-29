import os
import subprocess
import logging
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration for FFmpeg (MAKE SURE THESE PATHS ARE CORRECT) ---
# FFMPEG_PATH = "ffmpeg" # Use "ffmpeg" if it's in your system's PATH
FFMPEG_PATH = r"C:\Users\Ankit.Anand\Downloads\ffmpeg\ffmpeg\bin\ffmpeg.exe"  # Example for Windows portable

# FFmpeg normalization settings
# Use a common video codec (libx264) and audio codec (aac)
# Adjust crf and preset for quality vs. speed. Lower CRF = higher quality, larger file.
# Higher preset (e.g., ultrafast) = faster encoding, larger file/lower quality.
FFMPEG_NORMALIZATION_COMMAND_TEMPLATE = [
    FFMPEG_PATH, '-i', '{input_path}',
    '-c:v', 'libx264',
    '-preset', 'veryfast',  # Faster encoding, good for normalization. Use 'medium' for better quality.
    '-crf', '28',  # Constant Rate Factor: 0-51 (0 is lossless, ~23 is default, higher means more compression)
    '-c:a', 'aac',
    '-b:a', '128k',  # Audio bitrate
    '-vf', 'scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,format=yuv420p',
    # Ensure all videos have same resolution (1920x1080) and pixel format
    '-y', '{output_path}'
]

# Logging setup for this module
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("master_video_maker.log"),  # Still log to the main log
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)


def get_video_duration(video_path):
    """Gets video duration using ffprobe."""
    try:
        # Construct the ffprobe command
        ffprobe_command = [
            FFMPEG_PATH.replace('ffmpeg', 'ffprobe'),  # Assumes ffprobe is in the same directory as ffmpeg
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            video_path
        ]
        result = subprocess.run(ffprobe_command, capture_output=True, text=True, check=True)
        duration = float(result.stdout.strip())
        return duration
    except FileNotFoundError:
        logger.error(f"FFprobe executable not found. Make sure it's in the same directory as FFmpeg or in PATH.")
        return None
    except subprocess.CalledProcessError as e:
        logger.error(f"FFprobe failed for {video_path}: {e.stderr}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred getting duration for {video_path}: {e}")
        return None


def normalize_video(input_path, output_path):
    """Normalizes video to a consistent format and resolution using FFmpeg."""
    command = [arg.format(input_path=input_path, output_path=output_path) for arg in
               FFMPEG_NORMALIZATION_COMMAND_TEMPLATE]
    try:
        logger.info(f"  Normalizing '{os.path.basename(input_path)}'...")
        result = subprocess.run(command, capture_output=True, text=True, check=False)  # check=False to get stderr
        if result.returncode != 0:
            logger.error(
                f"FFmpeg normalization failed for {os.path.basename(input_path)} with exit code {result.returncode}.")
            logger.error(f"FFmpeg STDOUT:\n{result.stdout}")
            logger.error(f"FFmpeg STDERR:\n{result.stderr}")
            return None
        else:
            logger.info(f"  Normalized to: {os.path.basename(output_path)}")
            return output_path
    except FileNotFoundError:
        logger.error(
            f"FFmpeg executable not found at '{FFMPEG_PATH}'. Please ensure FFmpeg is installed and the path is correct.")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during normalization of {os.path.basename(input_path)}: {e}",
                     exc_info=True)
        return None


def merge_videos_in_folder(input_folder: str, output_filename: str) -> bool:
    """
    Normalizes and merges all video files (including the converted PPTX video) in a given folder.
    The order of merging is alphabetically, with the converted PPTX video first if present.
    """
    logger.info(f"Previous 'unmerged_videos_log.txt' cleared.")  # This log is from Django, keep for now

    video_files = []
    for item in os.listdir(input_folder):
        item_path = os.path.join(input_folder, item)
        if os.path.isfile(item_path) and (item.lower().endswith(('.mp4', '.mov', '.webm', '.avi', '.mkv', '.flv'))):
            video_files.append(item_path)

    if not video_files:
        logger.warning(f"No video files found in '{input_folder}' to merge.")
        return False

    # Sort videos to ensure consistent merge order.
    # Put the PPTX-converted video first if it exists.
    # Assuming the converted PPTX video will be named like 'original_pptx_name.mp4'
    pptx_video_path = None
    for video in video_files:
        if "station road.mp4" in os.path.basename(video).lower() and "chandausi" in os.path.basename(
                video).lower():  # More specific check
            pptx_video_path = video
            break

    if pptx_video_path and pptx_video_path in video_files:
        video_files.remove(pptx_video_path)
        video_files.sort()  # Sort remaining videos
        video_files.insert(0, pptx_video_path)  # Insert PPTX video at the beginning
    else:
        video_files.sort()  # If no PPTX or not found, just sort all existing videos

    logger.info(f"Found {len(video_files)} video files to merge:")
    for v_file in video_files:
        logger.info(f"- {os.path.basename(v_file)}")

    # Create a temporary directory for normalized videos
    temp_normalized_dir = os.path.join(input_folder, "temp_normalized_videos")
    os.makedirs(temp_normalized_dir, exist_ok=True)
    logger.info(f"Created temporary normalization folder: {temp_normalized_dir}")

    # Normalize videos in parallel
    normalized_paths = []
    max_workers = os.cpu_count() or 4  # Use number of CPU cores as workers
    logger.info(f"Starting video normalization process (parallelized)...")
    logger.info(
        f"Using FFmpeg Preset: '{FFMPEG_NORMALIZATION_COMMAND_TEMPLATE[4]}' and CRF: {FFMPEG_NORMALIZATION_COMMAND_TEMPLATE[6]} for faster encoding.")
    logger.info(f"Using {max_workers} worker processes for normalization.")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(normalize_video, video,
                                   os.path.join(temp_normalized_dir, f"normalized_{os.path.basename(video)}")): video
                   for video in video_files}
        for future in as_completed(futures):
            normalized_video_path = future.result()
            if normalized_video_path:
                normalized_paths.append(normalized_video_path)

    if len(normalized_paths) != len(video_files):
        logger.error("Not all videos were successfully normalized. Cannot proceed with merging.")
        shutil.rmtree(temp_normalized_dir)  # Clean up partial normalizations
        return False

    # Ensure the order of normalized videos matches the original intended order
    # (PPTX first, then others sorted)
    final_merge_order_paths = []
    # Re-create the list in the correct order using the normalized paths
    for original_video_path in video_files:
        normalized_name = f"normalized_{os.path.basename(original_video_path)}"
        found_normalized = False
        for np in normalized_paths:
            if os.path.basename(np) == normalized_name:
                final_merge_order_paths.append(np)
                found_normalized = True
                break
        if not found_normalized:
            logger.error(
                f"Could not find normalized version of {os.path.basename(original_video_path)}. This should not happen if previous check passed.")
            shutil.rmtree(temp_normalized_dir)
            return False

    # Generate a timestamp file (optional but good for tracking merge operations)
    timestamp_filename = os.path.join(os.path.dirname(output_filename),
                                      f"{os.path.splitext(os.path.basename(output_filename))[0]}_timestamps.txt")

    logger.info(f"Creating merged video timestamp file: {timestamp_filename}")
    try:
        with open(timestamp_filename, 'w') as f:
            total_duration = 0
            for i, video_path in enumerate(final_merge_order_paths):
                duration = get_video_duration(video_path)
                if duration is not None:
                    f.write(
                        f"File {i + 1}: {os.path.basename(video_path)}, Start: {total_duration:.2f}s, Duration: {duration:.2f}s\n")
                    total_duration += duration
                else:
                    f.write(
                        f"File {i + 1}: {os.path.basename(video_path)}, Duration: UNKNOWN (Failed to get duration)\n")
        logger.info("-" * 45)
        logger.info(f"Timestamp file created successfully: {timestamp_filename}")
        logger.info("-" * 45)
    except Exception as e:
        logger.warning(f"Failed to create timestamp file: {e}")

    # Create a temporary concat list file for FFmpeg
    concat_list_path = os.path.join(input_folder, "ffmpeg_concat_list.txt")
    try:
        with open(concat_list_path, "w") as f:
            for video_path in final_merge_order_paths:
                # CRITICAL FIX: Ensure the path written is the correct, absolute path,
                # or correctly relative to where FFmpeg is run IF FFmpeg is run with cwd=input_folder
                # For robustness, using absolute paths in the concat list is best.
                f.write(f"file '{video_path.replace(os.sep, '/')}'\n")  # Use forward slashes for FFmpeg compatibility
        logger.info(f"Created FFmpeg concat list file: {concat_list_path}")
    except Exception as e:
        logger.error(f"Failed to create FFmpeg concat list file: {e}")
        shutil.rmtree(temp_normalized_dir)
        return False

    # Construct FFmpeg merge command using the concat demuxer
    # Use -safe 0 for potentially unsafe characters in file paths (like spaces)
    # Use -c copy for speed IF all streams are compatible (same codec, resolution, etc.)
    # Since we normalize, -c copy should be fine.
    ffmpeg_command = [
        FFMPEG_PATH,
        '-f', 'concat',
        '-safe', '0',  # Allows absolute paths and paths with spaces
        '-i', concat_list_path,
        '-c', 'copy',
        '-y',  # Overwrite output file if it exists
        output_filename
    ]

    logger.info(f"Merging and saving final video to: {output_filename}")
    logger.info(f"Executing FFmpeg command: {' '.join(ffmpeg_command)}")

    try:
        # Use subprocess.run to execute the command and capture output
        result = subprocess.run(ffmpeg_command, capture_output=True, text=True,
                                check=False)  # check=False to inspect return code

        if result.returncode != 0:
            logger.error(
                f"An error occurred during final video merging with FFmpeg: Command '{' '.join(ffmpeg_command)}' returned non-zero exit status {result.returncode}.")
            logger.error(f"FFmpeg stdout:\n{result.stdout}")
            logger.error(f"FFmpeg stderr:\n{result.stderr}")
            # Log the troubleshooting tips explicitly
            logger.error("\nTroubleshooting tips for final merge:")
            logger.error(
                "1. Ensure FFmpeg is installed and its 'bin' directory is in your system's PATH, or FFMPEG_PATH is correct.")
            logger.error("2. Check the FFmpeg stderr output above for specific error messages from FFmpeg itself.")
            logger.error(
                "3. Verify that the normalized temporary video files are not corrupted (manually try playing them).")
            merge_success = False
        else:
            logger.info(f"Successfully merged video to: {output_filename}")
            merge_success = True

    except FileNotFoundError:
        logger.error(
            f"FFmpeg executable not found at '{FFMPEG_PATH}'. Please ensure FFmpeg is installed and the path is correct.")
        merge_success = False
    except Exception as e:
        logger.error(f"An unexpected error occurred during final FFmpeg execution: {e}", exc_info=True)
        merge_success = False
    finally:
        # Clean up temporary files
        if os.path.exists(concat_list_path):
            os.remove(concat_list_path)
            logger.info(f"Cleaned up temporary concat list file: {concat_list_path}")
        if os.path.exists(temp_normalized_dir):
            shutil.rmtree(temp_normalized_dir)
            logger.info(f"Cleaned up temporary folder: {temp_normalized_dir}")

    return merge_success


# Dummy Style and Stdout for helper functions if running outside Django context
class _DummyStyle:
    def SUCCESS(self, msg): return msg

    def WARNING(self, msg): return msg

    def ERROR(self, msg): return msg


_style = _DummyStyle()


class _DummyStdout:
    def write(self, msg, ending='\n'):
        logger.info(msg.strip())


# Example usage (for testing this module independently if needed)
if __name__ == "__main__":
    # Create a dummy folder with some video files for testing
    test_folder = "temp_test_videos_for_merge"
    os.makedirs(test_folder, exist_ok=True)

    # Create dummy video files (replace with actual small test videos if possible)
    # These need to be valid video files for FFmpeg to process them.
    # You might want to copy some small MP4s or MOVs here for a real test.
    # For a minimal test, you could create empty files, but FFmpeg would error.
    # Example:
    # with open(os.path.join(test_folder, "test_video1.mp4"), "w") as f: f.write("dummy")
    # with open(os.path.join(test_folder, "test_video2.mov"), "w") as f: f.write("dummy")

    # Simulate the PPTX converted video
    # with open(os.path.join(test_folder, "chandausi_station road.mp4"), "w") as f: f.write("dummy")

    # In a real test, ensure you have actual video files in test_folder
    # For instance, download a few small sample videos and place them here.

    logger.info(f"Place actual video files in '{test_folder}' to test merging.")
    input("Press Enter to continue after placing videos...")

    output_test_file = "test_merged_output.mp4"
    if merge_videos_in_folder(test_folder, output_test_file):
        logger.info(f"Test merge successful! Output: {output_test_file}")
    else:
        logger.error("Test merge failed.")

    # Clean up test folder
    # if os.path.exists(test_folder):
    #     shutil.rmtree(test_folder)
    #     logger.info(f"Cleaned up test folder: {test_folder}")
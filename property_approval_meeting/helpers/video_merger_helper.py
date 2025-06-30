import os
import subprocess
import logging
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

FFMPEG_PATH = r"C:\Users\Ankit.Anand\Downloads\ffmpeg\ffmpeg\bin\ffmpeg.exe"
FFMPEG_NORMALIZATION_COMMAND_TEMPLATE = [
    FFMPEG_PATH, '-i', '{input_path}',
    '-c:v', 'libx264',
    '-preset', 'veryfast',
    '-crf', '28',
    '-c:a', 'aac',
    '-b:a', '128k',
    '-vf', 'scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,format=yuv420p',
    '-y', '{output_path}'
]

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("master_video_maker.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)


def get_video_duration(video_path):
    try:
        ffprobe_command = [
            FFMPEG_PATH.replace('ffmpeg', 'ffprobe'),
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
    command = [arg.format(input_path=input_path, output_path=output_path) for arg in
               FFMPEG_NORMALIZATION_COMMAND_TEMPLATE]
    try:
        logger.info(f"  Normalizing '{os.path.basename(input_path)}'...")
        result = subprocess.run(command, capture_output=True, text=True, check=False)
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
    logger.info(f"Previous 'unmerged_videos_log.txt' cleared.")
    video_files = []
    for item in os.listdir(input_folder):
        item_path = os.path.join(input_folder, item)
        if os.path.isfile(item_path) and (item.lower().endswith(('.mp4', '.mov', '.webm', '.avi', '.mkv', '.flv'))):
            video_files.append(item_path)

    if not video_files:
        logger.warning(f"No video files found in '{input_folder}' to merge.")
        return False

    pptx_video_path = None
    for video in video_files:
        if "station road.mp4" in os.path.basename(video).lower() and "chandausi" in os.path.basename(
                video).lower():
            pptx_video_path = video
            break

    if pptx_video_path and pptx_video_path in video_files:
        video_files.remove(pptx_video_path)
        video_files.sort()
        video_files.insert(0, pptx_video_path)
    else:
        video_files.sort()

    logger.info(f"Found {len(video_files)} video files to merge:")
    for v_file in video_files:
        logger.info(f"- {os.path.basename(v_file)}")

    temp_normalized_dir = os.path.join(input_folder, "temp_normalized_videos")
    os.makedirs(temp_normalized_dir, exist_ok=True)
    logger.info(f"Created temporary normalization folder: {temp_normalized_dir}")
    normalized_paths = []
    max_workers = os.cpu_count() or 4
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
        shutil.rmtree(temp_normalized_dir)
        return False

    final_merge_order_paths = []
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

    concat_list_path = os.path.join(input_folder, "ffmpeg_concat_list.txt")
    try:
        with open(concat_list_path, "w") as f:
            for video_path in final_merge_order_paths:
                f.write(f"file '{video_path.replace(os.sep, '/')}'\n")
        logger.info(f"Created FFmpeg concat list file: {concat_list_path}")
    except Exception as e:
        logger.error(f"Failed to create FFmpeg concat list file: {e}")
        shutil.rmtree(temp_normalized_dir)
        return False

    ffmpeg_command = [
        FFMPEG_PATH,
        '-f', 'concat',
        '-safe', '0',
        '-i', concat_list_path,
        '-c', 'copy',
        '-y',
        output_filename
    ]

    logger.info(f"Merging and saving final video to: {output_filename}")
    logger.info(f"Executing FFmpeg command: {' '.join(ffmpeg_command)}")

    try:
        result = subprocess.run(ffmpeg_command, capture_output=True, text=True,
                                check=False)

        if result.returncode != 0:
            logger.error(
                f"An error occurred during final video merging with FFmpeg: Command '{' '.join(ffmpeg_command)}' returned non-zero exit status {result.returncode}.")
            logger.error(f"FFmpeg stdout:\n{result.stdout}")
            logger.error(f"FFmpeg stderr:\n{result.stderr}")
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
        if os.path.exists(concat_list_path):
            os.remove(concat_list_path)
            logger.info(f"Cleaned up temporary concat list file: {concat_list_path}")
        if os.path.exists(temp_normalized_dir):
            shutil.rmtree(temp_normalized_dir)
            logger.info(f"Cleaned up temporary folder: {temp_normalized_dir}")

    return merge_success


class _DummyStyle:
    def SUCCESS(self, msg): return msg

    def WARNING(self, msg): return msg

    def ERROR(self, msg): return msg


_style = _DummyStyle()


class _DummyStdout:
    def write(self, msg, ending='\n'):
        logger.info(msg.strip())
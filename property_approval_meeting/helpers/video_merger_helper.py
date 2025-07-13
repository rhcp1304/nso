import os
import subprocess
import logging
import shutil

FFMPEG_PATH = r"C:\Users\Ankit.Anand\Downloads\ffmpeg\ffmpeg\bin\ffmpeg.exe"
FFPROBE_PATH = r"C:\Users\Ankit.Anand\Downloads\ffmpeg\ffmpeg\bin\ffprobe.exe"

FFMPEG_MERGE_COMMAND_TEMPLATE = [
    FFMPEG_PATH,
    '-f', 'concat',
    '-safe', '0',
    '-i', '{concat_list_path}',
    '-c', 'copy',
    '-y',
    '{output_path}'
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
            FFPROBE_PATH,
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            video_path
        ]
        result = subprocess.run(ffprobe_command, capture_output=True, text=True, check=True)
        duration = float(result.stdout.strip())
        return duration
    except FileNotFoundError:
        logger.error(f"FFprobe executable not found at '{FFPROBE_PATH}'. Please ensure the path is correct.")
        return None
    except subprocess.CalledProcessError as e:
        logger.error(f"FFprobe failed for {video_path}: {e.stderr}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred getting duration for {video_path}: {e}")
        return None


def merge_videos_in_folder(input_folder: str, output_filename: str) -> bool:
    logger.info(f"Starting video merge process for folder: {input_folder}")
    logger.info(f"Output video will be named: {os.path.basename(output_filename)}")

    video_files = []
    for item in os.listdir(input_folder):
        item_path = os.path.join(input_folder, item)
        if os.path.isfile(item_path) and (item.lower().endswith(('.mp4', '.mov', '.webm', '.avi', '.mkv', '.flv'))):
            video_files.append(item_path)

    if not video_files:
        logger.warning(f"No video files found in '{input_folder}' to merge.")
        return False

    if len(video_files) == 1:
        source_video = video_files[0]
        # Check if the single video is already the desired output file
        if os.path.normpath(source_video) == os.path.normpath(output_filename):
            logger.info(f"Input and output are the same file. Skipping merge.")
            return True
        else:
            logger.warning(f"Only one video found. Copying it to the output location.")
            try:
                shutil.copyfile(source_video, output_filename)
                logger.info(f"Successfully copied single video to: {output_filename}")
                return True
            except Exception as e:
                logger.error(f"Failed to copy single video {source_video} to {output_filename}: {e}")
                return False

    pptx_video_path = None

    if pptx_video_path and pptx_video_path in video_files:
        video_files.remove(pptx_video_path)
        video_files.sort()
        video_files.insert(0, pptx_video_path)
    else:
        video_files.sort()

    logger.info(f"Found {len(video_files)} video files to merge:")
    for v_file in video_files:
        logger.info(f"- {os.path.basename(v_file)}")

    timestamp_filename = os.path.join(os.path.dirname(output_filename),
                                      f"{os.path.splitext(os.path.basename(output_filename))[0]}_timestamps.txt")
    logger.info(f"Creating merged video timestamp file: {timestamp_filename}")
    try:
        with open(timestamp_filename, 'w') as f:
            total_duration = 0
            for i, video_path in enumerate(video_files):
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
            for video_path in video_files:
                f.write(f"file '{video_path.replace(os.sep, '/')}'\n")
        logger.info(f"Created FFmpeg concat list file: {concat_list_path}")
    except Exception as e:
        logger.error(f"Failed to create FFmpeg concat list file: {e}")
        return False

    ffmpeg_command = [arg.format(concat_list_path=concat_list_path, output_path=output_filename) for arg in
                      FFMPEG_MERGE_COMMAND_TEMPLATE]

    logger.info(f"Merging and saving final video to: {output_filename}")
    logger.info(f"Executing FFmpeg command: {' '.join(ffmpeg_command)}")

    merge_success = False
    try:
        result = subprocess.run(ffmpeg_command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            logger.error(
                f"An error occurred during final video merging with FFmpeg: Command returned non-zero exit status {result.returncode}.")
            logger.error(f"FFmpeg stdout:\n{result.stdout}")
            logger.error(f"FFmpeg stderr:\n{result.stderr}")
        else:
            logger.info(f"Successfully merged video to: {output_filename}")
            merge_success = True
    except FileNotFoundError:
        logger.error(f"FFmpeg executable not found at '{FFMPEG_PATH}'. Please ensure the path is correct.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during final FFmpeg execution: {e}", exc_info=True)
    finally:
        if os.path.exists(concat_list_path):
            os.remove(concat_list_path)
            logger.info(f"Cleaned up temporary concat list file: {concat_list_path}")

    return merge_success
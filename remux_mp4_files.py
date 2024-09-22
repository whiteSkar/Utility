import os
import subprocess
from pathlib import Path
import shutil

# Thank you gpt mostly.
def remux_files(directory):
    # Get a list of all MP4 files in the directory and subdirectories
    mp4_files = list(Path(directory).rglob("*.mp4"))
    total_files = len(mp4_files)
    processed_files = 0
    directory_path = Path(directory)

    if total_files == 0:
        print(f"No MP4 files found in the directory {directory}.")
        return

    mkv_directories = set()

    # Process each MP4 file
    for mp4_file in mp4_files:
        processed_files += 1

        # Get the parent directory of the mp4 file
        parent_dir = mp4_file.parent

        # Create the 'mkv' and 'mp4' folders inside the parent directory
        mkv_dir = parent_dir / "mkv"
        mp4_dir = parent_dir / "remuxed"
        mkv_dir.mkdir(exist_ok=True)
        mp4_dir.mkdir(exist_ok=True)
        mkv_directories.add(mkv_dir)

        # Define the MKV file path and MP4 file path
        mkv_file = mkv_dir / (mp4_file.stem + ".mkv")
        final_mp4_file = mp4_dir / (mp4_file.stem + ".mp4")

        # Print the status
        print(f"[{processed_files}/{total_files}] Remuxing '{mp4_file.relative_to(directory_path)}'")

        files_failed = []
        mkv_created = False

        # Remux MP4 to MKV
        try:
            subprocess.run([
                "ffmpeg", "-i", str(mp4_file), "-c", "copy", str(mkv_file)
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            mkv_created = True
        except subprocess.CalledProcessError as e:
            print(f"Error during remuxing to MKV: {e}")
            files_failed.append(mp4_file)
            continue

        # Print progress
        # print(f"Successfully remuxed '{mp4_file.name}' to '{mkv_file.name}'.")

        if not mkv_created:
            print("Skipping remuxing it back to mp4 since remuxing to mkv failed.")
            continue

        # Remux MKV back to MP4
        # print(f"Remuxing '{mkv_file.name}' to '{final_mp4_file.name}'...")
        try:
            subprocess.run([
                "ffmpeg", "-i", str(mkv_file), "-c", "copy", str(final_mp4_file)
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error during remuxing back to MP4: {e}")
            files_failed.append(mp4_file)
            continue

        # Print progress
        # print(f"Successfully remuxed '{mkv_file.name}'.")

        # Remove the MKV file after remuxing back to MP4 to prevent hoarding the disk space
        try:
            os.remove(mkv_file)
            # print(f"Removed '{mkv_file.name}' after remuxing to MP4.")
        except OSError as e:
            print(f"Error deleting MKV file '{mkv_file.name}': {e}")

    # Delete the 'mkv' folder after processing all files
    for mkv_dir in mkv_directories:
        try:
            if mkv_dir.exists():
                shutil.rmtree(mkv_dir)
                # print(f"Deleted the 'mkv' folder: {mkv_dir}")
            else:
                print(f"{mkv_dir} does not exist for some reason. Skipping deleting")
        except OSError as e:
            print(f"Error deleting 'mkv' folder: {e}")

    # print("MKV directories deletion completed.")

    if files_failed:
        files_failed_msg = '\n'.join(files_failed)
        print(f"These files have failed in either remuxing to mkv or remuxing back to mp4.\n{files_failed_msg}")

    print(f"Remuxing completed: {processed_files}/{total_files} files processed.")


if __name__ == "__main__":
    # Ask for the directory path from the user
    directory = input("Enter the absolute path to the directory: ").strip()

    # Validate the provided directory path
    if not os.path.isdir(directory):
        print(f"The provided directory '{directory}' does not exist.")
    else:
        # Call the remux function
        remux_files(directory)

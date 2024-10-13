import os


def delete_files_with_string(directories, target_strings, dry_run=True):
    num_files_deleted = 0
    for directory in directories:
        # List all files in the directory
        files = os.listdir(directory)

        # Iterate through the files
        for file_name in files:
            # Check if the target string is present in the file name
            for target_string in target_strings:
                if target_string in file_name:
                    file_path = os.path.join(directory, file_name)

                    try:
                        if not dry_run:
                            # Remove the file
                            os.remove(file_path)
                        print(f"{'[Dry Run] ' if dry_run else ''}Deleted: {file_name}")
                        num_files_deleted += 1
                    except Exception as e:
                        print(f"Error deleting {file_name}: {e}")

    print(f"{num_files_deleted} files deleted")
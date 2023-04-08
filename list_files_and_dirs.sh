#!/bin/bash

# Check if the user provided a directory path
if [ $# -eq 0 ]; then
  echo "Usage: $0 <directory_path>"
  exit 1
fi

# Check if the provided path is a valid directory
if [ ! -d "$1" ]; then
  echo "Error: $1 is not a valid directory"
  exit 1
fi

# Function to process each file and directory found
process_item() {
  item="$1"

  # Get the absolute path
  abs_path="$(realpath "$item")"

  # Get the modification timestamp
  mod_time="$(stat -c %y "$item")"

  # Check if it's a file or directory and process accordingly
  if [ -d "$item" ]; then
    echo "Directory: $abs_path/"
    echo "Modification Timestamp: $mod_time"
    echo ""
  elif [ -f "$item" ]; then
    # Get the file size in bytes
    file_size="$(stat -c %s "$item")"

    echo "File: $abs_path"
    echo "Modification Timestamp: $mod_time"
    echo "File Size: $file_size bytes"
    echo ""
  fi
}

# Iterate through all files and directories recursively
while IFS= read -r -d '' item; do
  process_item "$item"
done < <(find "$1" -type f -o -type d -print0)

exit 0

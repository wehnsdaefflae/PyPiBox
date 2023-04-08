#!/bin/bash

# Check if the user provided the required arguments
if [ $# -lt 2 ]; then
  echo "Usage: $0 <samba_share_path> <username> [<password>]"
  exit 1
fi

# Check if smbclient is installed
if ! command -v smbclient &> /dev/null; then
  echo "Error: smbclient is not installed. Please install it and try again."
  exit 1
fi

samba_share_path="$1"
username="$2"
password="${3:-}"

# If the password is not provided, prompt for it
if [ -z "$password" ]; then
  read -s -p "Password: " password
  echo
fi

# Function to process the output of smbclient
process_output() {
  while IFS= read -r line; do
    # Extract the required information from the line
    item_type="$(echo "$line" | awk '{print $1}')"
    mod_time="$(echo "$line" | awk '{print $2 " " $3}')"
    size="$(echo "$line" | awk '{print $4}')"
    item_name="$(echo "$line" | awk '{for(i=5;i<=NF;++i)printf $i" ";print ""}')"

    # Process the item based on its type
    if [[ "$item_type" == "D" ]]; then
      echo "Directory: $item_name/"
      echo "Modification Timestamp: $mod_time"
      echo ""
    elif [[ "$item_type" == "A" ]]; then
      echo "File: $item_name"
      echo "Modification Timestamp: $mod_time"
      echo "File Size: $size bytes"
      echo ""
    fi
  done
}

# Connect to the Samba share and list the files and directories
output=$(smbclient "$samba_share_path" -U "${username}%${password}" -c "recurse;dir" 2>/dev/null)

# Process the output
echo "$output" | process_output

exit 0

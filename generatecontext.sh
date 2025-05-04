#!/bin/bash

# This script finds all .go files recursively from the current directory
# and prints their path followed by their content.

echo ">>> Searching for .go files and printing contents..."
echo

# Use find -print0 and while read -d $'\0' for safe handling of filenames
# that might contain spaces, newlines, or other special characters.
find . -name '*.go' -print0 | while IFS= read -r -d $'\0' file; do
  echo "##################################################"
  echo "# File: $file"
  echo "##################################################"
  cat "$file"
  echo # Add an empty line for better separation between files
  echo
done

echo ">>> Finished printing .go files."
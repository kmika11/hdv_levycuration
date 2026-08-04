#!/bin/zsh

csv_file="levy_illustrations_metadata.csv"
target_dir="./files"  # Or change to your specific directory
filename_column="filename"

# Extract header and find the index of the 'filename' column
header=$(head -n 1 "$csv_file")
IFS=',' read -A headers <<< "$header"

# Find the index of the filename column
filename_index=-1
for i in {1..${#headers[@]}}; do
    if [[ "${headers[$((i-1))]}" == "$filename_column" ]]; then
        filename_index=$i
        break
    fi
done

if [[ $filename_index -eq -1 ]]; then
    echo "Column '$filename_column' not found in CSV."
    exit 1
fi

# Build a set of filenames from CSV (excluding header)
csv_filenames=()
tail -n +2 "$csv_file" | while IFS=',' read -A line; do
    csv_filenames+=("${line[$((filename_index-1))]}")
done

# Build a set of actual files in the directory
dir_files=("${(f)$(ls -1 "$target_dir")}")

# Convert arrays to associative arrays for fast lookup
typeset -A csv_set dir_set
for f in "${csv_filenames[@]}"; do csv_set["$f"]=1; done
for f in "${dir_files[@]}"; do dir_set["$f"]=1; done

# 1. Files listed in CSV but NOT in directory → output full CSV row
{
  echo "$header"  # write the header
  tail -n +2 "$csv_file" | while IFS=',' read -r line; do
    filename=$(echo "$line" | cut -d',' -f"$filename_index")
    if [[ -z ${dir_set[$filename]} ]]; then
      echo "$line"
    fi
  done
} > missing_in_directory.csv

# 2. Files in directory but NOT listed in CSV → output filename only
{
  echo "filename"
  for f in "${dir_files[@]}"; do
    if [[ -z ${csv_set[$f]} ]]; then
      echo "$f"
    fi
  done
} > missing_in_csv.csv

echo "Done. Output files:"
echo " - missing_in_directory.csv"
echo " - missing_in_csv.csv"

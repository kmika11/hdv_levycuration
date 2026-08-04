# Access files from Dataverse dataset

# Set API variables
export SERVER_URL=https://dataverse.harvard.edu
export PID=doi:10.7910/DVN/XXXXXX #Replace with Dataset DOI using this format. 
export VERSION=:draft
export API_TOKEN=xxxx-xxx-xxx-xxxxxx-xxx #Replace with your API key (found by clicking on your username/account in the upper right hand corner of the website and choosing API Token)

# Output file name - may wish to adjust for each Dataset 
output_csv="file_metadata.csv"

# Write CSV header
echo "filename,persistentId,description" > "$output_csv"

# Fetch and process JSON
curl -s -H "X-Dataverse-key: $API_TOKEN" \
  "$SERVER_URL/api/datasets/:persistentId/versions/$VERSION/files?persistentId=$PID" | \
  jq -r '
    .data[] | [
      .label,
      .dataFile.persistentId,
      (.description | gsub("[\r\n]"; " ") | gsub(","; "‚"))  # sanitize for CSV
    ] | @csv
  ' >> "$output_csv"

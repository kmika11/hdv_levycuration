import dvuploader as dv
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(), override=True)

# import metadata tables
source_path = '/Users/katherinemika/Desktop/curation/leon_levy/'
file_dir = '/Users/katherinemika/Desktop/curation/leon_levy/photos/1997'

missing_images_addedphotos = pd.read_csv(source_path + 'photos/remaining_photos/levy_missing_images_addedphotos.csv', index_col=None, low_memory=False)
missing_images_fixedfiles = pd.read_csv(source_path + 'photos/remaining_photos/levy_missing_images_fixedfilenames.csv', index_col = None)
missing_links_fixedlinks = pd.read_csv(source_path + 'photos/remaining_photos/levy_missing_links_to_upload.csv', index_col = None)

# concatenate tables
long_metadata = pd.concat([missing_images_addedphotos, missing_images_fixedfiles, missing_links_fixedlinks])

# drop rows with duplicate 'filename' values, keeping the first occurrence
metadata = long_metadata.drop_duplicates(subset='filename', keep='first')

# create column for dataset based on year (filename prefix) 
metadata['year'] = metadata['filename'].str.split('_').str[0]
metadata['year'] = metadata['year'].str.extract(r'[a-zA-Z](\d{2})')

metadata.reset_index(drop=True, inplace=True)

# function to convert two-digit year to four-digit year
def convert_year(two_digit_year):
    if pd.isna(two_digit_year):  # check if the value is NaN
        return np.nan
    year = int(two_digit_year)
    if year <= 49:
        return 2000 + year
    else:
        return 1900 + year

# apply the conversion function to the 'year' column
metadata['year'] = metadata['year'].apply(convert_year)
metadata['year'] = metadata['year'].fillna(0).astype(int)

#slice out 1997
metadata_97 = metadata[metadata['year'] == 1997]
metadata_97 = metadata_97.reset_index(drop=True)

# Construct the list of File objects from the DataFrame
files = [
    dv.File(
        filepath=os.path.join(file_dir, row['filename']),
        #description=row['ochre_metadata']
    )
    for _, row in metadata_97.iterrows()
]

# Credentials come from .env (see .env.example). DATAVERSE_TARGET selects
# which installation: "demo" for testing, "harvard" for production.
_PREFIX = "DEMO_" if os.environ["DATAVERSE_TARGET"] == "demo" else ""
DV_URL = os.environ[f"{_PREFIX}DATAVERSE_URL"]
API_TOKEN = os.environ[f"{_PREFIX}DATAVERSE_API_TOKEN"]
PID = "doi:10.7910/DVN/RS8XWD"

print(f"Uploading {len(files)} files to {PID} on {DV_URL}")

dvuploader = dv.DVUploader(files=files)
dvuploader.upload(
    api_token=API_TOKEN,
    dataverse_url=DV_URL,
    persistent_id=PID,
    n_parallel_uploads=2,
)
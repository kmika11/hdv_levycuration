# Leon Levy Curation Script

This script is the latest version of the batch process workflow that deposits data files from the Leon Levy Expedition to Ashkelon into Harvard Dataverse. The full collection is available here: <add link>. 

This script relies on functions stored in the utils.py file. 

Basic document outline:
1. Establish metadata file for batch. Eg. for Photos subcollection, datasets are split into year; for Illustrations, datasets are split into arbitrary buckets of 500 files.
2. Deposit datasets using EasyDataverse with built in python DVUploader.
3. Manually QA Datasets on Harvard Dataverse
4. Finialize & publish datasets back in EasyDataverse.

# 0. Setup

Bypass SSL verification because of HUIT rate limiting issues...

This patch only works if you're connected to the Harvard VPN. Otherwise you're subject to the HUIT rate limiter that they have deployed to manage bot traffic. 


```python
# === PATCH CELL: Run this FIRST ===

import httpx

# Save originals in case you want to revert
_OriginalClient = httpx.Client
_OriginalAsyncClient = httpx.AsyncClient
_OriginalGet = httpx.get

# Patch Client (sync)
class InsecureClient(httpx.Client):
    def __init__(self, *args, **kwargs):
        kwargs["verify"] = False
        super().__init__(*args, **kwargs)

# Patch AsyncClient
class InsecureAsyncClient(httpx.AsyncClient):
    def __init__(self, *args, **kwargs):
        kwargs["verify"] = False
        super().__init__(*args, **kwargs)

# Patch httpx.get
def insecure_get(*args, **kwargs):
    kwargs["verify"] = False
    return _OriginalGet(*args, **kwargs)

# Apply all patches
httpx.Client = InsecureClient
httpx.AsyncClient = InsecureAsyncClient
httpx.get = insecure_get


# Patch requests (used by pyDataverse)
import requests

_original_requests_request = requests.Session.request

def insecure_requests_request(self, method, url, **kwargs):
    kwargs["verify"] = False
    return _original_requests_request(self, method, url, **kwargs)

requests.Session.request = insecure_requests_request

```


```python
# === Raise rate limits for large deposits ===

import resource

# Check current limits
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
print(f"Current limits — soft: {soft}, hard: {hard}")

# Raise the soft limit to the hard limit ceiling
resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))

soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
print(f"Updated limits — soft: {soft}, hard: {hard}")
```

    Current limits — soft: 4096, hard: 9223372036854775807
    Updated limits — soft: 9223372036854775807, hard: 9223372036854775807


#### Global Variables


```python
# set curation source path
source_path = '/Users/katherinemika/Desktop/curation/leon_levy/summer2026'

# path to inventory file
g_dataverse_inventory_file = '/Users/katherinemika/Desktop/curation/leon_levy/summer2026/metadata_fieldbooks_more.csv'

# dataset names
g_dataset_names = []

# dataset inventories (keyed on dataset name)
g_dataset_inventories = {}

# dataset metadata (keyed on dataset name)
g_dataset_metadata = {}

# dataverse installation + API key -- loaded from .env, never hard-coded.
# See .env.example. DATAVERSE_TARGET picks the installation: "demo" or "harvard".
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(), override=True)

_prefix = "DEMO_" if os.environ["DATAVERSE_TARGET"] == "demo" else ""
dv_installation_url = os.environ[f"{_prefix}DATAVERSE_URL"]
dv_api_key = os.environ[f"{_prefix}DATAVERSE_API_TOKEN"]

# Previously pointed at the staging host below, which requires the Harvard VPN.
# To use it again, set DATAVERSE_URL in .env:
#   https://dvn-cloud-app-2.lib.harvard.edu
print(f"Dataverse target: {os.environ['DATAVERSE_TARGET']} -> {dv_installation_url}")

# dataverse collection name
dv_collection = 'ashkelonexcavations'

# dataverse inventory dataframe
dv_inventory_df = None

# full path to location of datafiles (e.g., ../data/trade_statistics)
#datafiles_path = '/Users/katherinemika/Library/CloudStorage/OneDrive-HarvardUniversity/LeonLevy' #separated into subcollection and years
datafiles_path = '/Users/katherinemika/Desktop/curation/leon_levy/summer2026/fieldbooksandMC'
# dataverse dataset information (keyed on dataset name)
dv_dataset_info = {}

# datafile metadata (dataframe of datafile metadata, keyed on dataset name)
g_datafile_metadata = {}
```

#### Hard Coded Metadata Fields
These fields can be set at the script level, because they will apply to all datasets in this collection. 


```python
# dataset authors
author1_name = 'Master, Daniel M.'
author1_affil = "Wheaton College"
author2_name = 'Stager, Lawrence E.'
author2_affil = 'Harvard University'

# dataset contact information
dataset_contact = 'Master, Daniel M.'
dataset_contact_email = 'daniel.master@wheaton.edu'

# dataset subject
subj = 'Arts and Humanities'

# dataset keyword
kws = 'Archaeology'

# language 
lang = "English"

# Funding info
funding_agency = 'The Leon Levy Foundation'

# Depositor 
depositor = "Mika, Katherine"

# Geospatial
geo_coverage = "Ashkelon"
geo_unit = "City"
```


```python
# import libraries
import json
import requests
import pprint as pprint
import pandas as pd
import rich
import os
import sys
import numpy as np
from pyDataverse.models import Dataset
import requests
import dvuploader as dv
import unicodedata
import time
if source_path not in sys.path:
    sys.path.append(source_path)
```


```python
# Local functions
def convert_year(two_digit_year):
    if pd.isna(two_digit_year):  # check if the value is NaN
        return np.nan
    year = int(two_digit_year)
    if year <= 49:
        return 2000 + year
    else:
        return 1900 + year

def create_dataset_metadata(author_name1,
                            author_name2,
                            affiliation1,
                            affiliation2,
                            contactName,
                            contactEmail,
                            subject,
                            keywords,
                            lang,
                            funding,
                            depositor,
                            geo_coverage,
                            geo_unit,
                            title, 
                            inventory):
    """
    Create a dictionary of dataset metadata

    Parameters
    ----------
    author : str
        Dataset author names, assigned in Step 0 of curation script.
    affiliation : str
        Dataset athor affiliations, assigned in Step 0 of curation script.
    contact : str
        Dataset contact name (may be same as author), assigned in Step 0 of curation script.
    email : str
        Dataset contact email address, assigned in Step 0 of curation script.
    subject : str
        Dataset subjects, separated by semicolons, assigned in Step 0 of curation script.
    keywords : str
        Dataset keywords, separated by semicolons, assigned in Step 0 of curation script.
    lang : str
        Dataset language, assigned in Step 0 of curation script.
    funding : str
        Dataset funder information, assigned in Step 0 of curation script.
    depositer : str
        Person who deposited dataset into Dataverse
    geo_coverage : str
        Location, in natural language, of where data was collected or refers to
    geo_unit : str
        Level of geographic aggregation for dataset
    title : str
        Name of dataset (e.g., Tonnage: 1)
    inventory : DataFrame
        DataFrame containing file metadata

    Return
    ------
    dict
    """
    # Validate parameters
    if ((not author_name1) or
        (not affiliation1) or
        (not contactName) or
        (not contactEmail) or
        (not title) or 
        (inventory.empty)):
        print('Error: One or more invalid parameter values')
        return {}
    
    # Check the inventory for required fields
    required_fields = ['dataset_title', 'dataset_description']
    for field in required_fields:
        if field not in inventory.columns:
            print(f'Error: Missing required field {field} in inventory')
            return {}

    # Get the index of the first file in the inventory
    index = inventory.index.values[0]
    
    # Collect metadata variables
    dataset_title = inventory.at[index, 'dataset_title']
    description = inventory.at[index, 'dataset_description']
    
    # Process keywords
    keyword_str = keywords
    keywords = keyword_str.split(';') if isinstance(keyword_str, str) else []
    kws = [{'keywordValue': kw} for kw in keywords]

    #make sure language is a list
    language_value = lang
    language_value = [language_value]    

    #prod_date
    #production_date = inventory.at[index, 'prod_date']

    # Build the dataset metadata dictionary
    dataset_metadata = {
        'title': dataset_title,
        'author': [{'authorName': author_name1, 'authorAffiliation': affiliation1},
                   {'authorName': author_name2, 'authorAffiliation': affiliation2}],
        'description': [{'dsDescriptionValue': description}],
        'contact': [{'datasetContactName': contactName, 'datasetContactAffiliation': affiliation1, 'datasetContactEmail': contactEmail}],
        'subject': [subject],
        'license': 'CC BY-NC-ND 4.0',
        'keywords': kws,
        'funding': funding,
        #'productionDate': production_date,
        'depositor': depositor,
        'language': language_value,
        'geographicCoverage': geo_coverage,
        'geographicUnit': geo_unit
    }

    return dataset_metadata


def create_datafile_metadata(inventory_df):
    """
    Create metadata for data files based on a template

    Parameters
    -----------
    inventory_df : DataFrame
        DataFrame containing list of data files to upload

    Return
    -------
    DataFrame
    
    """
    # validate params
    if (inventory_df.empty == True):
        print('Error: One or more invalad parameters')
        return pd.DataFrame

    # check DataFrame for required fields
    if ((not 'filename' in inventory_df.columns) or
        (not 'dataset_title' in inventory_df.columns)):
        print('Error: One or more missing required fields in inventory')
        return pd.DataFrame()

    ## prepare series of values to add to metadata df

    # prepare file names & descriptions for actual file
    all_filenames = []
    all_descriptions = []

    # iterate through inventory and create datafile metadata
    for index, row in inventory_df.iterrows():
        # get inventory variables
        filename = row['filename']
        all_filenames.append(filename)
        dataset = row['dataset_title']
        df_description = row['file_description']
        all_descriptions.append(df_description)


    # build DataFrame
    df = pd.DataFrame({
        'filename': all_filenames,
        'df_description': all_descriptions
    })

    return df


def create_dataset(ds, dataset_metadata):
    """
    Create a dataverse dataset using easyDataverse.
    Note that metadata fields are hardcoded to reflect dataset's requirements. 

    Parameters
    ----------
    ds : initialized easyDataverse Dataset
    dataset_metadata : dict
        Dictionary of dataset metadata values

    Return
    ------
    dict: 
        {status: bool, dataset_id: int, dataset_pid: str}

    """

    # validate parameters
    if ((not ds) or
        (not dataset_metadata)):
        return {
            'status':False, 
            'dataset_id':-1, 
            'dataset_pid':''
        }


    # populate the dataset model with metadata values
    ds.citation.title = dataset_metadata.get('title')

    for authors in dataset_metadata.get('author'):
        ds.citation.add_author(name = authors['authorName'],
                              affiliation = authors['authorAffiliation'])

    for desc in dataset_metadata.get('description'):
        ds.citation.add_ds_description(value=desc['dsDescriptionValue'])
    
    for contact in dataset_metadata.get('contact'):
        ds.citation.add_dataset_contact(name = contact['datasetContactName'],
                                        email = contact['datasetContactEmail'])

    ds.citation.subject = dataset_metadata.get('subject')

    for keyword in dataset_metadata.get('keywords'):
        ds.citation.add_keyword(value = keyword['keywordValue'])

    ds.citation.depositor = dataset_metadata.get('depositor')
    ds.citation.language = dataset_metadata.get('language')
    ds.citation.add_grant_number(agency = dataset_metadata.get('funding'))
    
    #production_date = str(dataset_metadata.get('productionDate'))
    #ds.citation.production_date = production_date


    ds.geospatial.unit.append(dataset_metadata.get('geographicUnit'))
    #ds.geospatial.unit = dataset_metadata.get('geographicUnit')
    ds.geospatial.add_coverage(other_geographic_coverage = dataset_metadata.get('geographicCoverage'))

    return ds



def add_files_to_dataset(dataset, file_inventory, datafiles_path):
    """
    Add files to a dataset in easyDataverse

    Parameters
    ----------
    
    dataset : easyDataverse initialized dataset
    file_inventory : DataFrame
        DataFrame of file metadata, keyed on dataset
    datafiles_path : global variable
        Path to file directory set in global var cell
    
    Return
    ------
    dict: 
        {status: bool, dataset_id: int, dataset_pid: str}

    """
    # check inventoryDataFrame for required fields
    if ((not 'filename' in file_inventory.columns) or
        (not 'df_description' in file_inventory.columns)):
        print('Error: One or more missing required fields in inventory')
        return pd.DataFrame()
        
    for index, row in file_inventory.iterrows():
        dataset.add_file(
            local_path = os.path.join(datafiles_path, row['filename']),
            description = row['df_description']
        )
    return dataset

```


```python
#More helper functions

import os
import json
import time
import requests
from dvuploader import DVUploader
from pyDataverse.api import NativeApi
from pyDataverse.models import Dataset as PyDataset

# ---------------------------------------------------------------------------
# Checkpoint helpers – persist PIDs to disk so a kernel restart won't lose
# track of already-created datasets.
# ---------------------------------------------------------------------------

def save_pids(pid_file: str, pids: dict) -> None:
    """Write the PID checkpoint file."""
    with open(pid_file, "w") as f:
        json.dump(pids, f, indent=2)
    print(f"  💾 Checkpointed PIDs → {pid_file}")


def load_pids(pid_file: str) -> dict:
    """Read the PID checkpoint file (returns {} if it doesn't exist yet)."""
    if os.path.exists(pid_file):
        with open(pid_file) as f:
            return json.load(f)
    return {}


# ---------------------------------------------------------------------------
# Query Dataverse for files already in the DRAFT version of a dataset.
# Used to filter out already-uploaded files before a retry.
# ---------------------------------------------------------------------------

def get_uploaded_filenames(p_id: str, dataverse_url: str, api_token: str) -> set:
    """
    Return the set of filenames already present in the draft of *p_id*.
    Falls back to an empty set if the request fails so callers can proceed.
    """
    endpoint = (
        f"{dataverse_url.rstrip('/')}/api/datasets/:persistentId"
        f"/versions/:draft/files?persistentId={p_id}"
    )
    headers = {"X-Dataverse-key": api_token}

    try:
        resp = requests.get(endpoint, headers=headers, verify=False, timeout=30)
        resp.raise_for_status()
        return {item["dataFile"]["filename"] for item in resp.json().get("data", [])}
    except Exception as exc:
        print(f"  ⚠️  Could not fetch existing files (will assume none): {exc}")
        return set()


# ---------------------------------------------------------------------------
# Main upload function – replaces dataset.upload()
# ---------------------------------------------------------------------------

def resilient_upload(
    dataset,
    dataverse_name: str,
    n_parallel: int = 1,
    max_retries: int = 5,
    base_delay: int = 60,       # seconds; doubles on each retry
    batch_size: int = 100,      # files per upload batch
) -> str:
    """
    Upload an easyDataverse Dataset with fault-tolerant file handling.

    Differences from dataset.upload():
    ─ dataset.p_id is set immediately after the dataset record is created,
      before any files are transferred.
    ─ Already-uploaded files are detected and skipped before every (re)try.
    ─ Files are uploaded in batches so a 503 only affects one batch.
    ─ Exponential backoff (base_delay × 2^attempt) between retries.

    Parameters
    ----------
    dataset        : easyDataverse Dataset object (with .DATAVERSE_URL / .API_TOKEN)
    dataverse_name : target Dataverse collection alias
    n_parallel     : parallel file streams inside each batch (keep at 1 for safety)
    max_retries    : retry attempts per batch before raising
    base_delay     : initial wait in seconds before the first retry
    batch_size     : number of files per upload call

    Returns
    -------
    str : persistent identifier (PID) of the dataset
    """
    dv_url = str(dataset.DATAVERSE_URL)
    token  = str(dataset.API_TOKEN)
    api    = NativeApi(dv_url, token)

    # ── 1. Create the dataset record (skip if it already has a PID) ──────────
    if dataset.p_id is None:
        print(f"\n📦 Creating dataset: '{dataset.citation.title}'")
        ds_model = PyDataset()
        ds_model.from_json(dataset.dataverse_json())

        resp = api.create_dataset(
            dataverse=dataverse_name,
            metadata=dataset.dataverse_json(),
        )
        resp.raise_for_status()

        # ← store immediately so a later file-upload failure doesn't lose it
        dataset.p_id = resp.json()["data"]["persistentId"]
        print(f"  ✅ Dataset created → {dataset.p_id}")
    else:
        print(f"\n📦 Resuming dataset '{dataset.citation.title}' → {dataset.p_id}")

    p_id = dataset.p_id

    # ── 2. Skip files already present in the draft ───────────────────────────
    already_uploaded = get_uploaded_filenames(p_id, dv_url, token)
    pending = [
        f for f in dataset.files
        if os.path.basename(str(f.filepath)) not in already_uploaded
    ]

    print(f"  📂 {len(already_uploaded)} already uploaded · "
          f"{len(pending)} still to upload out of {len(dataset.files)} total")

    if not pending:
        print("  ✅ Nothing left to upload.")
        return p_id

    # ── 3. Batch upload with exponential-backoff retry ────────────────────────
    batches = [pending[i : i + batch_size] for i in range(0, len(pending), batch_size)]

    for batch_idx, batch in enumerate(batches, start=1):
        print(f"\n  🗂  Batch {batch_idx}/{len(batches)} — {len(batch)} files")

        for attempt in range(1, max_retries + 1):
            try:
                uploader = DVUploader(files=batch)
                uploader.upload(
                    persistent_id=p_id,
                    dataverse_url=dv_url,
                    api_token=token,
                    n_parallel_uploads=n_parallel,
                )
                print(f"  ✅ Batch {batch_idx} complete.")
                break  # ← success; advance to the next batch

            except Exception as exc:
                if attempt == max_retries:
                    print(f"  ❌ Batch {batch_idx} failed after {max_retries} attempts. Re-raising.")
                    raise

                # Re-check what was actually uploaded during the failed attempt
                # so the retry list only contains genuinely missing files.
                uploaded_now = get_uploaded_filenames(p_id, dv_url, token)
                batch = [
                    f for f in batch
                    if os.path.basename(str(f.filepath)) not in uploaded_now
                ]

                if not batch:
                    print(f"  ✅ All files in batch {batch_idx} appear uploaded despite error. Moving on.")
                    break

                delay = base_delay * (2 ** (attempt - 1))
                print(f"  ⚠️  Attempt {attempt} failed ({type(exc).__name__}: {exc})")
                print(f"  ↺  {len(batch)} files remain · retrying in {delay}s …")
                time.sleep(delay)

        # Brief cooldown between batches (not after the last one)
        if batch_idx < len(batches):
            print("  ⏳ Pausing 30s before next batch …")
            time.sleep(30)

    return p_id
```

## 1. Metadata
**Documentation**
Fields are assigned at the file level in a table. Files are then grouped into datasets according to value in dataset_title field. Some fields are assigned universally in Step 0 to all datasets. <p>
Fields:
1. dataset_title: title of dataset
2. author_name: last, first
3. author_affiliation: ??
4. author_identifier_type: ORCID
5. author_identifier: orcid #
6. contact_name: last, first
7. contact_affiliation: ??
8. contact_email: ??
9. description: dataset description, duplicate for all files in dataset
10. subject: dataset subject
11. keyword: dataset keywords, separated by semicolons
12. depositor: Mika, Katherine
13. license: CC0 1.0
14. language: English??
15. funding_agency: Leon Levy foundation info
16. production_date: date of data production (if known)
17. production_location: Israel
18. filename: filename
19. file_ext: file extension (all lowercase - change in table ahead of deposit if necessary)
20. file_description: OCHRE link with href html markup

#### Internal notes about metadata spreasheets: 
- levy_missing_images_addedphotos = table of photos that have not been uploaded because the image was missing. files are located in "additional_photos" folder
- levy_missing_images_fixedfilename = table of photos that have not been uploaded - due to errors in filenames. files are located in existing photos folders (by year)
- levy_missing_links_fixedlinks = table of photos that have not been uploaded due to errors missing ochre links. files are located in existing photos folder (by year)

*To Do:*
1. Join all tables, dropping rows with identical filenames
2. Add columns for all necessary mta


```python
# import metadata tables
#missing_images_addedphotos = pd.read_csv(source_path + 'photos/remaining_photos/levy_missing_images_addedphotos.csv', index_col=None, low_memory=False)
#missing_images_fixedfiles = pd.read_csv(source_path + 'photos/remaining_photos/levy_missing_images_fixedfilenames.csv', index_col = None)
#missing_links_fixedlinks = pd.read_csv(source_path + 'photos/remaining_photos/levy_missing_links_to_upload.csv', index_col = None)
metadata = pd.read_csv(g_dataverse_inventory_file, index_col = None)
#dir_files = pd.read_csv("/Users/katherinemika/Desktop/curation/leon_levy/illustrations/files/files.csv", index_col = None)
```


```python
# concatenate tables when importing multiple metadata files
#long_metadata = pd.concat([missing_images_addedphotos, missing_images_fixedfiles, missing_links_fixedlinks])

# drop rows with duplicate 'filename' values, keeping the first occurrence
#metadata = long_metadata.drop_duplicates(subset='filename', keep='first')
#metadata = metadata.drop_duplicates(subset='filename', keep='first')

#to lowercase all chars in filenames col and dir_files
#metadata['filename'] = metadata['filename'].str.lower()
#dir_files['files'] = dir_files['files'].str.lower()

#find missing values in each df
#filename in metadata not in directory:
#metadata['status'] = metadata['filename'].apply(
#    lambda x: 'missing in dir_files' if x not in dir_files['files'].values else ''
#)

#file in directory not listed in metadata sheet:
#dir_files['status'] = dir_files['files'].apply(
#    lambda x: 'missing in metadata' if x not in metadata['filename'].values else ''
#)

#keep only metadata rows that have matching file in dir_rows: 
#metadata_matched = metadata[metadata['status'] == ''].copy()
#metadata_matched.reset_index(drop=True, inplace=True)

# create column for dataset based on year (filename prefix) 
#metadata['year'] = metadata['filename'].str.split('_').str[0]
#metadata['year'] = metadata['year'].str.extract(r'[a-zA-Z](\d{2})')

#metadata.reset_index(drop=True, inplace=True)

# apply the conversion function to the 'year' column
#metadata['year'] = metadata['year'].apply(convert_year)
#metadata['year'] = metadata['year'].fillna(0).astype(int)

#metadata.to_csv("missing_files.csv", index = False)
#dir_files.to_csv('missing_metadata.csv', index = False)
```


```python
#metadata_matched = metadata_matched.dropna(subset=['filename'])
#metadata_matched.head(-5)
```


```python
# rename columns
#metadata_matched.rename(columns = {'metadata':'file_description', 'dataset':'dataset_title'}, inplace = True)

# add href wrapper to description link
#metadata_matched['file_description'] = "OCHRE Link: <a href=" + metadata_matched['file_description'] + ">" + metadata_matched['file_description'] + "</a>"

# duplicate dataset_title for prod. date
#metadata['prod_date'] = metadata['dataset_title']
#metadata['prod_date'] = "2004"

# edit dataset_title col
#metadata['dataset_title'] = metadata['dataset_title'].astype(str) + ' Photographs'
#metadata['dataset_title'] = "Ashkelon Surface Photos"

#add dataset_desc column
#metadata['dataset_description'] = "Dataset contains photographs from " + metadata['prod_date'].astype(str) + " excavation."
#metadata_matched['dataset_description'] = "Dataset contains renderings of the excavation site."

#metadata_matched
#metadata_matched.to_csv('files_to_add.csv', index = False)
```


```python

```


```python
# split inventory into datasets
# get list of datasets in the full inventory
#change metadata to metadata_matched if using above cells to process metadata tables
g_dataset_titles = list(metadata.dataset_title.unique())

# create inventories of datasets
for title in g_dataset_titles:
    # get inventory of dataset
    g_dataset_inventories[title] = metadata.loc[metadata['dataset_title'] == title]

pprint.pprint(g_dataset_titles)
```

    ['Leon Levy Expedition to Ashkelon Dataset: Fieldbooks',
     'Leon Levy Expedition to Ashkelon Dataset: Material Culture Registry',
     'Leon Levy Expedition to Ashkelon Dataset: Field Reports']



```python
for title in g_dataset_titles:
    # get dataset inventory
    dataset_inventory = g_dataset_inventories[title]
    md = create_dataset_metadata(author1_name,
                                 author2_name,
                                 author1_affil,
                                 author2_affil,
                                 dataset_contact,
                                 dataset_contact_email,
                                 subj,
                                 kws,
                                 lang,
                                 funding_agency,
                                 depositor,
                                 geo_coverage,
                                 geo_unit,
                                 title,
                                 dataset_inventory)
    
    g_dataset_metadata[title] = md

pprint.pprint(g_dataset_metadata)
```

    {'Leon Levy Expedition to Ashkelon Dataset: Field Reports': {'author': [{'authorAffiliation': 'Wheaton '
                                                                                                  'College',
                                                                             'authorName': 'Master, '
                                                                                           'Daniel '
                                                                                           'M.'},
                                                                            {'authorAffiliation': 'Harvard '
                                                                                                  'University',
                                                                             'authorName': 'Stager, '
                                                                                           'Lawrence '
                                                                                           'E.'}],
                                                                 'contact': [{'datasetContactAffiliation': 'Wheaton '
                                                                                                           'College',
                                                                              'datasetContactEmail': 'daniel.master@wheaton.edu',
                                                                              'datasetContactName': 'Master, '
                                                                                                    'Daniel '
                                                                                                    'M.'}],
                                                                 'depositor': 'Mika, '
                                                                              'Katherine',
                                                                 'description': [{'dsDescriptionValue': 'Field '
                                                                                                        'reports '
                                                                                                        'from '
                                                                                                        'excavation: '
                                                                                                        '1985-2016'}],
                                                                 'funding': 'The '
                                                                            'Leon '
                                                                            'Levy '
                                                                            'Foundation',
                                                                 'geographicCoverage': 'Ashkelon',
                                                                 'geographicUnit': 'City',
                                                                 'keywords': [{'keywordValue': 'Archaeology'}],
                                                                 'language': ['English'],
                                                                 'license': 'CC '
                                                                            'BY-NC-ND '
                                                                            '4.0',
                                                                 'subject': ['Arts '
                                                                             'and '
                                                                             'Humanities'],
                                                                 'title': 'Leon '
                                                                          'Levy '
                                                                          'Expedition '
                                                                          'to '
                                                                          'Ashkelon '
                                                                          'Dataset: '
                                                                          'Field '
                                                                          'Reports'},
     'Leon Levy Expedition to Ashkelon Dataset: Fieldbooks': {'author': [{'authorAffiliation': 'Wheaton '
                                                                                               'College',
                                                                          'authorName': 'Master, '
                                                                                        'Daniel '
                                                                                        'M.'},
                                                                         {'authorAffiliation': 'Harvard '
                                                                                               'University',
                                                                          'authorName': 'Stager, '
                                                                                        'Lawrence '
                                                                                        'E.'}],
                                                              'contact': [{'datasetContactAffiliation': 'Wheaton '
                                                                                                        'College',
                                                                           'datasetContactEmail': 'daniel.master@wheaton.edu',
                                                                           'datasetContactName': 'Master, '
                                                                                                 'Daniel '
                                                                                                 'M.'}],
                                                              'depositor': 'Mika, '
                                                                           'Katherine',
                                                              'description': [{'dsDescriptionValue': 'Fieldbooks '
                                                                                                     'from '
                                                                                                     'excavation: '
                                                                                                     '1985-2016'}],
                                                              'funding': 'The Leon '
                                                                         'Levy '
                                                                         'Foundation',
                                                              'geographicCoverage': 'Ashkelon',
                                                              'geographicUnit': 'City',
                                                              'keywords': [{'keywordValue': 'Archaeology'}],
                                                              'language': ['English'],
                                                              'license': 'CC '
                                                                         'BY-NC-ND '
                                                                         '4.0',
                                                              'subject': ['Arts '
                                                                          'and '
                                                                          'Humanities'],
                                                              'title': 'Leon Levy '
                                                                       'Expedition '
                                                                       'to '
                                                                       'Ashkelon '
                                                                       'Dataset: '
                                                                       'Fieldbooks'},
     'Leon Levy Expedition to Ashkelon Dataset: Material Culture Registry': {'author': [{'authorAffiliation': 'Wheaton '
                                                                                                              'College',
                                                                                         'authorName': 'Master, '
                                                                                                       'Daniel '
                                                                                                       'M.'},
                                                                                        {'authorAffiliation': 'Harvard '
                                                                                                              'University',
                                                                                         'authorName': 'Stager, '
                                                                                                       'Lawrence '
                                                                                                       'E.'}],
                                                                             'contact': [{'datasetContactAffiliation': 'Wheaton '
                                                                                                                       'College',
                                                                                          'datasetContactEmail': 'daniel.master@wheaton.edu',
                                                                                          'datasetContactName': 'Master, '
                                                                                                                'Daniel '
                                                                                                                'M.'}],
                                                                             'depositor': 'Mika, '
                                                                                          'Katherine',
                                                                             'description': [{'dsDescriptionValue': 'Matieral '
                                                                                                                    'culture '
                                                                                                                    'registry '
                                                                                                                    'files '
                                                                                                                    'from '
                                                                                                                    'excavation: '
                                                                                                                    '1985-2016'}],
                                                                             'funding': 'The '
                                                                                        'Leon '
                                                                                        'Levy '
                                                                                        'Foundation',
                                                                             'geographicCoverage': 'Ashkelon',
                                                                             'geographicUnit': 'City',
                                                                             'keywords': [{'keywordValue': 'Archaeology'}],
                                                                             'language': ['English'],
                                                                             'license': 'CC '
                                                                                        'BY-NC-ND '
                                                                                        '4.0',
                                                                             'subject': ['Arts '
                                                                                         'and '
                                                                                         'Humanities'],
                                                                             'title': 'Leon '
                                                                                      'Levy '
                                                                                      'Expedition '
                                                                                      'to '
                                                                                      'Ashkelon '
                                                                                      'Dataset: '
                                                                                      'Material '
                                                                                      'Culture '
                                                                                      'Registry'}}



```python
#Create dict of DataFrames containing metadata about individual files
for dataset in g_dataset_titles:
    # get dataset metadata 
    dataset_metadata = g_dataset_metadata[dataset]
    # get dataset inventory
    dataset_inventory_df = g_dataset_inventories[dataset]
    # create data file metadata
    g_datafile_metadata[dataset] = create_datafile_metadata(dataset_inventory_df)
```

# 3. Deposit Data Using EasyDataverse


```python
import sys
import rich
from easyDataverse import Dataverse
```


```python
dataverse = Dataverse(
    server_url= dv_installation_url,
    api_token= dv_api_key
)
```

    
    



    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">🎉 <span style="font-weight: bold">Connected to </span><span style="color: #008000; text-decoration-color: #008000; font-weight: bold">'https://dvn-cloud-app-2.lib.harvard.edu'</span>
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>




```python
# For each dataset, create a dataset and and save its information

for dataset in g_dataset_titles:
    # initiate dataset
    dv_dataset = dataverse.create_dataset()
    # get metadata 
    dataset_metadata = g_dataset_metadata[dataset]
    # create the dataset
    dv_dataset_info[dataset] = create_dataset(dv_dataset, dataset_metadata)
```


```python
# add files to datasets
for dataset_title, dataset in dv_dataset_info.items():
    file_inventory = g_datafile_metadata.get(dataset_title)
    # build path for each dataset dir
    dataset_dict = dataset.dataverse_dict()
    citation_fields = dataset_dict.get('datasetVersion', {}).get('metadataBlocks', {}).get('citation', {}).get('fields', [])
    # add files to dataset from inventory using dataset files path
    add_files_to_dataset(dataset, file_inventory, datafiles_path)
    title = dataset.citation.title
    rich.print(f"Added {len(dataset.files)} file to {title}")
```


<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">Added <span style="color: #008080; text-decoration-color: #008080; font-weight: bold">343</span> file to Leon Levy Expedition to Ashkelon Dataset: Fieldbooks
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">Added <span style="color: #008080; text-decoration-color: #008080; font-weight: bold">37</span> file to Leon Levy Expedition to Ashkelon Dataset: Material Culture Registry
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">Added <span style="color: #008080; text-decoration-color: #008080; font-weight: bold">428</span> file to Leon Levy Expedition to Ashkelon Dataset: Field Reports
</pre>




```python
total_length = sum(len(dataset.files) for dataset_title, dataset in dv_dataset_info.items())
total_length

# compare to length of dataset inventory table
```




    808




```python
#New upload loop - June 2026
# Path for crash-recovery checkpoint
pid_checkpoint_file = os.path.join(source_path, "upload_pids.json")

# Load any PIDs saved from a previous (possibly interrupted) run
collection_pids = load_pids(pid_checkpoint_file)

failed_datasets = {}

for dataset_title, dataset in dv_dataset_info.items():
    print(f"\n{'═' * 60}")
    print(f"  {dataset_title}")
    print(f"{'═' * 60}")

    # ── Restore PID from checkpoint so a kernel restart can resume cleanly ──
    if dataset_title in collection_pids and dataset.p_id is None:
        dataset.p_id = collection_pids[dataset_title]
        print(f"  🔁 Restored PID from checkpoint: {dataset.p_id}")

    dataset.license = dataverse.licenses["CC BY-NC-ND 4.0"]

    try:
        pid = resilient_upload(
            dataset=dataset,
            dataverse_name=dv_collection,
            n_parallel=1,       # 1 is safest against the rate limiter
            max_retries=5,
            base_delay=60,      # 60 → 120 → 240 → 480 → 960 s
            batch_size=100,
        )
        collection_pids[dataset_title] = pid
        save_pids(pid_checkpoint_file, collection_pids)  # ← persist after every dataset
        print(f"\n  🎉 '{dataset_title}' done → {pid}")

    except Exception as exc:
        print(f"\n  ❌ '{dataset_title}' ultimately failed: {exc}")
        failed_datasets[dataset_title] = str(exc)
        # Save progress so you can fix the failure and rerun just this dataset
        save_pids(pid_checkpoint_file, collection_pids)

    # Inter-dataset cooldown (skip after last)
    if dataset_title != list(dv_dataset_info.keys())[-1]:
        print("\n  ⏳ Waiting 5 minutes before next dataset …")
        time.sleep(300)

# ── Final summary ────────────────────────────────────────────────────────────
print(f"\n{'═' * 60}")
print(f"  Succeeded : {len(collection_pids)}")
print(f"  Failed    : {len(failed_datasets)}")
if failed_datasets:
    print(f"  Failed datasets: {list(failed_datasets.keys())}")
```

    
    ════════════════════════════════════════════════════════════
      Leon Levy Expedition to Ashkelon Dataset: Fieldbooks
    ════════════════════════════════════════════════════════════
    
    📦 Creating dataset: 'Leon Levy Expedition to Ashkelon Dataset: Fieldbooks'
    Dataset with pid 'doi:10.7910/DVN/MV7O1B' created.
      ✅ Dataset created → doi:10.7910/DVN/MV7O1B
      📂 0 already uploaded · 343 still to upload out of 343 total
    
      🗂  Batch 1/4 — 100 files
    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">╭────────────────── <span style="font-weight: bold">DVUploader</span> ───────────────────╮
│ Server: <span style="font-weight: bold">https://dvn-cloud-app-2.lib.harvard.edu</span> │
│ PID: <span style="font-weight: bold">doi:10.7910/DVN/MV7O1B</span>                     │
│ Files: 100                                      │
╰─────────────────────────────────────────────────╯
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="font-style: italic">   </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🔎 Checking</span><span style="font-style: italic">   </span>
<span style="font-style: italic">  </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">dataset files</span><span style="font-style: italic">  </span>
┏━━━━━┳━━━━━━━━━┓
┃<span style="font-weight: bold"> New </span>┃<span style="font-weight: bold"> Replace </span>┃
┡━━━━━╇━━━━━━━━━┩
│<span style="color: #00d75f; text-decoration-color: #00d75f"> 100 </span>│<span style="color: #808080; text-decoration-color: #808080"> 0       </span>│
└─────┴─────────┘
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🚀 Uploading files</span>

</pre>




    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">✅ Upload complete</span>

</pre>



      ✅ Batch 1 complete.
      ⏳ Pausing 30s before next batch …
    
      🗂  Batch 2/4 — 100 files
    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">╭────────────────── <span style="font-weight: bold">DVUploader</span> ───────────────────╮
│ Server: <span style="font-weight: bold">https://dvn-cloud-app-2.lib.harvard.edu</span> │
│ PID: <span style="font-weight: bold">doi:10.7910/DVN/MV7O1B</span>                     │
│ Files: 100                                      │
╰─────────────────────────────────────────────────╯
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="font-style: italic">   </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🔎 Checking</span><span style="font-style: italic">   </span>
<span style="font-style: italic">  </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">dataset files</span><span style="font-style: italic">  </span>
┏━━━━━┳━━━━━━━━━┓
┃<span style="font-weight: bold"> New </span>┃<span style="font-weight: bold"> Replace </span>┃
┡━━━━━╇━━━━━━━━━┩
│<span style="color: #00d75f; text-decoration-color: #00d75f"> 100 </span>│<span style="color: #808080; text-decoration-color: #808080"> 0       </span>│
└─────┴─────────┘
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🚀 Uploading files</span>

</pre>




    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">✅ Upload complete</span>

</pre>



      ✅ Batch 2 complete.
      ⏳ Pausing 30s before next batch …
    
      🗂  Batch 3/4 — 100 files
    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">╭────────────────── <span style="font-weight: bold">DVUploader</span> ───────────────────╮
│ Server: <span style="font-weight: bold">https://dvn-cloud-app-2.lib.harvard.edu</span> │
│ PID: <span style="font-weight: bold">doi:10.7910/DVN/MV7O1B</span>                     │
│ Files: 100                                      │
╰─────────────────────────────────────────────────╯
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="font-style: italic">   </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🔎 Checking</span><span style="font-style: italic">   </span>
<span style="font-style: italic">  </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">dataset files</span><span style="font-style: italic">  </span>
┏━━━━━┳━━━━━━━━━┓
┃<span style="font-weight: bold"> New </span>┃<span style="font-weight: bold"> Replace </span>┃
┡━━━━━╇━━━━━━━━━┩
│<span style="color: #00d75f; text-decoration-color: #00d75f"> 100 </span>│<span style="color: #808080; text-decoration-color: #808080"> 0       </span>│
└─────┴─────────┘
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🚀 Uploading files</span>

</pre>




    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">✅ Upload complete</span>

</pre>



      ✅ Batch 3 complete.
      ⏳ Pausing 30s before next batch …
    
      🗂  Batch 4/4 — 43 files
    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">╭────────────────── <span style="font-weight: bold">DVUploader</span> ───────────────────╮
│ Server: <span style="font-weight: bold">https://dvn-cloud-app-2.lib.harvard.edu</span> │
│ PID: <span style="font-weight: bold">doi:10.7910/DVN/MV7O1B</span>                     │
│ Files: 43                                       │
╰─────────────────────────────────────────────────╯
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🔎 Checking dataset files</span><span style="font-style: italic">                               </span>
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┓
┃<span style="font-weight: bold"> File                               </span>┃<span style="font-weight: bold"> Status </span>┃<span style="font-weight: bold"> Action </span>┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━┩
│<span style="color: #008080; text-decoration-color: #008080"> 50.59 1989 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.59 1990 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.59 1991 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.59 1992 Fieldbook (Summer).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.59 1992 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.59 1993 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.59 1996 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.59 1997 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.59 1998 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.59 1999 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.59 2000 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.66,67 1995 Fieldbook.pdf        </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.67 1997 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.67 1998 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.67 1999 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.67 2000 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 50.68 1995 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 51.73 1997 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 51.73 1998 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 51.73 1999 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 51.73 2000 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 51.74 1997 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 51.74 1998 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 51.74 1999 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 51.74 2000 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.58 1986 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.58 1987 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.58 1988 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.58 1989 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.58 1990 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.58,68 1986 Fieldbook photos.pdf </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.65 1991 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.65 1992 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.68 1986 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.68 1987 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.68 1988 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.68 1989 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.68 1990 Fieldbook (North).pdf   </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.68 1990 Fieldbook (South).pdf   </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 57.68 1991 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 64.87 1987 Fieldbook.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 64.87, 71.25 1988 Fieldbook.pdf    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 71 1987 Fieldbook.pdf              </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
└────────────────────────────────────┴────────┴────────┘
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🚀 Uploading files</span>

</pre>




    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">✅ Upload complete</span>

</pre>



      ✅ Batch 4 complete.
      💾 Checkpointed PIDs → /Users/katherinemika/Desktop/curation/leon_levy/summer2026/upload_pids.json
    
      🎉 'Leon Levy Expedition to Ashkelon Dataset: Fieldbooks' done → doi:10.7910/DVN/MV7O1B
    
      ⏳ Waiting 5 minutes before next dataset …
    
    ════════════════════════════════════════════════════════════
      Leon Levy Expedition to Ashkelon Dataset: Material Culture Registry
    ════════════════════════════════════════════════════════════
    
    📦 Creating dataset: 'Leon Levy Expedition to Ashkelon Dataset: Material Culture Registry'
    Dataset with pid 'doi:10.7910/DVN/OD9HLS' created.
      ✅ Dataset created → doi:10.7910/DVN/OD9HLS
      📂 0 already uploaded · 37 still to upload out of 37 total
    
      🗂  Batch 1/1 — 37 files
    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">╭────────────────── <span style="font-weight: bold">DVUploader</span> ───────────────────╮
│ Server: <span style="font-weight: bold">https://dvn-cloud-app-2.lib.harvard.edu</span> │
│ PID: <span style="font-weight: bold">doi:10.7910/DVN/OD9HLS</span>                     │
│ Files: 37                                       │
╰─────────────────────────────────────────────────╯
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🔎 Checking dataset files</span><span style="font-style: italic">                                </span>
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┓
┃<span style="font-weight: bold"> File                                </span>┃<span style="font-weight: bold"> Status </span>┃<span style="font-weight: bold"> Action </span>┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━┩
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 01 (1985).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 02 (1985).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 03 (1985).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 04 (1985).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 05 (1986).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 06 (1986).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 07 (1987).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 09 (1987).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 10 (1988).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 11 (1988).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 11a (1988).pdf </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 12 (1989).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 13 (1989).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 14 (1989).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 15 (1990).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 16 (1990).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 17 (1990).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 18 (1991).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 19 (1991).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 20 (1992).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 21 (1992).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 22 (1993).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 23 (1993).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 24 (1994).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 25 (1995).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 26 (1996).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 27 (1996).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 28 (1997).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 29 (1997).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 30 (1998).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 31 (1998).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 33 (1999).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 34 (2000).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 35 (2000).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 36 (2000).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Master Registry vol. 37 (2004).pdf  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> Metal Objects Catalogue.pdf         </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
└─────────────────────────────────────┴────────┴────────┘
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🚀 Uploading files</span>

</pre>




    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">✅ Upload complete</span>

</pre>



      ✅ Batch 1 complete.
      💾 Checkpointed PIDs → /Users/katherinemika/Desktop/curation/leon_levy/summer2026/upload_pids.json
    
      🎉 'Leon Levy Expedition to Ashkelon Dataset: Material Culture Registry' done → doi:10.7910/DVN/OD9HLS
    
      ⏳ Waiting 5 minutes before next dataset …
    
    ════════════════════════════════════════════════════════════
      Leon Levy Expedition to Ashkelon Dataset: Field Reports
    ════════════════════════════════════════════════════════════
    
    📦 Creating dataset: 'Leon Levy Expedition to Ashkelon Dataset: Field Reports'
    Dataset with pid 'doi:10.7910/DVN/OSFQA7' created.
      ✅ Dataset created → doi:10.7910/DVN/OSFQA7
      📂 0 already uploaded · 428 still to upload out of 428 total
    
      🗂  Batch 1/5 — 100 files
    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">╭────────────────── <span style="font-weight: bold">DVUploader</span> ───────────────────╮
│ Server: <span style="font-weight: bold">https://dvn-cloud-app-2.lib.harvard.edu</span> │
│ PID: <span style="font-weight: bold">doi:10.7910/DVN/OSFQA7</span>                     │
│ Files: 100                                      │
╰─────────────────────────────────────────────────╯
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="font-style: italic">   </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🔎 Checking</span><span style="font-style: italic">   </span>
<span style="font-style: italic">  </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">dataset files</span><span style="font-style: italic">  </span>
┏━━━━━┳━━━━━━━━━┓
┃<span style="font-weight: bold"> New </span>┃<span style="font-weight: bold"> Replace </span>┃
┡━━━━━╇━━━━━━━━━┩
│<span style="color: #00d75f; text-decoration-color: #00d75f"> 100 </span>│<span style="color: #808080; text-decoration-color: #808080"> 0       </span>│
└─────┴─────────┘
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🚀 Uploading files</span>

</pre>




    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">✅ Upload complete</span>

</pre>



      ✅ Batch 1 complete.
      ⏳ Pausing 30s before next batch …
    
      🗂  Batch 2/5 — 100 files
    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">╭────────────────── <span style="font-weight: bold">DVUploader</span> ───────────────────╮
│ Server: <span style="font-weight: bold">https://dvn-cloud-app-2.lib.harvard.edu</span> │
│ PID: <span style="font-weight: bold">doi:10.7910/DVN/OSFQA7</span>                     │
│ Files: 100                                      │
╰─────────────────────────────────────────────────╯
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="font-style: italic">   </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🔎 Checking</span><span style="font-style: italic">   </span>
<span style="font-style: italic">  </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">dataset files</span><span style="font-style: italic">  </span>
┏━━━━━┳━━━━━━━━━┓
┃<span style="font-weight: bold"> New </span>┃<span style="font-weight: bold"> Replace </span>┃
┡━━━━━╇━━━━━━━━━┩
│<span style="color: #00d75f; text-decoration-color: #00d75f"> 100 </span>│<span style="color: #808080; text-decoration-color: #808080"> 0       </span>│
└─────┴─────────┘
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🚀 Uploading files</span>

</pre>




    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">✅ Upload complete</span>

</pre>



      ✅ Batch 2 complete.
      ⏳ Pausing 30s before next batch …
    
      🗂  Batch 3/5 — 100 files
    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">╭────────────────── <span style="font-weight: bold">DVUploader</span> ───────────────────╮
│ Server: <span style="font-weight: bold">https://dvn-cloud-app-2.lib.harvard.edu</span> │
│ PID: <span style="font-weight: bold">doi:10.7910/DVN/OSFQA7</span>                     │
│ Files: 100                                      │
╰─────────────────────────────────────────────────╯
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="font-style: italic">   </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🔎 Checking</span><span style="font-style: italic">   </span>
<span style="font-style: italic">  </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">dataset files</span><span style="font-style: italic">  </span>
┏━━━━━┳━━━━━━━━━┓
┃<span style="font-weight: bold"> New </span>┃<span style="font-weight: bold"> Replace </span>┃
┡━━━━━╇━━━━━━━━━┩
│<span style="color: #00d75f; text-decoration-color: #00d75f"> 100 </span>│<span style="color: #808080; text-decoration-color: #808080"> 0       </span>│
└─────┴─────────┘
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🚀 Uploading files</span>

</pre>




    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">✅ Upload complete</span>

</pre>



      ✅ Batch 3 complete.
      ⏳ Pausing 30s before next batch …
    
      🗂  Batch 4/5 — 100 files
    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">╭────────────────── <span style="font-weight: bold">DVUploader</span> ───────────────────╮
│ Server: <span style="font-weight: bold">https://dvn-cloud-app-2.lib.harvard.edu</span> │
│ PID: <span style="font-weight: bold">doi:10.7910/DVN/OSFQA7</span>                     │
│ Files: 100                                      │
╰─────────────────────────────────────────────────╯
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="font-style: italic">   </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🔎 Checking</span><span style="font-style: italic">   </span>
<span style="font-style: italic">  </span><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">dataset files</span><span style="font-style: italic">  </span>
┏━━━━━┳━━━━━━━━━┓
┃<span style="font-weight: bold"> New </span>┃<span style="font-weight: bold"> Replace </span>┃
┡━━━━━╇━━━━━━━━━┩
│<span style="color: #00d75f; text-decoration-color: #00d75f"> 100 </span>│<span style="color: #808080; text-decoration-color: #808080"> 0       </span>│
└─────┴─────────┘
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🚀 Uploading files</span>

</pre>




    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">✅ Upload complete</span>

</pre>



      ✅ Batch 4 complete.
      ⏳ Pausing 30s before next batch …
    
      🗂  Batch 5/5 — 28 files
    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">╭────────────────── <span style="font-weight: bold">DVUploader</span> ───────────────────╮
│ Server: <span style="font-weight: bold">https://dvn-cloud-app-2.lib.harvard.edu</span> │
│ PID: <span style="font-weight: bold">doi:10.7910/DVN/OSFQA7</span>                     │
│ Files: 28                                       │
╰─────────────────────────────────────────────────╯
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🔎 Checking dataset files</span><span style="font-style: italic">                               </span>
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┓
┃<span style="font-weight: bold"> File                               </span>┃<span style="font-weight: bold"> Status </span>┃<span style="font-weight: bold"> Action </span>┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━┩
│<span style="color: #008080; text-decoration-color: #008080"> 2014_N5_Aja.pdf                    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2014_Physical_Anthropology_Fox.pdf </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_16.39-49-59_Shames.pdf        </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_16_Walton.pdf                 </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_25.96_Ehrlich.pdf             </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_25_Hoffman.pdf                </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_47.94_Ehrlich.pdf             </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_47_Hoffman.pdf                </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_51.73_Dutton.pdf              </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_51.74-75_Holtje.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_51.83_Dutton.pdf              </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_51.84_Erickson.pdf            </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_51_Wright-Wylie.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_54_Hoffman.pdf                </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_N5.14_Fu.pdf                  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_N5.14_Kalisher.pdf            </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_N5.64_Fu.pdf                  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_N5.64_Kalisher.pdf            </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2015_N5_Aja.pdf                    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2016_51.74_Erickson.pdf            </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2016_51.74-75_Walton.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2016_51.83_Dutton.pdf              </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2016_51.84-85_Erickson.pdf         </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2016_51_Wylie.pdf                  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2016_N5.3_Walton.pdf               </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2016_N5.4_Fu-McCully.pdf           </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2016_N5.24_Hoffman.pdf             </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> 2016_N5_Aja.pdf                    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
└────────────────────────────────────┴────────┴────────┘
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🚀 Uploading files</span>

</pre>




    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">✅ Upload complete</span>

</pre>



      ✅ Batch 5 complete.
      💾 Checkpointed PIDs → /Users/katherinemika/Desktop/curation/leon_levy/summer2026/upload_pids.json
    
      🎉 'Leon Levy Expedition to Ashkelon Dataset: Field Reports' done → doi:10.7910/DVN/OSFQA7
    
    ════════════════════════════════════════════════════════════
      Succeeded : 13
      Failed    : 0



```python
# ── Retry failed datasets after main loop resolves ───────────────────────────

if not failed_datasets:
    print("✅ No failed datasets to retry.")
else:
    print(f"\n{'═' * 60}")
    print(f"  Retrying {len(failed_datasets)} failed dataset(s)")
    print(f"{'═' * 60}")

    # Reload checkpoint in case anything was written during the loop
    collection_pids = load_pids(pid_checkpoint_file)

    retry_succeeded = []
    still_failing   = {}

    for dataset_title in list(failed_datasets.keys()):
        print(f"\n{'─' * 60}")
        print(f"  🔁 Retrying: {dataset_title}")
        print(f"{'─' * 60}")

        dataset = dv_dataset_info[dataset_title]

        # ── Restore PID so resilient_upload skips dataset creation ───────────
        # If PID is in checkpoint the dataset was created but files failed.
        # If PID is NOT in checkpoint the dataset itself never got created —
        # resilient_upload will create it fresh.
        if dataset_title in collection_pids and dataset.p_id is None:
            dataset.p_id = collection_pids[dataset_title]
            print(f"  📌 Restored PID from checkpoint: {dataset.p_id}")
            print(f"     → Dataset already exists; will only upload missing files")
        elif dataset.p_id is not None:
            print(f"  📌 PID already on dataset object: {dataset.p_id}")
        else:
            print(f"  ⚠️  No PID found — dataset will be created from scratch")

        # Restore license (may have been lost if kernel was restarted)
        dataset.license = dataverse.licenses["CC BY-NC-ND 4.0"]

        try:
            pid = resilient_upload(
                dataset=dataset,
                dataverse_name=dv_collection,
                n_parallel=1,
                max_retries=5,
                base_delay=120,     # more generous on retry
                batch_size=25,      # smaller batches on retry
            )
            collection_pids[dataset_title] = pid
            save_pids(pid_checkpoint_file, collection_pids)
            retry_succeeded.append(dataset_title)
            # Remove from failed_datasets now that it succeeded
            del failed_datasets[dataset_title]
            print(f"\n  🎉 '{dataset_title}' retry succeeded → {pid}")

        except Exception as exc:
            print(f"\n  ❌ '{dataset_title}' still failing: {exc}")
            still_failing[dataset_title] = str(exc)

    # ── Retry summary ─────────────────────────────────────────────────────────
    print(f"\n{'═' * 60}")
    print(f"  Retry succeeded : {len(retry_succeeded)}")
    if retry_succeeded:
        for t in retry_succeeded:
            print(f"    • {t}  →  {collection_pids[t]}")
    print(f"  Still failing   : {len(still_failing)}")
    if still_failing:
        for t, err in still_failing.items():
            print(f"    • {t}  —  {err}")
        print(f"\n  💡 For persistent failures, try running the single-dataset")
        print(f"     retry cell with batch_size reduced to 10 or 5.")
    print(f"{'═' * 60}")
```

    ✅ No failed datasets to retry.



```python
# ── Targeted retry for a single failed dataset ───────────────────────────────

retry_title = "2022 Photographs"

# Reload checkpoint in case kernel was restarted
collection_pids = load_pids(pid_checkpoint_file)

dataset = dv_dataset_info[retry_title]

# Restore PID from checkpoint so resilient_upload knows not to create a new dataset
if retry_title in collection_pids and dataset.p_id is None:
    dataset.p_id = collection_pids[retry_title]
    print(f"  🔁 Restored PID from checkpoint: {dataset.p_id}")
else:
    print(f"  ℹ️  PID already set on dataset object: {dataset.p_id}")

dataset.license = dataverse.licenses["CC BY-NC-ND 4.0"]

try:
    pid = resilient_upload(
        dataset=dataset,
        dataverse_name=dv_collection,
        n_parallel=1,
        max_retries=5,
        base_delay=60,
        batch_size=50,      # ← reduced from 100; "Too much data" suggests smaller chunks help
    )
    collection_pids[retry_title] = pid
    save_pids(pid_checkpoint_file, collection_pids)
    print(f"\n  🎉 '{retry_title}' done → {pid}")

except Exception as exc:
    print(f"\n  ❌ Still failing: {exc}")
    print("  💡 Try reducing batch_size further (e.g. 10) or check your VPN connection.")
```

      ℹ️  PID already set on dataset object: doi:10.7910/DVN/HBCIZR
    
    📦 Resuming dataset '2022 Photographs' → doi:10.7910/DVN/HBCIZR
      📂 800 already uploaded · 146 still to upload out of 946 total
    
      🗂  Batch 1/3 — 50 files
    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">╭───────────── <span style="font-weight: bold">DVUploader</span> ──────────────╮
│ Server: <span style="font-weight: bold">https://dataverse.harvard.edu</span> │
│ PID: <span style="font-weight: bold">doi:10.7910/DVN/HBCIZR</span>           │
│ Files: 50                             │
╰───────────────────────────────────────╯
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🔎 Checking dataset files</span><span style="font-style: italic">          </span>
┏━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┓
┃<span style="font-weight: bold"> File          </span>┃<span style="font-weight: bold"> Status </span>┃<span style="font-weight: bold"> Action </span>┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━┩
│<span style="color: #008080; text-decoration-color: #008080"> A22_34917.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34918.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34919.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34920.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34921.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34922.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34923.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34924.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34925.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34926.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34927.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34928.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34929.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34930.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34931.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34932.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34933.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34934.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34935.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34936.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34937.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34938.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34939.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34940.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34941.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34942.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34943.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34944.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34945.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34946.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34947.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34948.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34949.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34950.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34951.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34952.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34953.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34954.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34955.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34956.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34957.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34958.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34959.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34960.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34961.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34962.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34963.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34964.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34965.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A22_34966.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #808080; text-decoration-color: #808080">Upload</span> │
└───────────────┴────────┴────────┘
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🚀 Uploading files</span>

</pre>




    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>



      ⚠️  Attempt 1 failed (LocalProtocolError: Too much data for declared Content-Length)
      ↺  50 files remain · retrying in 60s …



    ---------------------------------------------------------------------------

    LocalProtocolError                        Traceback (most recent call last)

    Cell In[7], line 138, in resilient_upload(dataset, dataverse_name, n_parallel, max_retries, base_delay, batch_size)
        137 uploader = DVUploader(files=batch)
    --> 138 uploader.upload(
        139     persistent_id=p_id,
        140     dataverse_url=dv_url,
        141     api_token=token,
        142     n_parallel_uploads=n_parallel,
        143 )
        144 print(f"  ✅ Batch {batch_idx} complete.")


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/dvuploader/dvuploader.py:153, in DVUploader.upload(self, persistent_id, dataverse_url, api_token, n_parallel_uploads, force_native, replace_existing, proxy)
        152     with progress:
    --> 153         asyncio.run(
        154             direct_upload(
        155                 files=files,
        156                 dataverse_url=dataverse_url,
        157                 api_token=api_token,
        158                 persistent_id=persistent_id,
        159                 pbars=pbars,
        160                 progress=progress,
        161                 n_parallel_uploads=n_parallel_uploads,
        162             )
        163         )
        165 if self.verbose:


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/nest_asyncio.py:30, in _patch_asyncio.<locals>.run(main, debug)
         29 try:
    ---> 30     return loop.run_until_complete(task)
         31 finally:


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/nest_asyncio.py:98, in _patch_loop.<locals>.run_until_complete(self, future)
         96     raise RuntimeError(
         97         'Event loop stopped before Future completed.')
    ---> 98 return f.result()


    File ~/anaconda3/envs/curation/lib/python3.12/asyncio/futures.py:203, in Future.result(self)
        202 if self._exception is not None:
    --> 203     raise self._exception.with_traceback(self._exception_tb)
        204 return self._result


    File ~/anaconda3/envs/curation/lib/python3.12/asyncio/tasks.py:316, in Task.__step_run_and_handle_result(***failed resolving arguments***)
        315     else:
    --> 316         result = coro.throw(exc)
        317 except StopIteration as exc:


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/dvuploader/directupload.py:81, in direct_upload(files, dataverse_url, api_token, persistent_id, progress, pbars, n_parallel_uploads, proxy)
         66     tasks = [
         67         _upload_to_store(
         68             session=session,
       (...)
         78         for pbar, file in zip(pbars, files)
         79     ]
    ---> 81     upload_results = await asyncio.gather(*tasks)
         83 for status, file in upload_results:


    File ~/anaconda3/envs/curation/lib/python3.12/asyncio/tasks.py:385, in Task.__wakeup(self, future)
        384 try:
    --> 385     future.result()
        386 except BaseException as exc:
        387     # This may also be a cancellation.


    File ~/anaconda3/envs/curation/lib/python3.12/asyncio/tasks.py:314, in Task.__step_run_and_handle_result(***failed resolving arguments***)
        311 if exc is None:
        312     # We use the `send` method directly, because coroutines
        313     # don't have `__iter__` and `__next__` methods.
    --> 314     result = coro.send(None)
        315 else:


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/dvuploader/directupload.py:153, in _upload_to_store(session, file, persistent_id, dataverse_url, api_token, pbar, progress, delay, leave_bar)
        152 if "urls" not in ticket:
    --> 153     status, storage_identifier = await _upload_singlepart(
        154         session=session,
        155         ticket=ticket,
        156         file=file,
        157         pbar=pbar,
        158         progress=progress,
        159         api_token=api_token,
        160         leave_bar=leave_bar,
        161     )
        163 else:


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/dvuploader/directupload.py:260, in _upload_singlepart(session, ticket, file, pbar, progress, api_token, leave_bar)
        249 params = {
        250     "headers": headers,
        251     "url": ticket["url"],
       (...)
        257     ),
        258 }
    --> 260 response = await session.put(**params)
        261 response.raise_for_status()


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpx/_client.py:1896, in AsyncClient.put(self, url, content, data, files, json, params, headers, cookies, auth, follow_redirects, timeout, extensions)
       1891 """
       1892 Send a `PUT` request.
       1893 
       1894 **Parameters**: See `httpx.request`.
       1895 """
    -> 1896 return await self.request(
       1897     "PUT",
       1898     url,
       1899     content=content,
       1900     data=data,
       1901     files=files,
       1902     json=json,
       1903     params=params,
       1904     headers=headers,
       1905     cookies=cookies,
       1906     auth=auth,
       1907     follow_redirects=follow_redirects,
       1908     timeout=timeout,
       1909     extensions=extensions,
       1910 )


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpx/_client.py:1540, in AsyncClient.request(self, method, url, content, data, files, json, params, headers, cookies, auth, follow_redirects, timeout, extensions)
       1527 request = self.build_request(
       1528     method=method,
       1529     url=url,
       (...)
       1538     extensions=extensions,
       1539 )
    -> 1540 return await self.send(request, auth=auth, follow_redirects=follow_redirects)


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpx/_client.py:1629, in AsyncClient.send(self, request, stream, auth, follow_redirects)
       1627 auth = self._build_request_auth(request, auth)
    -> 1629 response = await self._send_handling_auth(
       1630     request,
       1631     auth=auth,
       1632     follow_redirects=follow_redirects,
       1633     history=[],
       1634 )
       1635 try:


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpx/_client.py:1657, in AsyncClient._send_handling_auth(self, request, auth, follow_redirects, history)
       1656 while True:
    -> 1657     response = await self._send_handling_redirects(
       1658         request,
       1659         follow_redirects=follow_redirects,
       1660         history=history,
       1661     )
       1662     try:


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpx/_client.py:1694, in AsyncClient._send_handling_redirects(self, request, follow_redirects, history)
       1692     await hook(request)
    -> 1694 response = await self._send_single_request(request)
       1695 try:


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpx/_client.py:1730, in AsyncClient._send_single_request(self, request)
       1729 with request_context(request=request):
    -> 1730     response = await transport.handle_async_request(request)
       1732 assert isinstance(response.stream, AsyncByteStream)


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpx/_transports/default.py:394, in AsyncHTTPTransport.handle_async_request(self, request)
        393 with map_httpcore_exceptions():
    --> 394     resp = await self._pool.handle_async_request(req)
        396 assert isinstance(resp.stream, typing.AsyncIterable)


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpcore/_async/connection_pool.py:256, in AsyncConnectionPool.handle_async_request(self, request)
        255     await self._close_connections(closing)
    --> 256     raise exc from None
        258 # Return the response. Note that in this case we still have to manage
        259 # the point at which the response is closed.


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpcore/_async/connection_pool.py:236, in AsyncConnectionPool.handle_async_request(self, request)
        234 try:
        235     # Send the request on the assigned connection.
    --> 236     response = await connection.handle_async_request(
        237         pool_request.request
        238     )
        239 except ConnectionNotAvailable:
        240     # In some cases a connection may initially be available to
        241     # handle a request, but then become unavailable.
        242     #
        243     # In this case we clear the connection and try again.


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpcore/_async/connection.py:103, in AsyncHTTPConnection.handle_async_request(self, request)
        101     raise exc
    --> 103 return await self._connection.handle_async_request(request)


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpcore/_async/http11.py:136, in AsyncHTTP11Connection.handle_async_request(self, request)
        135         await self._response_closed()
    --> 136 raise exc


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpcore/_async/http11.py:88, in AsyncHTTP11Connection.handle_async_request(self, request)
         87     async with Trace("send_request_body", logger, request, kwargs) as trace:
    ---> 88         await self._send_request_body(**kwargs)
         89 except WriteError:
         90     # If we get a write error while we're writing the request,
         91     # then we supress this error and move on to attempting to
         92     # read the response. Servers can sometimes close the request
         93     # pre-emptively and then respond with a well formed HTTP
         94     # error response.


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpcore/_async/http11.py:159, in AsyncHTTP11Connection._send_request_body(self, request)
        158     event = h11.Data(data=chunk)
    --> 159     await self._send_event(event, timeout=timeout)
        161 await self._send_event(h11.EndOfMessage(), timeout=timeout)


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/httpcore/_async/http11.py:164, in AsyncHTTP11Connection._send_event(self, event, timeout)
        163 async def _send_event(self, event: h11.Event, timeout: float | None = None) -> None:
    --> 164     bytes_to_send = self._h11_state.send(event)
        165     if bytes_to_send is not None:


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/h11/_connection.py:512, in Connection.send(self, event)
        492 """Convert a high-level event into bytes that can be sent to the peer,
        493 while updating our internal state machine.
        494 
       (...)
        510 
        511 """
    --> 512 data_list = self.send_with_data_passthrough(event)
        513 if data_list is None:


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/h11/_connection.py:545, in Connection.send_with_data_passthrough(self, event)
        544 data_list: List[bytes] = []
    --> 545 writer(event, data_list.append)
        546 return data_list


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/h11/_writers.py:65, in BodyWriter.__call__(self, event, write)
         64 if type(event) is Data:
    ---> 65     self.send_data(event.data, write)
         66 elif type(event) is EndOfMessage:


    File ~/anaconda3/envs/curation/lib/python3.12/site-packages/h11/_writers.py:91, in ContentLengthWriter.send_data(self, data, write)
         90 if self._length < 0:
    ---> 91     raise LocalProtocolError("Too much data for declared Content-Length")
         92 write(data)


    LocalProtocolError: Too much data for declared Content-Length

    
    During handling of the above exception, another exception occurred:


    KeyboardInterrupt                         Traceback (most recent call last)

    Cell In[21], line 20
         17 dataset.license = dataverse.licenses["CC BY-NC-ND 4.0"]
         19 try:
    ---> 20     pid = resilient_upload(
         21         dataset=dataset,
         22         dataverse_name=dv_collection,
         23         n_parallel=1,
         24         max_retries=5,
         25         base_delay=60,
         26         batch_size=50,      # ← reduced from 100; "Too much data" suggests smaller chunks help
         27     )
         28     collection_pids[retry_title] = pid
         29     save_pids(pid_checkpoint_file, collection_pids)


    Cell In[7], line 167, in resilient_upload(dataset, dataverse_name, n_parallel, max_retries, base_delay, batch_size)
        165         print(f"  ⚠️  Attempt {attempt} failed ({type(exc).__name__}: {exc})")
        166         print(f"  ↺  {len(batch)} files remain · retrying in {delay}s …")
    --> 167         time.sleep(delay)
        169 # Brief cooldown between batches (not after the last one)
        170 if batch_idx < len(batches):


    KeyboardInterrupt: 


### Set user permissions for all sub-collections

Necessary step because permissions do not inherit! 


```python
def get_subcollections(parent_collection_id, dataverse_url, api_token):
    """
    Retrieve all sub-collections within a parent Dataverse collection.
    Filters to only 'dataverse' type items (excludes datasets).

    Parameters
    ----------
    parent_collection_id : str
        Alias or ID of the parent collection (e.g. 'leon_levy')
    dataverse_url : str
        Base URL of the Dataverse installation
    api_token : str
        Dataverse API token

    Returns
    -------
    list of dict : each dict contains 'id' and 'title' for one sub-collection
    """
    endpoint = f"{dataverse_url.rstrip('/')}/api/dataverses/{parent_collection_id}/contents"
    headers  = {"X-Dataverse-key": api_token}

    print(f"📋 Fetching contents of '{parent_collection_id}'...")

    try:
        resp = requests.get(endpoint, headers=headers, verify=False, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"  ❌ HTTP error fetching contents: {e}")
        print(f"  Response body: {resp.text[:500]}")
        return []
    except Exception as e:
        print(f"  ❌ Unexpected error fetching contents: {e}")
        return []

    contents = resp.json().get("data", [])

    # Filter to sub-collections only (type == "dataverse")
    # Note: the API response only returns 'id' and 'title', no 'alias'
    subcollections = [
        {
            "id"    : item["id"],
            "title" : item.get("title", "Untitled"),
        }
        for item in contents
        if item.get("type") == "dataverse"
    ]

    print(f"  ✅ Found {len(subcollections)} sub-collection(s) "
          f"(skipped {len(contents) - len(subcollections)} non-dataverse items)")

    return subcollections


def assign_role(collection_id, assignee, role, dataverse_url, api_token):
    """
    Assign a role to a user for a given Dataverse collection.

    Parameters
    ----------
    collection_id : str or int
        Numeric ID of the target collection
    assignee : str
        Dataverse username prefixed with '@' (e.g. '@dmmaster')
    role : str
        Role alias to assign (e.g. 'admin', 'curator', 'editor')
    dataverse_url : str
        Base URL of the Dataverse installation
    api_token : str
        Dataverse API token

    Returns
    -------
    bool : True if assignment succeeded, False otherwise
    """
    endpoint = f"{dataverse_url.rstrip('/')}/api/dataverses/{collection_id}/assignments"
    headers  = {
        "X-Dataverse-key" : api_token,
        "Content-Type"    : "application/json",
    }
    payload = {
        "assignee" : assignee,
        "role"     : role,
    }

    try:
        resp = requests.post(
            endpoint,
            headers=headers,
            json=payload,
            verify=False,
            timeout=30,
        )
        resp.raise_for_status()
        print(f"    ✅ Assigned '{role}' to '{assignee}' on collection '{collection_id}'")
        return True

    except requests.exceptions.HTTPError as e:
        print(f"    ❌ HTTP error assigning role on '{collection_id}': {e}")
        print(f"       Response body: {resp.text[:300]}")
        return False

    except Exception as e:
        print(f"    ❌ Unexpected error assigning role on '{collection_id}': {e}")
        return False


def assign_role_to_all_subcollections(
    parent_collection_id,
    assignee,
    role,
    dataverse_url,
    api_token,
    include_parent=False,
):
    """
    Full pipeline: fetch all sub-collections under parent_collection_id,
    then assign the given role to assignee for every one of them.

    Parameters
    ----------
    parent_collection_id : str
        Alias or ID of the parent collection (e.g. 'leon_levy')
    assignee : str
        Dataverse username prefixed with '@' (e.g. '@dmmaster')
    role : str
        Role alias to assign (e.g. 'admin', 'curator')
    dataverse_url : str
        Base URL of the Dataverse installation
    api_token : str
        Dataverse API token
    include_parent : bool
        If True, also assign the role on the parent collection itself.
        Defaults to False.

    Returns
    -------
    dict : {'succeeded': [...], 'failed': [...]}
    """
    print(f"\n{'═' * 60}")
    print(f"  Role assignment pipeline")
    print(f"  Parent collection : {parent_collection_id}")
    print(f"  Assignee          : {assignee}")
    print(f"  Role              : {role}")
    print(f"{'═' * 60}\n")

    # ── Step 1: get sub-collections ───────────────────────────────────────────
    subcollections = get_subcollections(parent_collection_id, dataverse_url, api_token)

    if not subcollections:
        print("  ⚠️  No sub-collections found. Exiting.")
        return {"succeeded": [], "failed": []}

    print(f"\n  Sub-collections to process:")
    for sc in subcollections:
        print(f"    • [{sc['id']}]  {sc['title']}")

    # Optionally prepend the parent itself
    targets = subcollections.copy()
    if include_parent:
        targets.insert(0, {
            "id"    : parent_collection_id,
            "title" : "(parent collection)",
        })

    # ── Step 2: assign role on every target ───────────────────────────────────
    print(f"\n  Assigning roles...")

    succeeded = []
    failed    = []

    for sc in targets:
        target_id = sc["id"]
        ok = assign_role(target_id, assignee, role, dataverse_url, api_token)

        if ok:
            succeeded.append({"id": target_id, "title": sc["title"]})
        else:
            failed.append({"id": target_id, "title": sc["title"]})

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'═' * 60}")
    print(f"  ✅ Succeeded : {len(succeeded)}")
    if succeeded:
        for s in succeeded:
            print(f"    • [{s['id']}]  {s['title']}")
    print(f"  ❌ Failed    : {len(failed)}")
    if failed:
        for f in failed:
            print(f"    • [{f['id']}]  {f['title']}")
    print(f"{'═' * 60}\n")

    return {"succeeded": succeeded, "failed": failed}
```


```python
# ── Run it ────────────────────────────────────────────────────────────────────
results = assign_role_to_all_subcollections(
    parent_collection_id = dv_collection,  # parent collection alias
    assignee             = "@dmmaster",
    role                 = "admin",
    dataverse_url        = dv_installation_url,
    api_token            = dv_api_key,
    include_parent       = False,        # set True to also assign on leon_levy itself
)
```

    
    ════════════════════════════════════════════════════════════
      Role assignment pipeline
      Parent collection : levy_photos
      Assignee          : @dmmaster
      Role              : admin
    ════════════════════════════════════════════════════════════
    
    📋 Fetching contents of 'levy_photos'...
      ✅ Found 28 sub-collection(s) (skipped 33 non-dataverse items)
    
      Sub-collections to process:
        • [4040415]  2012 Photographs
        • [4549804]  1995 Photographs
        • [4550541]  1996 Photographs
        • [4550543]  1997 Photographs
        • [4550544]  1998 Photographs
        • [4550546]  1999 Photographs
        • [4550547]  2000 Photographs
        • [4638448]  1993 Photographs
        • [4880636]  1985 Photographs
        • [4880646]  1986 Photographs
        • [4883659]  1987 Photographs
        • [4887813]  1988 Photographs
        • [4892626]  1989 Photographs
        • [4916701]  1990 Photographs
        • [4929239]  1991 Photographs
        • [4930201]  1992 Photographs
        • [4936295]  1994 Photographs
        • [4938341]  2004 Photographs
        • [4946989]  2007 Photographs
        • [4950812]  2008 Photographs
        • [4953735]  2009 Photographs
        • [4954152]  2010 Photographs
        • [4968696]  2011 Photographs
        • [4973279]  2013 Photographs
        • [4974444]  2014 Photographs
        • [5006091]  2015 Photographs
        • [5111503]  2016 Photographs
        • [5205186]  2017 Photographs
    
      Assigning roles...
        ✅ Assigned 'admin' to '@dmmaster' on collection '4040415'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4549804'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4550541'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4550543'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4550544'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4550546'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4550547'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4638448'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4880636'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4880646'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4883659'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4887813'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4892626'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4916701'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4929239'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4930201'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4936295'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4938341'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4946989'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4950812'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4953735'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4954152'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4968696'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4973279'
        ✅ Assigned 'admin' to '@dmmaster' on collection '4974444'
        ✅ Assigned 'admin' to '@dmmaster' on collection '5006091'
        ✅ Assigned 'admin' to '@dmmaster' on collection '5111503'
        ✅ Assigned 'admin' to '@dmmaster' on collection '5205186'
    
    ════════════════════════════════════════════════════════════
      ✅ Succeeded : 28
        • [4040415]  2012 Photographs
        • [4549804]  1995 Photographs
        • [4550541]  1996 Photographs
        • [4550543]  1997 Photographs
        • [4550544]  1998 Photographs
        • [4550546]  1999 Photographs
        • [4550547]  2000 Photographs
        • [4638448]  1993 Photographs
        • [4880636]  1985 Photographs
        • [4880646]  1986 Photographs
        • [4883659]  1987 Photographs
        • [4887813]  1988 Photographs
        • [4892626]  1989 Photographs
        • [4916701]  1990 Photographs
        • [4929239]  1991 Photographs
        • [4930201]  1992 Photographs
        • [4936295]  1994 Photographs
        • [4938341]  2004 Photographs
        • [4946989]  2007 Photographs
        • [4950812]  2008 Photographs
        • [4953735]  2009 Photographs
        • [4954152]  2010 Photographs
        • [4968696]  2011 Photographs
        • [4973279]  2013 Photographs
        • [4974444]  2014 Photographs
        • [5006091]  2015 Photographs
        • [5111503]  2016 Photographs
        • [5205186]  2017 Photographs
      ❌ Failed    : 0
    ════════════════════════════════════════════════════════════
    


### Request file DOIs for dataset


```python
import requests
import json
```


```python
#Note! In API guide, requests may follow ID or persistentId format. All endpoints can use either ID or persistentId according to the
#method described in the guide (typically ID) or the method used here: persistentId.

#Note 2! Version may or may not need to be specified, dpending on the endpoint. For published datasets, the default is "latestVersion."
#For drafts, you typically will need to indicate ":draft" as seen below. 

# Reuses the credentials loaded in the global-variables cell.
# NOTE: this previously hard-coded SERVER_URL to production
# (https://dataverse.harvard.edu) even when deposits ran against staging.
# It now follows DATAVERSE_TARGET; override here if you need to harvest DOIs
# from a different installation than you deposited to.
SERVER_URL = dv_installation_url
VERSION = ":draft"
API_KEY = dv_api_key
headers = {'X-Dataverse-key': API_KEY}
photos_dois = pd.DataFrame()
```


```python
def get_file_dois(
    dataset_dict,
    dataverse_url,
    api_token,
    version=":latest",
    output_csv_path=None,
    public_base_url="https://dataverse.harvard.edu",
):
    """
    Retrieve file-level DOIs for multiple datasets and build a
    consolidated DataFrame with direct download links.

    Parameters
    ----------
    dataset_dict : dict
        Keys are dataset titles (str), values are dataset DOIs (str).
        e.g. {"2022 Photographs": "doi:10.7910/DVN/28B3QA", ...}
    dataverse_url : str
        Base URL of the Dataverse installation (used for API calls).
    api_token : str
        Dataverse API token.
    version : str
        Dataset version to query. Defaults to ':latest'.
        Other options: ':latest-published', '1.0', '2.1', etc.
    output_csv_path : str or None
        Full path to write the output CSV. If None, CSV is not written.
    public_base_url : str
        Base URL used to construct direct download links.
        Defaults to 'https://dataverse.harvard.edu'.

    Returns
    -------
    pd.DataFrame
        Columns: filename, file_doi, dataset_name, dataset_doi,
                 direct_download_link
    """

    headers = {"X-Dataverse-key": api_token}
    all_rows = []
    failed   = {}

    print(f"📋 Fetching file DOIs for {len(dataset_dict)} dataset(s)...\n")

    for dataset_name, dataset_doi in dataset_dict.items():
        print(f"  📂 {dataset_name}  ({dataset_doi})")

        url = (
            f"{dataverse_url.rstrip('/')}/api/datasets/:persistentId"
            f"/versions/{version}/files?persistentId={dataset_doi}"
        )

        try:
            response = requests.get(url, headers=headers, verify=False, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"    ❌ HTTP error: {e}")
            print(f"       Response: {response.text[:300]}")
            failed[dataset_name] = str(e)
            continue
        except Exception as e:
            print(f"    ❌ Unexpected error: {e}")
            failed[dataset_name] = str(e)
            continue

        file_list = response.json().get("data", [])

        if not file_list:
            print(f"    ⚠️  No files returned for this dataset")
            continue

        dataset_rows = []
        missing_doi  = 0

        for file_entry in file_list:
            datafile = file_entry.get("dataFile", {})
            filename = datafile.get("filename")
            file_doi = datafile.get("persistentId")

            if not file_doi:
                missing_doi += 1

            # Build direct download link from file DOI
            direct_link = (
                f"{public_base_url.rstrip('/')}"
                f"/api/access/datafile/:persistentId"
                f"?persistentId={file_doi}"
                if file_doi else None
            )

            dataset_rows.append({
                "filename"             : filename,
                "file_doi"             : file_doi,
                "dataset_name"         : dataset_name,
                "dataset_doi"          : dataset_doi,
                "direct_download_link" : direct_link,
            })

        all_rows.extend(dataset_rows)

        print(f"    ✅ {len(dataset_rows)} files retrieved", end="")
        if missing_doi:
            print(f"  ⚠️  {missing_doi} file(s) had no DOI", end="")
        print()

    # ── Build final DataFrame ─────────────────────────────────────────────────
    df = pd.DataFrame(all_rows, columns=[
        "filename",
        "file_doi",
        "dataset_name",
        "dataset_doi",
        "direct_download_link",
    ])

    # ── Write CSV ─────────────────────────────────────────────────────────────
    if output_csv_path:
        df.to_csv(output_csv_path, index=False)
        print(f"\n  💾 CSV written → {output_csv_path}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'═' * 60}")
    print(f"  Total files      : {len(df)}")
    print(f"  Datasets queried : {len(dataset_dict)}")
    print(f"  Failed datasets  : {len(failed)}")
    if failed:
        for name, err in failed.items():
            print(f"    • {name}  —  {err}")
    print(f"{'═' * 60}")

    return df

```


```python
# ── Run it ────────────────────────────────────────────────────────────────────

# Build the input dict from your existing collection_pids checkpoint,
# or define it manually
dataset_doi_dict = {
    "2019 Photographs": "doi:10.7910/DVN/GC7VPA",
    "2022 Photographs": "doi:10.7910/DVN/I61NUB",
    "Leon Levy Expedition to Ashkelon Dataset: Videos": "doi:10.7910/DVN/PUPH8R",
    "Pottery Registry: Dataset 1": "doi:10.7910/DVN/AIZG6H",
    "Pottery Registry: Dataset 2": "doi:10.7910/DVN/WHN3EV",
    "Pottery Readings: Dataset 1": "doi:10.7910/DVN/2DRF2T",
    "Pottery Readings: Dataset 2": "doi:10.7910/DVN/LPMXIY",
    "Pottery Readings: Dataset 3": "doi:10.7910/DVN/FQ4IV2",
    "Pottery Readings: Dataset 4": "doi:10.7910/DVN/GGMWB6",
    "Pottery Readings: Dataset 5": "doi:10.7910/DVN/T5LVID",
    "Pottery Readings: Dataset 6": "doi:10.7910/DVN/KKNVNP",
    "Leon Levy Expedition to Ashkelon Dataset: Fieldbooks": "doi:10.7910/DVN/MV7O1B",
    "Leon Levy Expedition to Ashkelon Dataset: Material Culture Registry": "doi:10.7910/DVN/OD9HLS",
    "Leon Levy Expedition to Ashkelon Dataset: Field Reports": "doi:10.7910/DVN/OSFQA7"
}

# Or build it automatically from your collection_pids checkpoint:
# dataset_doi_dict = collection_pids  # keys = titles, values = DOIs

file_dois_df = get_file_dois(
    dataset_dict    = dataset_doi_dict,
    dataverse_url   = dv_installation_url,
    api_token       = dv_api_key,
    version         = ":latest",
    output_csv_path = os.path.join(source_path, "file_dois.csv"),
    public_base_url = "https://dataverse.harvard.edu",
)

file_dois_df.head()
```

    📋 Fetching file DOIs for 14 dataset(s)...
    
      📂 2019 Photographs  (doi:10.7910/DVN/GC7VPA)
        ✅ 197 files retrieved
      📂 2022 Photographs  (doi:10.7910/DVN/I61NUB)
        ✅ 946 files retrieved
      📂 Leon Levy Expedition to Ashkelon Dataset: Videos  (doi:10.7910/DVN/PUPH8R)
        ✅ 405 files retrieved
      📂 Pottery Registry: Dataset 1  (doi:10.7910/DVN/AIZG6H)
        ✅ 899 files retrieved
      📂 Pottery Registry: Dataset 2  (doi:10.7910/DVN/WHN3EV)
        ✅ 500 files retrieved
      📂 Pottery Readings: Dataset 1  (doi:10.7910/DVN/2DRF2T)
        ✅ 898 files retrieved
      📂 Pottery Readings: Dataset 2  (doi:10.7910/DVN/LPMXIY)
        ✅ 901 files retrieved
      📂 Pottery Readings: Dataset 3  (doi:10.7910/DVN/FQ4IV2)
        ✅ 900 files retrieved
      📂 Pottery Readings: Dataset 4  (doi:10.7910/DVN/GGMWB6)
        ✅ 900 files retrieved
      📂 Pottery Readings: Dataset 5  (doi:10.7910/DVN/T5LVID)
        ✅ 900 files retrieved
      📂 Pottery Readings: Dataset 6  (doi:10.7910/DVN/KKNVNP)
        ✅ 425 files retrieved
      📂 Leon Levy Expedition to Ashkelon Dataset: Fieldbooks  (doi:10.7910/DVN/MV7O1B)
        ✅ 343 files retrieved
      📂 Leon Levy Expedition to Ashkelon Dataset: Material Culture Registry  (doi:10.7910/DVN/OD9HLS)
        ✅ 37 files retrieved
      📂 Leon Levy Expedition to Ashkelon Dataset: Field Reports  (doi:10.7910/DVN/OSFQA7)
        ✅ 428 files retrieved
    
      💾 CSV written → /Users/katherinemika/Desktop/curation/leon_levy/summer2026/file_dois.csv
    
    ════════════════════════════════════════════════════════════
      Total files      : 8679
      Datasets queried : 14
      Failed datasets  : 0
    ════════════════════════════════════════════════════════════





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>filename</th>
      <th>file_doi</th>
      <th>dataset_name</th>
      <th>dataset_doi</th>
      <th>direct_download_link</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A19_33914.jpg</td>
      <td>doi:10.7910/DVN/GC7VPA/XT6P9S</td>
      <td>2019 Photographs</td>
      <td>doi:10.7910/DVN/GC7VPA</td>
      <td>https://dataverse.harvard.edu/api/access/dataf...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A19_33915.jpg</td>
      <td>doi:10.7910/DVN/GC7VPA/4W0A11</td>
      <td>2019 Photographs</td>
      <td>doi:10.7910/DVN/GC7VPA</td>
      <td>https://dataverse.harvard.edu/api/access/dataf...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A19_33916.jpg</td>
      <td>doi:10.7910/DVN/GC7VPA/CFVAXV</td>
      <td>2019 Photographs</td>
      <td>doi:10.7910/DVN/GC7VPA</td>
      <td>https://dataverse.harvard.edu/api/access/dataf...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>A19_33917.jpg</td>
      <td>doi:10.7910/DVN/GC7VPA/TN3TY4</td>
      <td>2019 Photographs</td>
      <td>doi:10.7910/DVN/GC7VPA</td>
      <td>https://dataverse.harvard.edu/api/access/dataf...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A19_33918.jpg</td>
      <td>doi:10.7910/DVN/GC7VPA/JLMI8H</td>
      <td>2019 Photographs</td>
      <td>doi:10.7910/DVN/GC7VPA</td>
      <td>https://dataverse.harvard.edu/api/access/dataf...</td>
    </tr>
  </tbody>
</table>
</div>




```python

```


```python

```


```python

```


```python
#get all files in SINGLE dataset
PID = "doi:10.7910/DVN/28B3QA" #assign this manually if only requesting DOIs for one dataset. 
url = "{}/api/datasets/:persistentId/versions/{}/files?persistentId={}".format(SERVER_URL, VERSION, PID)
response = requests.get(url, headers = headers)
datasets = response.json()
dataset_metadata = datasets['data']

#parse response to get dois
#metadata[0][0]['dataFile']['filename']

# List to accumulate rows
rows = []

# Loop through the nested list
for file in dataset_metadata:
    datafile = file.get('dataFile', {})
    filename = datafile.get('filename')
    doi = datafile.get('persistentId')
    #Append as a dict
    rows.append({'filename': filename, 'DOI': doi})

# Create DataFrame all at once & append to illustrations_dois
df = pd.DataFrame(rows)
photos_dois = pd.concat([photos_dois, df], ignore_index=True)
```


```python
photos_dois
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>filename</th>
      <th>DOI</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A85_184B.jpg</td>
      <td>doi:10.7910/DVN/0Z3FC1/QGJTAX</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A85_296b.jpg</td>
      <td>doi:10.7910/DVN/0Z3FC1/X1K79T</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A85_315b.jpg</td>
      <td>doi:10.7910/DVN/0Z3FC1/RT9A95</td>
    </tr>
    <tr>
      <th>3</th>
      <td>A85_315c.jpg</td>
      <td>doi:10.7910/DVN/0Z3FC1/POZ965</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A85_315d.jpg</td>
      <td>doi:10.7910/DVN/0Z3FC1/FPF7EG</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2743</th>
      <td>A19_33909.jpg</td>
      <td>doi:10.7910/DVN/28B3QA/5LM3T1</td>
    </tr>
    <tr>
      <th>2744</th>
      <td>A19_33910.jpg</td>
      <td>doi:10.7910/DVN/28B3QA/BHVFDI</td>
    </tr>
    <tr>
      <th>2745</th>
      <td>A19_33911.jpg</td>
      <td>doi:10.7910/DVN/28B3QA/DHOJI4</td>
    </tr>
    <tr>
      <th>2746</th>
      <td>A19_33912.jpg</td>
      <td>doi:10.7910/DVN/28B3QA/QO3OK2</td>
    </tr>
    <tr>
      <th>2747</th>
      <td>A19_33913.jpg</td>
      <td>doi:10.7910/DVN/28B3QA/FKN5VK</td>
    </tr>
  </tbody>
</table>
<p>2748 rows × 2 columns</p>
</div>




```python
photos_dois.to_csv("/Users/katherinemika/Desktop/curation/leon_levy/illustrations/photos_files_dois.csv", index = False)
```


```python
#get all PIDs in a collection 
url = "{}/api/dataverses/{}/contents".format(SERVER_URL, dv_collection)
response = requests.get(url, headers = headers)
datasets = response.json()
```


```python
dataset_dois = []
for item in datasets['data']:
    dataset_dois.append(f"doi:10.7910/{item['identifier']}")

dataset_dois
```




    ['doi:10.7910/DVN/9S3JTN',
     'doi:10.7910/DVN/FUJ1AC',
     'doi:10.7910/DVN/7LRAEQ',
     'doi:10.7910/DVN/AEUYOC',
     'doi:10.7910/DVN/Y23OH9',
     'doi:10.7910/DVN/ZK9VLW',
     'doi:10.7910/DVN/X1LU8T',
     'doi:10.7910/DVN/UCS72H',
     'doi:10.7910/DVN/ZGXU2M',
     'doi:10.7910/DVN/JGIF2H',
     'doi:10.7910/DVN/AD8LMX',
     'doi:10.7910/DVN/FUOGIZ',
     'doi:10.7910/DVN/2Y3PF3',
     'doi:10.7910/DVN/TC8GZQ',
     'doi:10.7910/DVN/BL4GMW',
     'doi:10.7910/DVN/7PPFLN',
     'doi:10.7910/DVN/JZFYR4',
     'doi:10.7910/DVN/IAHTDW',
     'doi:10.7910/DVN/HG8MYJ',
     'doi:10.7910/DVN/KEEUSH',
     'doi:10.7910/DVN/NOAFYQ',
     'doi:10.7910/DVN/8SGKFH',
     'doi:10.7910/DVN/BNLWJE',
     'doi:10.7910/DVN/VMC6XQ',
     'doi:10.7910/DVN/B8TRKI',
     'doi:10.7910/DVN/UIQOJU',
     'doi:10.7910/DVN/UGMBOQ',
     'doi:10.7910/DVN/9TICC3',
     'doi:10.7910/DVN/GBWVB3',
     'doi:10.7910/DVN/EHLYBV',
     'doi:10.7910/DVN/MQUIJX',
     'doi:10.7910/DVN/1ICKOL',
     'doi:10.7910/DVN/ZKIYFK',
     'doi:10.7910/DVN/QWSOUK',
     'doi:10.7910/DVN/IG9XJT',
     'doi:10.7910/DVN/AXEE57',
     'doi:10.7910/DVN/O86S7R',
     'doi:10.7910/DVN/0XY1V9',
     'doi:10.7910/DVN/4VSUKK',
     'doi:10.7910/DVN/4EIH9B',
     'doi:10.7910/DVN/ZYLBDU',
     'doi:10.7910/DVN/Y0MYUS',
     'doi:10.7910/DVN/RDXXTI',
     'doi:10.7910/DVN/5GSKRL',
     'doi:10.7910/DVN/RFU0A0',
     'doi:10.7910/DVN/4WYEBB',
     'doi:10.7910/DVN/NPFDGE',
     'doi:10.7910/DVN/P91VKE',
     'doi:10.7910/DVN/BNQO5G',
     'doi:10.7910/DVN/YEJ0WH',
     'doi:10.7910/DVN/QRWECE',
     'doi:10.7910/DVN/6XPWNF',
     'doi:10.7910/DVN/L3A0Q4',
     'doi:10.7910/DVN/DWMWNC',
     'doi:10.7910/DVN/YOFFKB',
     'doi:10.7910/DVN/MOQMTG',
     'doi:10.7910/DVN/X2IWBS',
     'doi:10.7910/DVN/JJLONA',
     'doi:10.7910/DVN/RQWR7T',
     'doi:10.7910/DVN/SWSHBI']




```python
#get files from dataset dois
dataset_metadata = []
for doi in dataset_dois:
    url = "{}/api/datasets/:persistentId/versions/{}/files?persistentId={}".format(SERVER_URL, VERSION, doi)
    response = requests.get(url, headers = headers)
    dataset = response.json()
    dataset_metadata.append(dataset['data'])
```


```python
#parse JSON output to access metadata fields for each file. 
dataset_metadata[0][0]['dataFile']['filename']

# List to accumulate rows
rows = []

# Loop through the nested list
for dataset in dataset_metadata:
    for file in dataset:
        datafile = file.get('dataFile', {})
        filename = datafile.get('filename')
        doi = datafile.get('persistentId')
        # Append as a dict
        rows.append({'filename': filename, 'DOI': doi})

# Create DataFrame all at once
df = pd.DataFrame(rows)
```


```python
illustrations_dois = df
illustrations_dois
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>filename</th>
      <th>DOI</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>dwgid0001.jpg</td>
      <td>doi:10.7910/DVN/9S3JTN/WYCYHI</td>
    </tr>
    <tr>
      <th>1</th>
      <td>dwgid0003.jpg</td>
      <td>doi:10.7910/DVN/9S3JTN/EVW4JB</td>
    </tr>
    <tr>
      <th>2</th>
      <td>dwgid0004.jpg</td>
      <td>doi:10.7910/DVN/9S3JTN/FK0F20</td>
    </tr>
    <tr>
      <th>3</th>
      <td>dwgid0005.jpg</td>
      <td>doi:10.7910/DVN/9S3JTN/GB0MSO</td>
    </tr>
    <tr>
      <th>4</th>
      <td>dwgid0006.jpg</td>
      <td>doi:10.7910/DVN/9S3JTN/TTDBY9</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>26372</th>
      <td>dwgid29339.jpg</td>
      <td>doi:10.7910/DVN/DWMWNC/NYTLYF</td>
    </tr>
    <tr>
      <th>26373</th>
      <td>dwgid29340.jpg</td>
      <td>doi:10.7910/DVN/DWMWNC/LSOSZA</td>
    </tr>
    <tr>
      <th>26374</th>
      <td>dwgid29341.jpg</td>
      <td>doi:10.7910/DVN/DWMWNC/T6EB1G</td>
    </tr>
    <tr>
      <th>26375</th>
      <td>dwgid29342.jpg</td>
      <td>doi:10.7910/DVN/DWMWNC/Z26COQ</td>
    </tr>
    <tr>
      <th>26376</th>
      <td>dwgid29343.jpg</td>
      <td>doi:10.7910/DVN/DWMWNC/JH2DGH</td>
    </tr>
  </tbody>
</table>
<p>26377 rows × 2 columns</p>
</div>




```python
illustrations_dois.to_csv("/Users/katherinemika/Desktop/curation/leon_levy/illustrations/illustrations_dois.csv", index = False)
```

## Add resolved images/files to existing datasets


```python
#import new spreadsheet
#make column for year based on filename
#get all dataset DOIs and add to DF as new column (dataset_DOI)
#for row in df: create file matedata payload and upload to dataset in dataset_doi column
```


```python
july25 = pd.read_csv("/Users/katherinemika/Desktop/curation/leon_levy/photos/remaining_photos/july_25.csv")

# create column for dataset based on year (filename prefix) 
july25['year'] = july25['filename'].str.split('_').str[0]
july25['year'] = july25['year'].str.extract(r'[a-zA-Z](\d{2})')

july25.reset_index(drop=True, inplace=True)

# function to convert two-digit year to four-digit year is from early in script
july25['year'] = july25['year'].apply(convert_year)
july25['year'] = july25['year'].fillna(0).astype(int)

july25
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ochre_metadata</th>
      <th>filename</th>
      <th>Dataset</th>
      <th>year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>http://pi.lib.uchicago.edu/1001/org/ochre/b119...</td>
      <td>A99_12624.jpg</td>
      <td>doi:10.7910/DVN/K4QL7B</td>
      <td>1999</td>
    </tr>
  </tbody>
</table>
</div>




```python
files_by_dataset = {}

# Group by dataset
for dataset, group in july25.groupby('Dataset'):
    files_list = [
        {
            'filepath': os.path.join(datafiles_path, row['filename']),
            'description': f"<a href='{row['ochre_metadata']}'>{row['ochre_metadata']}</a>"
        }
        for _, row in group.iterrows()
    ]
    files_by_dataset[dataset] = files_list

# The files_by_dataset dictionary now contains lists of files for each dataset
files_by_dataset['doi:10.7910/DVN/K4QL7B']
```




    [{'filepath': '/Users/katherinemika/Desktop/curation/leon_levy/photos/july25/A99_12624.jpg',
      'description': "<a href='http://pi.lib.uchicago.edu/1001/org/ochre/b1197a83-05a6-592b-ef8f-9a00d81fd3d0'>http://pi.lib.uchicago.edu/1001/org/ochre/b1197a83-05a6-592b-ef8f-9a00d81fd3d0</a>"}]




```python
unique_datasets = july25['Dataset'].unique().tolist()
unique_datasets
```




    ['doi:10.7910/DVN/K4QL7B']




```python
for doi in unique_datasets:
    print("Dataset: ",doi, files_by_dataset[doi])
```

    Dataset:  doi:10.7910/DVN/K4QL7B [{'filepath': '/Users/katherinemika/Desktop/curation/leon_levy/photos/july25/A99_12624.jpg', 'description': "<a href='http://pi.lib.uchicago.edu/1001/org/ochre/b1197a83-05a6-592b-ef8f-9a00d81fd3d0'>http://pi.lib.uchicago.edu/1001/org/ochre/b1197a83-05a6-592b-ef8f-9a00d81fd3d0</a>"}]



```python

```


```python

```


```python

```


```python

```

## Old code I don't want to delete


```python
#inital loop I used to deposit datasets from easydataverse to HDV
# deposit datasets to repo (all) - be sure to enable FILE DOIS if necessary. Code is in "dataverse-scripts" folder if necessary.
collection_pids = {}
for dataset_title, dataset in list(dv_dataset_info.items()): #add eg. "[1:]" if not first pass. This is for subsequent uploads when retrying
    #set license
    dataset.license = dataverse.licenses["CC BY-NC-ND 4.0"]
    #upload datasets and files
    pid = dataset.upload(dataverse_name = dv_collection, n_parallel=2)
    collection_pids[dataset_title] = pid
    
    # Wait 5 minutes before uploading the next dataset
    print(f"Uploaded '{dataset_title}', waiting 5 minutes before next upload...")
    time.sleep(300)  # 300 seconds = 5 minutes
```

## Troubleshooting

There's somthing funny about the metadata for the 1997 dataset that is causing the files to fail to register. Seems like the "file metadata" is the problem. Ideas for fixing
- save metadata to csv file to investigate
- reconfirm that all files in directory are not corrupted (or something)
- use the dvuploader to test file upload with no added metadata (eg. without ochre or tags)

**A:** There were some issues with filenames, so I flagged them. 


```python

```


```python

```


```python
for dataset_title, dataset in dv_dataset_info.items():
    file_inventory = g_datafile_metadata.get(dataset_title)
    # build path for each dataset dir
    dataset_dict = dataset.dataverse_dict()
    citation_fields = dataset_dict.get('datasetVersion', {}).get('metadataBlocks', {}).get('citation', {}).get('fields', [])
    for field in citation_fields:
        if field.get('typeName') == 'productionDate':
            # Extract and print the productionDate value
            production_date = field.get('value')
            dataset_files_path = os.path.join(datafiles_path, str(production_date))
            break
    # add files to dataset from inventory using dataset files path
    curate.add_files_to_dataset(dataset, file_inventory, dataset_files_path)
    title = dataset.citation.title
    rich.print(f"Added {len(dataset.files)} file to {title}")
```


```python
#test for one dataset
#dv_dataset_info['1991 Photographs']

test_file_inventory = g_datafile_metadata.get('1991 Photographs')
dataset_dict = dv_dataset_info['1991 Photographs'].dataverse_dict()
citation_fields = dataset_dict.get('datasetVersion', {}).get('metadataBlocks', {}).get('citation', {}).get('fields', [])
for field in citation_fields:
    if field.get('typeName') == 'productionDate':
        # Extract and print the productionDate value
        production_date = field.get('value')
        dataset_files_path = os.path.join(datafiles_path, str(production_date))
        break
curate.add_files_to_dataset(dv_dataset_info['1991 Photographs'], test_file_inventory, dataset_files_path)
title = dv_dataset_info['1991 Photographs'].citation.title
rich.print(f"Added {len(dv_dataset_info['1991 Photographs'].files)} file to {title}")
```


<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">Added <span style="color: #008080; text-decoration-color: #008080; font-weight: bold">50</span> file to <span style="color: #008080; text-decoration-color: #008080; font-weight: bold">1991</span> Photographs
</pre>




```python
test_file_inventory = g_datafile_metadata.get('1991 Photographs')
test_file_inventory
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>filename</th>
      <th>df_description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A91_4093.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A91_4209.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A91_4233a1.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>A91_4273.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A91_4304.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>A91_4305.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>A91_4306.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>A91_4307.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>A91_4308.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>A91_4309.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>10</th>
      <td>A91_4310.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>11</th>
      <td>A91_4311.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>12</th>
      <td>A91_4312.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>13</th>
      <td>A91_4312b.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>14</th>
      <td>A91_4313.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>15</th>
      <td>A91_4314.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>16</th>
      <td>A91_4315.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>17</th>
      <td>A91_4316.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>18</th>
      <td>A91_4317.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>19</th>
      <td>A91_4318.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>20</th>
      <td>A91_4319.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>21</th>
      <td>A91_4320.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>22</th>
      <td>A91_4321.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>23</th>
      <td>A91_4322.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>24</th>
      <td>A91_4323.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>25</th>
      <td>A91_4324.jpg</td>
      <td>OCHRE Link: &lt;a href=http://pi.lib.uchicago.edu...</td>
    </tr>
    <tr>
      <th>26</th>
      <td>A91_3929-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>27</th>
      <td>A91_3942b.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>28</th>
      <td>A91_3986-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>29</th>
      <td>A91_3993-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>30</th>
      <td>A91_3994-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>31</th>
      <td>A91_3995-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>32</th>
      <td>A91_3996-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>33</th>
      <td>A91_4038-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>34</th>
      <td>A91_4052.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>35</th>
      <td>A91_4076-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>36</th>
      <td>A91_4099-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>37</th>
      <td>A91_4151-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>38</th>
      <td>A91_4153-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>39</th>
      <td>A91_4161-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>40</th>
      <td>A91_4171b-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>41</th>
      <td>A91_4172-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>42</th>
      <td>A91_4205-p.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>43</th>
      <td>A91_4232.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>44</th>
      <td>A91_4234a.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>45</th>
      <td>A91_4234b.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>46</th>
      <td>A91_4237.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>47</th>
      <td>A91_4238.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>48</th>
      <td>A91_4240.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
    <tr>
      <th>49</th>
      <td>A91_4987.jpg</td>
      <td>OCHRE Link: &lt;a href=https://pi.lib.uchicago.ed...</td>
    </tr>
  </tbody>
</table>
</div>




```python
collection_pids = {}
pid = dv_dataset_info['1991 Photographs'].upload(dataverse_name = dv_collection)
collection_pids['1991 Photographs'] = pid
```

    Dataset with pid 'doi:10.7910/DVN/SVC2FI' created.
    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">╭───────────── <span style="font-weight: bold">DVUploader</span> ──────────────╮
│ Server: <span style="font-weight: bold">https://dataverse.harvard.edu</span> │
│ PID: <span style="font-weight: bold">doi:10.7910/DVN/SVC2FI</span>           │
│ Files: 50                             │
╰───────────────────────────────────────╯
</pre>




    Output()


    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>



    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🔎 Checking dataset files</span><span style="font-style: italic">            </span>
┏━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┓
┃<span style="font-weight: bold"> File            </span>┃<span style="font-weight: bold"> Status </span>┃<span style="font-weight: bold"> Action </span>┃
┡━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━┩
│<span style="color: #008080; text-decoration-color: #008080"> A91_4093.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4209.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4233a1.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4273.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4304.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4305.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4306.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4307.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4308.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4309.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4310.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4311.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4312.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4312b.jpg   </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4313.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4314.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4315.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4316.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4317.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4318.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4319.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4320.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4321.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4322.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4323.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4324.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_3929-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_3942b.jpg   </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_3986-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_3993-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_3994-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_3995-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_3996-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4038-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4052.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4076-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4099-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4151-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4153-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4161-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4171b-p.jpg </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4172-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4205-p.jpg  </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4232.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4234a.jpg   </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4234b.jpg   </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4237.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4238.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4240.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
│<span style="color: #008080; text-decoration-color: #008080"> A91_4987.jpg    </span>│ <span style="color: #00d75f; text-decoration-color: #00d75f">New</span>    │ <span style="color: #00d75f; text-decoration-color: #00d75f">Upload</span> │
└─────────────────┴────────┴────────┘
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">🚀 Uploading files</span>

</pre>




    Output()



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"></pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
</pre>




<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace">
<span style="color: #c0c0c0; text-decoration-color: #c0c0c0; font-weight: bold; font-style: italic">✅ Upload complete</span>

</pre>



    
    



<pre style="white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"><span style="color: #008000; text-decoration-color: #008000">╭─ Dataset URL ───────────────────────────────────────────────────────────────────────────────────────────────────╮</span>
<span style="color: #008000; text-decoration-color: #008000">│</span>                                                                                                                 <span style="color: #008000; text-decoration-color: #008000">│</span>
<span style="color: #008000; text-decoration-color: #008000">│</span>  🎉 https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/SVC2FI                             <span style="color: #008000; text-decoration-color: #008000">│</span>
<span style="color: #008000; text-decoration-color: #008000">│</span>                                                                                                                 <span style="color: #008000; text-decoration-color: #008000">│</span>
<span style="color: #008000; text-decoration-color: #008000">╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯</span>
</pre>




```python
#next steps: redetect file type; other QA?; publish? 
```


```python
# get all datasetIDs
headers = {'X-Dataverse-key': dv_api_key}
url = f"{dv_installation_url}/api/dataverses/{dv_collection}/contents"
response = requests.get(url, headers=headers)

dataset_ids = []
for r in response.json()['data']:
    dataset_ids.append(r['id'])
```


```python
dataset_ids
```




    [11194521]




```python
# get all fileIDs for redetect
file_ids = []

for dataset in dataset_ids: 
    headers = {'X-Dataverse-key': dv_api_key}
    url = f"{dv_installation_url}/api/datasets/{dataset}/versions/:draft/files"
    response = requests.get(url, headers = headers)
    for r in response.json()['data']:
        file_ids.append(r['dataFile']['id'])
```


```python
file_ids
```




    [11194568,
     11194537,
     11194570,
     11194551,
     11194527,
     11194536,
     11194535,
     11194572,
     11194565,
     11194566,
     11194545,
     11194531,
     11194557,
     11194553,
     11194548,
     11194555,
     11194560,
     11194563,
     11194528,
     11194529,
     11194558,
     11194573,
     11194546,
     11194571,
     11194562,
     11194564,
     11194549,
     11194547,
     11194554,
     11194524,
     11194569,
     11194543,
     11194561,
     11194552,
     11194556,
     11194539,
     11194532,
     11194550,
     11194567,
     11194525,
     11194538,
     11194544,
     11194533,
     11194559,
     11194540,
     11194530,
     11194526,
     11194542,
     11194534,
     11194541]




```python
# redetect files
for id in file_ids:
    headers = {'X-Dataverse-key': dv_api_key}
    url = f"{dv_installation_url}/api/files/{id}/redetect"
    response = requests.post(url, headers=headers)
    print(response.json())
    time.sleep(2)
```

    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}
    {'status': 'OK', 'data': {'dryRun': False, 'oldContentType': 'text/plain', 'newContentType': 'image/jpeg'}}



```python

```

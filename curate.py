""""
Dataverse Curation Functions Module

Functions supporting the processing of files associated with metadata transformation, dataset curation, and upload of datafiles. 
"""
import json
import numpy as np
import pandas as pd
import rich
from pyDataverse.models import Dataset
import requests
import dvuploader as dv
import time
import os

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
    production_date = inventory.at[index, 'prod_date']

    # Build the dataset metadata dictionary
    dataset_metadata = {
        'title': dataset_title,
        'author': [{'authorName': author_name1, 'authorAffiliation': affiliation1},
                   {'authorName': author_name2, 'authorAffiliation': affiliation2}],
        'description': [{'dsDescriptionValue': description}],
        'contact': [{'datasetContactName': contactName, 'datasetContactAffiliation': affiliation1, 'datasetContactEmail': contactEmail}],
        'subject': [subject],
        'license': 'CC0 1.0',
        'keywords': kws,
        'funding': funding,
        'productionDate': production_date,
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
    
    production_date = str(dataset_metadata.get('productionDate'))
    ds.citation.production_date = production_date


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

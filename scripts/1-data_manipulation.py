import pandas as pd
import re


def fix_encoding(text):
    if isinstance(text, str):
        return text.encode('ISO-8859-1').decode('utf-8')
    return text


def standardize_address_extended(address):
    """
    Standardizes various components of an address string to a consistent format.

    This function takes an address string and replaces certain address components
    with their standardized abbreviations. It handles common street types such as
    "Avenue", "Street", "Highway", "Road", "Boulevard", "Drive", and "Parkway",
    converting them to their respective abbreviations like "Ave", "St", "Hwy",
    "Rd", "Blvd", "Dr", and "Pkwy". The function ignores case and matches both
    with and without trailing periods.

    Parameters:
    address (str): The address string to be standardized.

    Returns:
    str: The standardized address string with the appropriate components replaced.
    """
    # Extend the patterns to include more address components
    patterns_extended = {
        r'\bAve\b\.?': 'Ave',      # Matches "Ave" and "Ave." with "Ave"
        r'\bAvenue\b': 'Ave',      # Matches "Avenue" with "Ave"
        r'\bAv\b\.?': 'Ave',       # Matches "Av" and "Av." with "Ave"
        r'\bStreet\b': 'St',       # Matches "Street" with "St"
        r'\bSt\b\.?': 'St',        # Matches "St" and "St." with "St"
        r'\bHighway\b': 'Hwy',     # Matches "Highway" with "Hwy"
        r'\bHwy\b\.?': 'Hwy',      # Matches "Hwy" and "Hwy." with "Hwy"
        r'\bRoad\b': 'Rd',         # Matches "Road" with "Rd"
        r'\bRd\b\.?': 'Rd',        # Matches "Rd" and "Rd." with "Rd"
        r'\bBoulevard\b': 'Blvd',  # Matches "Boulevard" with "Blvd"
        r'\bBlvd\b\.?': 'Blvd',    # Matches "Blvd" and "Blvd." with "Blvd"
        r'\bBl\b\.?': 'Blvd',      # Matches "Bl" and "Bl." with "Blvd"
        r'\bDrive\b': 'Dr',        # Matches "Drive" with "Dr"
        r'\bDr\b\.?': 'Dr',        # Matches "Dr" and "Dr." with "Dr"
        r'\bParkway\b': 'Pkwy',    # Matches "Parkway" with "Pkwy"
    }

    # Ensure the address is title-cased
    address = address.title()

    # Do not capitalize "and" if it is a standalone word
    address = re.sub(r'\bAnd\b', 'and', address)

    # Do not capitalize an 's' following an apostrophe
    address = re.sub(r"\'S\b", "'s", address)

    # Iterate over the patterns and apply the replacements
    for pattern, replacement in patterns_extended.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)

    # Fix the capitalization issue for ordinal numbers
    address = re.sub(r'(\d+)([A-Za-z]+)', lambda x: x.group(1) + x.group(2).lower(), address)

    return address


def filter_empty_datasets(grouping):
    """
        Filters a DataFrame group to check if it contains data in "Total Site Energy Usage (kBtu)" column.

        Parameters:
        group (DataFrame): A DataFrame group by BERDO ID.

        Returns:
        DataFrame: The same group if it meets the criteria (contains data in desired column),
                   otherwise returns None.
        """
    if grouping['Total Site Energy Usage (kBtu)'].dropna().empty:
        return None
    else:
        return grouping


def check_for_duplicates(df, column_name):
    """
    Checks for duplicate values in the specified column of the DataFrame
    and prints the unique duplicated values.

    Parameters:
    df (pd.DataFrame): The DataFrame to check for duplicates.
    column_name (str): The name of the column to check for duplicates.

    Returns:
    list: A list of unique duplicated values.
    """
    duplicate_mask = df[column_name].duplicated(keep=False)
    duplicate_values = df[column_name][duplicate_mask].unique()
    print(f"\nDuplicated {column_name} values:")
    for value in duplicate_values:
        print(value)
    return list(duplicate_values)


# File paths to BERDO Data 2021-2023
file_path_2022 = '../data-files/0-berdo-raw-data-2022.csv'
file_path_2023 = '../data-files/0-berdo-raw-data-2023.csv'

# Load file paths and create dataframes
df_2022 = pd.read_csv(file_path_2022, encoding='ISO-8859-1', skipinitialspace=True)
df_2023 = pd.read_csv(file_path_2023, encoding='ISO-8859-1', skipinitialspace=True)

# Clean columns of leading characters
df_2022.columns = df_2022.columns.str.strip().str.replace('\n', '').str.replace('\r', '')
df_2023.columns = df_2023.columns.str.strip().str.replace('\n', '').str.replace('\r', '')

# Columns to remove from data
columns_to_drop_2023 = ['Building Address City', 'Parcel Address City', 'Reported Enclosed Parking Area (Sq Ft)',
                        'Energy Star Score', 'Compliance Status', 'BERDO Report Status',
                        'Community Choice Electricity Participation',
                        'Renewable Energy Purchased through a Power Purchase Agreement (PPA)',
                        'Renewable Energy Certificate (REC) Purchase', 'Backup Generator', 'Battery Storage',
                        'Electric Vehicle (EV) Charging', 'Notes']

columns_to_drop_2022 = ['Building Address City', 'Parcel Address City', 'Reported Enclosed Parking Area (Sq Ft)',
                        'Energy Star Score', 'Compliance Status', 'BERDO Reporting Status',
                        'Community Choice Electricity Participation',
                        'Renewable Energy Purchased through a Power Purchase Agreement (PPA)',
                        'Renewable Energy Certificate (REC) Purchase', 'Backup Generator', 'Battery Storage',
                        'Electric Vehicle (EV) Charging', 'Notes']

# Two DataFrames with 2022 and 2023 data without unnecessary columns
df_with_drop_2023 = df_2023.drop(columns_to_drop_2023, axis=1)
df_with_drop_2022 = df_2022.drop(columns_to_drop_2022, axis=1)

# Remove column for water data that is unable to be removed due to decoding
column_index = 25
df_with_drop_2022 = df_with_drop_2022.drop(df_with_drop_2022.columns[column_index], axis=1)
df_with_drop_2023 = df_with_drop_2023.drop(df_with_drop_2023.columns[column_index], axis=1)

# Fill empty cells in Building Address column with an empty string
df_with_drop_2022['Building Address'] = df_with_drop_2022['Building Address'].fillna('')
df_with_drop_2023['Building Address'] = df_with_drop_2023['Building Address'].fillna('')

# Fill empty cells in Parcel Address column with an empty
df_with_drop_2023['Parcel Address'] = df_with_drop_2023['Parcel Address'].fillna('')
df_with_drop_2022['Parcel Address'] = df_with_drop_2022['Parcel Address'].fillna('')

# Convert columns to string dtype
df_with_drop_2023['Building Address'] = df_with_drop_2023['Building Address'].astype(str)
df_with_drop_2023['Parcel Address'] = df_with_drop_2023['Parcel Address'].astype(str)

df_with_drop_2022['Building Address'] = df_with_drop_2022['Building Address'].astype(str)
df_with_drop_2022['Parcel Address'] = df_with_drop_2022['Parcel Address'].astype(str)



# Create a copy of the data to avoid unexpected data issues later on
df_clean_2023 = df_with_drop_2023.copy()
df_clean_2022 = df_with_drop_2022.copy()

# Apply the extended standardization function to the Building Address column
df_clean_2023.loc[:, 'Building Address'] = df_clean_2023['Building Address'].apply(standardize_address_extended)
df_clean_2022.loc[:, 'Building Address'] = df_clean_2022['Building Address'].apply(standardize_address_extended)

# Apply the extended standardization function to the Parcel Address column
df_clean_2023.loc[:, 'Parcel Address'] = df_clean_2023['Parcel Address'].apply(standardize_address_extended)
df_clean_2022.loc[:, 'Parcel Address'] = df_clean_2022['Parcel Address'].apply(standardize_address_extended)


# Ensure that BERDO IDs are in ascending order by sorting then groupby BERDO ID
df_grouped_2023 = df_clean_2023.sort_values(by='BERDO ID', ascending=True).groupby('BERDO ID')
# 2022 data does NOT need to be grouped because it does not require aggregation at this time
df_clean_2022 = df_clean_2022.sort_values(by='BERDO ID', ascending=True)

# Initialize an empty list to store both the filtered data with and without reported energy usage
berdo_reported_list_2023 = []
berdo_not_reported_list_2023 = []

# Iterate over Site Energy Usage column to determine if property was reported on in 2023
for name, group in df_grouped_2023:
    filtered_group = filter_empty_datasets(group)
    if filtered_group is not None:
        berdo_reported_list_2023.append(filtered_group)
    else:
        berdo_not_reported_list_2023.append(group)

# Creat DataFrame for the two lists created for BERDO data that was or was not reported in 2023
df_berdo_reported_2023 = pd.concat(berdo_reported_list_2023)
df_berdo_not_reported_2023 = pd.concat(berdo_not_reported_list_2023)

# Add a Data Year column to df_berdo_reported and make it 2023
df_berdo_reported_2023['Data Year'] = 2022

# Take not reported data from 2023 and join those BERDO IDs on the 2022 data
df_berdo_2022 = df_berdo_not_reported_2023[['BERDO ID']].merge(df_clean_2022, how='inner')

# Find the BERDO IDs that were never reported in either dataset
merged_ids = df_berdo_2022['BERDO ID'].unique()
df_berdo_never_reported_1 = df_berdo_not_reported_2023[~df_berdo_not_reported_2023['BERDO ID'].isin(merged_ids)]

# Group 2022 data so that it can be iterated over for separation
df_grouped_2022 = df_berdo_2022.sort_values(by='BERDO ID', ascending=True).groupby('BERDO ID')

# Initialize an empty list to store both the filtered data with and without reported energy usage
berdo_reported_list_2022 = []
berdo_not_reported_list_2022 = []

# Iterate over Site Energy Usage column to determine if property was reported on in 2023
for name, group in df_grouped_2022:
    filtered_group = filter_empty_datasets(group)
    if filtered_group is not None:
        berdo_reported_list_2022.append(filtered_group)
    else:
        berdo_not_reported_list_2022.append(group)

# Creat DataFrame for the two lists created for BERDO data that was or was not reported in 2022
df_berdo_reported_2022 = pd.concat(berdo_reported_list_2022)
df_berdo_not_reported_2022 = pd.concat(berdo_not_reported_list_2022)

# Add a Data Year column to df_berdo_reported and make it 2021
df_berdo_reported_2022['Data Year'] = 2021

# Concatenate the data that was reported in neither 2022 nor 2023
df_berdo_never_reported = pd.concat([df_berdo_never_reported_1, df_berdo_not_reported_2022], axis=0)
df_berdo_never_reported = df_berdo_never_reported.sort_values(by='BERDO ID', ascending=True)

# Add indexes to the reported BERDO data so that they can be sorted to remove duplicate values
df_berdo_reported_2022['_id'] = df_berdo_reported_2022.index
df_berdo_reported_2023['_id'] = df_berdo_reported_2023.index

# # Get rid of all duplicate BERDO IDs in the 2023 datasets
# Gets rid of duplicate condition 500 Walk Hill St
df_berdo_reported_2023 = df_berdo_reported_2023[df_berdo_reported_2023['_id'] != 2506]

# Corrects the BERDO ID for Tudor Place & adds necessary data
df_berdo_reported_2023.loc[df_berdo_reported_2023['_id'] == 5958, 'BERDO ID'] = 106338
df_berdo_reported_2023.loc[df_berdo_reported_2023['_id'] == 5958, 'Property Owner Name'] = 'TUDOR PLACE CONDO TRUST'

# Add property owner name to BERDO ID 105780 and delete the unneeded row
df_berdo_reported_2023.loc[df_berdo_reported_2023['_id'] == 5948, 'Property Owner Name'] = 'SIMMONS UNIVERSITY'
df_berdo_reported_2023 = df_berdo_reported_2023[df_berdo_reported_2023['_id'] != 2733]

# Add blank columns to 154 Ruskindale Rd and delete incomplete column
ruskindale_new_values = {
    'Property Owner Name': 'CITY OF BOSTON',
    'Tax Parcel ID': 1803703000,
    'Building Address': '154-164 Ruskindale Rd',
    'Parcel Address': '154 156 ruskindale rd',
    'Parcel Address Zip Code': 2136
}

df_berdo_reported_2023.loc[df_berdo_reported_2023['_id'] == 5948, ['Property Owner Name', 'Tax Parcel ID',
                                                                   'Building Address', 'Parcel Address',
                                                                   'Parcel Address Zip Code']]\
                                                                    = ruskindale_new_values.values()

df_berdo_reported_2023 = df_berdo_reported_2023[df_berdo_reported_2023['_id'] != 638]

# Drop column for 150 Mass Ave that appears to be a single building in a campus
df_berdo_reported_2023 = df_berdo_reported_2023[df_berdo_reported_2023['_id'] != 5917]

# # Get rid of all duplicate BERDO IDs in the 2022 datasets
# Get rid of duplicates for 1910-1920 Centre St
df_berdo_reported_2022 = df_berdo_reported_2022[df_berdo_reported_2022['_id'] != 1901]

# Get rid of duplicates for 52-68 Highland Park
df_berdo_reported_2022 = df_berdo_reported_2022[df_berdo_reported_2022['_id'] != 2312]

# Get rid of duplicates for 3033-3039 Washington St
df_berdo_reported_2023 = df_berdo_reported_2023[df_berdo_reported_2023['_id'] != 830]

# Add GFA for BERDO ID 104612
df_berdo_reported_2023.loc[df_berdo_reported_2023['_id'] == 2708, 'Reported Gross Floor Area (Sq Ft)'] = 13440

# Check number of reported and non-reported data so that the total matches the total number of properties
print(df_clean_2023['BERDO ID'].nunique())
print(df_clean_2023.shape)
print(df_berdo_reported_2023['BERDO ID'].nunique())
print(df_berdo_reported_2023.shape)
print(df_berdo_reported_2022['BERDO ID'].nunique())
print(df_berdo_reported_2022.shape)
print(df_berdo_never_reported['BERDO ID'].nunique())
print(df_berdo_never_reported.shape)


# Remove unique identifiers used for filtering duplicates
df_berdo_reported_2022.drop(['_id', 'Unnamed: 40'], axis=1, inplace=True)
df_berdo_reported_2023.drop('_id', axis=1, inplace=True)

# Replace Site EUI column in 2023 data with a propertly encoded one
column_index_EUI = 10
df_berdo_reported_2023['Site EUI (Energy Use Intensity kBtu/ft2)'] = df_berdo_reported_2023.iloc[:, column_index_EUI]
df_berdo_reported_2023.drop(df_berdo_reported_2023.columns[column_index_EUI], axis=1, inplace=True)

# # Add leading zero to zip codes
# df_berdo_reported_2022['Building Address Zip Code'] = df_berdo_reported_2022['Building Address Zip Code'].astype(int)
# df_berdo_reported_2022['Parcel Address Zip Code'] = df_berdo_reported_2022['Parcel Address Zip Code'].astype(int)
#
# df_berdo_reported_2023['Building Address Zip Code'] = df_berdo_reported_2023['Building Address Zip Code'].astype(int)
# df_berdo_reported_2023['Parcel Address Zip Code'] = df_berdo_reported_2023['Parcel Address Zip Code'].astype(int)

# # Download data into separate .csv files
# df_berdo_reported_2022.to_csv('../data-files/2-berdo_reported_2022.csv', index=False)
# df_berdo_reported_2023.to_csv('../data-files/2-berdo_reported_2023.csv', index=False)
# df_berdo_never_reported.to_csv('../data-files/2-berdo_never_reported.csv', index=False)

# # Checking for duplicate values in dataset
# duplicate_ids_2022 = check_for_duplicates(df_berdo_reported_2022, 'BERDO ID')
# duplicate_ids_2023 = check_for_duplicates(df_berdo_reported_2023, 'BERDO ID')

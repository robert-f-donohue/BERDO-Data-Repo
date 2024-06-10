import pandas as pd
import re


def fix_encoding(text):
    if isinstance(text, str):
        return text.encode('ISO-8859-1').decode('utf-8')
    return text


def standardize_address_extended(address):
    '''Input: list of addresses and standardizes the address formatting
    Output: Standardized address formatted for use in the application'''

    # Extend the patterns to include more address components
    patterns_extended = {
        r'\bAve\b\.?': 'Ave',  # Matches "Ave" and "Ave." with "Ave"
        r'\bAvenue\b': 'Ave',  # Matches "Avenue" with "Ave"
        r'\bStreet\b': 'St',   # Matches "Street" with "St"
        r'\bSt\b\.?': 'St',    # Matches "St" and "St." with "St"
        r'\bHighway\b': 'Hwy', # Matches "Highway" with "Hwy"
        r'\bHwy\b\.?': 'Hwy',  # Matches "Hwy" and "Hwy." with "Hwy"
        r'\bRoad\b': 'Rd',     # Matches "Road" with "Rd"
        r'\bRd\b\.?': 'Rd',    # Matches "Rd" and "Rd." with "Rd"
        r'\bBoulevard\b': 'Blvd', # Matches "Boulevard" with "Blvd"
        r'\bBlvd\b\.?': 'Blvd',   # Matches "Blvd" and "Blvd." with "Blvd"
        r'\bDrive\b': 'Dr',    # Matches "Drive" with "Dr"
        r'\bDr\b\.?': 'Dr',    # Matches "Dr" and "Dr." with "Dr"
        r'\bParkway\b': 'Pkwy',
        # Add more patterns as needed
    }
    # Iterate over the patterns and apply the replacements
    for pattern, replacement in patterns_extended.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)

    return address


def filter_empty_datasets(group):
    """
        Filters a DataFrame group to check if it contains data in "Total Site Energy Usage (kBtu)" column.

        Parameters:
        group (DataFrame): A DataFrame group by BERDO ID.

        Returns:
        DataFrame: The same group if it meets the criteria (contains data in desired column),
                   otherwise returns None.
        """
    if group['Total Site Energy Usage (kBtu)'].dropna().empty:
        return None
    else:
        return group


# File paths to BERDO Data 2021-2023
file_path_2022 = '../data-files/0-berdo-raw-data-2022.csv'
file_path_2023 = '../data-files/0-berdo-raw-data-2023.csv'

# Load file paths and create dataframes
df_2022 = pd.read_csv(file_path_2022, encoding='ISO-8859-1', skipinitialspace=True)
df_2023 = pd.read_csv(file_path_2023, encoding='ISO-8859-1', skipinitialspace=True)



# Clean columns of leading characters
df_2022.columns = df_2022.columns.str.strip().str.replace('\n', '').str.replace('\r', '')
df_2023.columns = df_2023.columns.str.strip().str.replace('\n', '').str.replace('\r', '')

# df_2023.to_csv('../data-files/test.csv', index=False)

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

# Fill empty cells in Building Address column with an empty string
df_with_drop_2022['Building Address'] = df_with_drop_2022['Building Address'].fillna('')
df_with_drop_2023['Building Address'] = df_with_drop_2023['Building Address'].fillna('')

# Fill empty cells in Parcel Address column with an empty
df_with_drop_2023['Parcel Address'] = df_with_drop_2023['Parcel Address'].fillna('')
df_with_drop_2022['Parcel Address'] = df_with_drop_2022['Parcel Address'].fillna('')

# Convert columns to string dtype
df_with_drop_2023['Building Address'] = df_with_drop_2023['Building Address'].astype('string')
df_with_drop_2023['Parcel Address'] = df_with_drop_2023['Parcel Address'].astype('string')

df_with_drop_2022['Building Address'] = df_with_drop_2022['Building Address'].astype('string')
df_with_drop_2022['Parcel Address'] = df_with_drop_2022['Parcel Address'].astype('string')

# Change df Building Address column to Title Case
df_with_drop_2023['Building Address'] = df_with_drop_2023['Building Address'].str.title()
df_with_drop_2022['Building Address'] = df_with_drop_2022['Building Address'].str.title()

# Change df Parcel Address column to Title Case
df_with_drop_2023['Parcel Address'] = df_with_drop_2023['Parcel Address'].str.title()
df_with_drop_2022['Parcel Address'] = df_with_drop_2022['Parcel Address'].str.title()


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
df_berdo_reported_2023['Data Year'] = 2023

# Take not reported data from 2023 and join those BERDO IDs on the 2022 data
df_berdo_2022 = df_berdo_not_reported_2023[['BERDO ID']].merge(df_clean_2022, how='inner')

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

# Add a Data Year column to df_berdo_reported and make it 2023
df_berdo_reported_2022['Data Year'] = 2022

# Check number of reported and non-reported data so that the total matches the total number of properties
print(df_clean_2023['BERDO ID'].nunique())
print(df_clean_2023.shape)
print(df_berdo_reported_2023['BERDO ID'].nunique())
print(df_berdo_reported_2023.shape)
print(df_berdo_not_reported_2023['BERDO ID'].nunique())
print(df_berdo_not_reported_2023.shape)
print(df_berdo_reported_2022['BERDO ID'].nunique())
print(df_berdo_reported_2022.shape)
print(df_berdo_not_reported_2022['BERDO ID'].nunique())
print(df_berdo_not_reported_2022.shape)

# # Download data into separate .csv files
# df_berdo_reported_2023.to_csv('../data-files/2-berdo_reported_2023.csv', index=False)
# df_berdo_not_reported_2023.to_csv('../data-files/3-berdo_not_reported_2023.csv', index=False)

duplicate_mask_2023 = df_clean_2023['BERDO ID'].duplicated(keep=False)

duplicate_values = df_clean_2023['BERDO ID'][duplicate_mask_2023].unique()
print("\nDuplicated BERDO IDs column 2023:")
for value in duplicate_values:
    print(value)


# # Checking for duplicate values in dataset
duplicate_mask = df_berdo_2022['BERDO ID'].duplicated(keep=False)

duplicate_values = df_berdo_2022['BERDO ID'][duplicate_mask].unique()
print("\nDuplicated BERDO IDs column 2022:")
for value in duplicate_values:
    print(value)



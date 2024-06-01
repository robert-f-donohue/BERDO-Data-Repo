import pandas as pd
import re


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


# Upload CSV
file_path = '../data-files/0-berdo-raw-data-2022.csv'
df = pd.read_csv(file_path)

# Columns to remove from data
columns_to_drop = ['Building Address City', 'Parcel Address City', 'Reported Enclosed Parking Area (Sq Ft)',
                   'Energy Star Score', 'Water Usage Intensity (Gallons/ft2)', 'Compliance Status',
                   'BERDO Report Status', 'Community Choice Electricity Participation',
                   'Renewable Energy Purchased through a Power Purchase Agreement (',
                   'Renewable Energy Certificate (REC) Purchase', 'Backup Generator', 'Battery Storage',
                   'Electric Vehicle (EV) Charging', 'Cooresponding Campus ID', 'Notes']

df_with_drop = df.drop(columns_to_drop, axis=1)

# Fill empty cells with an empty string
df_with_drop['Building Address'] = df_with_drop['Building Address'].fillna('')
df_with_drop['Parcel Address'] = df_with_drop['Parcel Address'].fillna('')

# Convert columns to string dtype
df_with_drop['Building Address'] = df_with_drop['Building Address'].astype('string')
df_with_drop['Parcel Address'] = df_with_drop['Parcel Address'].astype('string')


# Change df Building Address column to Title Case
df_with_drop['Building Address'] = df_with_drop['Building Address'].str.title()
# Change df Parcel Address column to Title Case
df_with_drop['Parcel Address'] = df_with_drop['Parcel Address'].str.title()

# Create a copy of the data to avoid unexpected data issues later on
df_clean = df_with_drop.copy()

# Apply the extended standardization function to the Building Address column
df_clean.loc[:, 'Building Address'] = df_clean['Building Address'].apply(standardize_address_extended)

# Apply the extended standardization function to the Parcel Address column
df_clean.loc[:, 'Parcel Address'] = df_clean['Parcel Address'].apply(standardize_address_extended)

# Ensure that BERDO IDs are in ascending order by sorting then groupby BERDO ID
df_clean = df_clean.sort_values(by='BERDO ID', ascending=True).groupby('BERDO ID')

# Initialize an empty list to store both the filtered data with and without reported energy usage
berdo_reported_list = []
berdo_not_reported_list = []

# Iterate over Site Energy Usage column to determine if property was reported on in 2022
for name, group in df_clean:
    filtered_group = filter_empty_datasets(group)
    if filtered_group is not None:
        berdo_reported_list.append(filtered_group)
    else:
        berdo_not_reported_list.append(group)

# Creat DataFrame for the two lists created for BERDO data that was or was not reported in 2022
df_berdo_reported = pd.concat(berdo_reported_list)
df_berdo_not_reported = pd.concat(berdo_not_reported_list)

# Gets rid of duplicate condition 500 Walk Hill St
index_to_drop_Walk_Hill = df_berdo_reported[df_berdo_reported['_id'] == 2508].index
df_berdo_reported = df_berdo_reported.drop(index_to_drop_Walk_Hill)

# Corrects the BERDO ID for Tudor Place & Adds
df_berdo_reported.loc[df_berdo_reported['_id'] == 5959, 'BERDO ID'] = 106338
df_berdo_reported.loc[df_berdo_reported['_id'] == 5959, 'Property Owner Name'] = 'TUDOR PLACE CONDO TRUST'

# Add property owner name to BERDO ID 105780 and delete the unneeded row
df_berdo_reported.loc[df_berdo_reported['_id'] == 5949, 'Property Owner Name'] = 'SIMMONS UNIVERSITY'
index_to_drop_Simmons = df_berdo_reported[df_berdo_reported['_id'] == 2734].index
df_berdo_reported = df_berdo_reported.drop(index_to_drop_Simmons)

# Add blank columns to 154 Ruskindale Rd and delete incomplete column
ruskindale_new_values = {
    'Property Owner Name': 'CITY OF BOSTON',
    'Tax Parcel ID': 1803703000,
    'Building Address': '154-164 Ruskindale Rd',
    'Parcel Address': '154 156 ruskindale rd',
    'Parcel Address Zip Code': 2136
}

df_berdo_reported.loc[df_berdo_reported['_id'] == 5934, ['Property Owner Name', 'Tax Parcel ID', 'Building Address',
                                                         'Parcel Address', 'Parcel Address Zip Code']]\
                                                        = ruskindale_new_values.values()

index_to_drop_Ruskindale = df_berdo_reported[df_berdo_reported['_id'] == 639].index
df_berdo_reported = df_berdo_reported.drop(index_to_drop_Ruskindale)

# Drop column for 150 Mass Ave that appears to be a single building in a campus
index_to_drop_Berklee = df_berdo_reported[df_berdo_reported['_id'] == 5918].index
df_berdo_reported = df_berdo_reported.drop(index_to_drop_Berklee)

# Download data into separate .csv files
df_berdo_reported.to_csv('../data-files/berdo_reported.csv', index=False)
df_berdo_not_reported.to_csv('../data-files/berdo_not_reported.csv', index=False)

# Checking for duplicate values in dataset
# duplicate_mask = df_berdo_reported['BERDO ID'].duplicated(keep=False)
#
# duplicate_values = df_berdo_reported['BERDO ID'][duplicate_mask].unique()
# print("\nDuplicated BERDO IDs column:")
# for value in duplicate_values:
#     print(value)

# Check
# print(df_clean['BERDO ID'].nunique())
# print(df_berdo_reported['BERDO ID'].nunique())
# print(df_berdo_not_reported['BERDO ID'].nunique())

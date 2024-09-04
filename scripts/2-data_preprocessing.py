import pandas as pd
import re


def clean_cell(cell):
    """
    Cleans a cell by stripping whitespace, removing occurrences of "Parking", and handling specific cases.

    Args:
        cell (str): The cell content to clean.

    Returns:
        int or str: The cleaned cell content, converted to an integer if possible,
                    or "Storage" if the cell content is "Parking", or the original cleaned string.
    """
    # Strip white space
    cell = cell.strip()
    # Replace cell with 0 if it is just a comma
    if cell == ',':
        return 0
    if cell == 'nan':
        return 0
    if cell == 'Parking':
        return 'Storage'
    # Remove any occurrence of "Parking" (case-insensitive) surrounded by commas
    cell = re.sub(r'\s*,?\s*parking\s*,?\s*', ',', cell, flags=re.IGNORECASE).strip(',')
    # Convert to integer if possible
    try:
        return int(cell)
    except ValueError:
        return cell


def is_integer(value):
    """
    Checks if a value can be converted to an integer.

    Args:
        value: The value to check.

    Returns:
        bool: True if the value can be converted to an integer, False otherwise.
    """
    try:
        int(value)
        return True
    except ValueError:
        return False


# Function to add leading zero to zip codes
def add_leading_zero(zip_code):
    """
    Ensures zip codes have a leading zero if they are 4 digits long.

    Args:
        zip_code (int or str): The zip code to format.

    Returns:
        str: The formatted zip code with a leading zero if needed.
    """
    zip_code_str = str(zip_code)
    if len(zip_code_str) == 4:
        zip_code_str = '0' + zip_code_str
    return zip_code_str


# Function to clean the Largest Property Type column with debug prints
def clean_largest_property_type(row):
    """
    Cleans the Largest Property Type by replacing missing or invalid values with All Property Types.

    Args:
        row (pd.Series): The row of the DataFrame.

    Returns:
        str: The cleaned Largest Property Type.
    """
    if pd.isna(row['Largest Property Type']) or row['Largest Property Type'] == '' or row['Largest Property Type'] == 0:
        all_types = str(row['All Property Types']).split(',')
        non_residential_types = [ptype.strip() for ptype in all_types if
                                 ptype.strip() != 'Multifamily Housing' and ptype.strip()]

        # Debug output
        selected_type = non_residential_types[0] if non_residential_types else 'Multifamily Housing'

        if non_residential_types:
            return selected_type
        return 'Multifamily Housing'
    return row['Largest Property Type']


# File paths to separated BERDO data and emissions factors for all fuel types each year through 2050
file_path_2022 = '../data-files/2-berdo_reported_2022.csv'
file_path_2023 = '../data-files/2-berdo_reported_2023.csv'
file_path_emissions = '../data-files/1-emissions-factors.csv'
file_path_property_types = '../data-files/1-property-types.csv'

# DataFrames for BERDO data and emissions factors
df_berdo_reported_2022 = pd.read_csv(file_path_2022)
df_berdo_reported_2023 = pd.read_csv(file_path_2023)
df_berdo_emissions_factors = pd.read_csv(file_path_emissions)

# Concatenated DataFrame for all BERDO Data
df_berdo_data = pd.concat([df_berdo_reported_2022, df_berdo_reported_2023], axis=0)

# Merged DataFrame including emissions factors data
df_berdo = pd.merge(df_berdo_data, df_berdo_emissions_factors, on=['Data Year'], how='left')

# Remove unneeded columns
df_berdo = df_berdo.drop(['Estimated Total GHG Emissions (kgCO2e)'], axis=1)

# Convert Property Type columns to strings
df_berdo['Largest Property Type'] = df_berdo['Largest Property Type'].astype(str)
df_berdo['All Property Types'] = df_berdo['All Property Types'].astype(str)
# Clean Property Type columns of all whitespace and replace columns with just a comma with a zero
df_berdo['Largest Property Type'] = df_berdo['Largest Property Type'].apply(lambda x: clean_cell(x))
df_berdo['All Property Types'] = df_berdo['All Property Types'].apply(lambda x: clean_cell(x))

# Fill in all NaN's with 0's
df_berdo.fillna(0, inplace=True)

# Add columns for emissions by fuel source for each Data Year/BERDO ID
df_berdo['Electricity Emissions (MT CO2e)'] = (
    (df_berdo['Electricity Usage (kBtu)'] - df_berdo['Renewable System Electricity Usage Onsite (kBtu)']) / 1000
) * df_berdo['Electricity Emissions'] / 1000

df_berdo['Natural Gas Emissions (MT CO2e)'] = ((df_berdo['Natural Gas Usage (kBtu)'] / 1000)
                                               * df_berdo['Natural Gas Emissions']) / 1000

df_berdo['Fuel Oil #1 Emissions (MT CO2e)'] = ((df_berdo['Fuel Oil 1 Usage (kBtu)'] / 1000)
                                               * df_berdo['Fuel Oil #1 Emissions']) / 1000

df_berdo['Fuel Oil #2 Emissions (MT CO2e)'] = ((df_berdo['Fuel Oil 2 Usage (kBtu)'] / 1000)
                                               * df_berdo['Fuel Oil #2 Emissions']) / 1000

df_berdo['Fuel Oil #4 Emissions (MT CO2e)'] = ((df_berdo['Fuel Oil 4 Usage (kBtu)'] / 1000)
                                               * df_berdo['Fuel Oil #4 Emissions']) / 1000

df_berdo['Fuel Oil #5 & #6 Emissions (MT CO2e)'] = ((df_berdo['Fuel Oil 5 and 6 Usage (kBtu)'] / 1000)
                                                    * df_berdo['Fuel Oil #5 & 6 Emissions']) / 1000

df_berdo['Diesel #2 Emissions (MT CO2e)'] = ((df_berdo['Diesel Usage (kBtu)'] / 1000)
                                             * df_berdo['Diesel #2 Emissions']) / 1000

df_berdo['Propane Emissions (MT CO2e)'] = ((df_berdo['Propane Usage (kBtu)'] / 1000)
                                           * df_berdo['Propane Emissions']) / 1000

df_berdo['Kerosene Emissions (MT CO2e)'] = ((df_berdo['Kerosene Usage (kBtu)'] / 1000)
                                            * df_berdo['Kerosene Emissions']) / 1000

df_berdo['District Chilled Water Emissions (MT CO2e)'] = ((df_berdo['District Chilled Water Usage (kBtu)'] / 1000)
                                                          * df_berdo['District Chilled Water Emissions']) / 1000

df_berdo['District Steam Emissions (MT CO2e)'] = ((df_berdo['District Steam Usage (kBtu)'] / 1000)
                                                  * df_berdo['District Steam Emissions']) / 1000

# Create a list of all GHG emissions columns to feed into the next operation
GHG_emissions_columns = ['Electricity Emissions (MT CO2e)', 'Natural Gas Emissions (MT CO2e)',
                         'Fuel Oil #1 Emissions (MT CO2e)', 'Fuel Oil #2 Emissions (MT CO2e)',
                         'Fuel Oil #4 Emissions (MT CO2e)', 'Fuel Oil #5 & #6 Emissions (MT CO2e)',
                         'Diesel #2 Emissions (MT CO2e)', 'Propane Emissions (MT CO2e)', 'Kerosene Emissions (MT CO2e)',
                         'District Chilled Water Emissions (MT CO2e)', 'District Steam Emissions (MT CO2e)']

# Sum all emissions data rows to get the total GHG emissions based on established emissions factors
# Important to note that this value is NOT the same as the reported value & should be checked with Samira
df_berdo['Total GHG Emissions (MT CO2e)'] = df_berdo[GHG_emissions_columns].sum(axis=1)
df_berdo['Total GHG Emissions (MT CO2e)'] = df_berdo['Total GHG Emissions (MT CO2e)'].round().astype(int)

# Remove rows with no value for Largest Property Type and replace it with All Property Types column
df_berdo['Largest Property Type'] = df_berdo.apply(clean_largest_property_type, axis=1)

# # If All Property Types is now the same as Largest Property Type, then zero out the All Property Types
# df_berdo['All Property Types'] = df_berdo.apply(
#     lambda row: 0 if row['Largest Property Type'] == row['All Property Types'] else row['All Property Types'],
#     axis=1)

# Filter out rows that have more than one BERDO ID in the BERDO ID column to move campus projects to another DataFrame
df_berdo_buildings = df_berdo[df_berdo['BERDO ID'].apply(is_integer)].copy()
df_berdo_buildings['BERDO ID'] = df_berdo_buildings['BERDO ID'].astype(int)
df_berdo_campuses = df_berdo[~df_berdo['BERDO ID'].apply(is_integer)].copy()

# Add leading zero to zip codes
df_berdo['Building Address Zip Code'] = df_berdo['Building Address Zip Code'].astype(int)
df_berdo['Parcel Address Zip Code'] = df_berdo['Parcel Address Zip Code'].astype(int)
df_berdo['Building Address Zip Code'] = df_berdo['Building Address Zip Code'].apply(add_leading_zero)
df_berdo['Parcel Address Zip Code'] = df_berdo['Parcel Address Zip Code'].apply(add_leading_zero)

# Sort BERDO buildings by BERDO ID
df_berdo_buildings = df_berdo_buildings.sort_values(by='BERDO ID', ascending=True)

# # Check all property types used in the raw data to map to BERDO threshold categories
# property_types = df_berdo_buildings['Largest Property Type'].unique()
# df_property_types = pd.DataFrame(property_types, columns=['Property Type'])
# df_property_types.to_csv('../data-files/1-preprocessed-emissions-data/0-property-types.csv')

# Load in BERDO property types mapping
df_property_types = pd.read_csv(file_path_property_types)

# Merge BERDO property types to the Largest Property Type column of df_berdo_buildings
df_berdo_buildings_merged = df_berdo_buildings.merge(df_property_types, on='Largest Property Type', how='left')
# df_berdo_buildings_merged = df_berdo_buildings_merged.drop(['Unnamed: 2'], axis=1)

# Send data to CSV for further processing
df_berdo_campuses.to_csv('../data-files/1-preprocessed-emissions-data/2-berdo-campus-emissions-data.csv', index=False)
df_berdo_buildings_merged.to_csv('../data-files/1-preprocessed-emissions-data/1-berdo-emissions-data.csv', index=False)

print(df_berdo_buildings_merged.shape)




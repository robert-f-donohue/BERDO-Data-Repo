import pandas as pd


def clean_cell(cell):
    # Strip white space
    cell = cell.strip()
    # Replace cell with 0 if it is just a comma
    if cell == ',':
        return 0
    # Convert to integer if possible
    try:
        return int(cell)
    except ValueError:
        return cell

# File paths to separated BERDO data and emissions factors for all fuel types each year through 2050
file_path_2022 = '../data-files/2-berdo_reported_2022.csv'
file_path_2023 = '../data-files/2-berdo_reported_2023.csv'
file_path_emissions = '../data-files/1-emissions-factors.csv'

# DataFrames for BERDO data and emissions factors
df_berdo_reported_2022 = pd.read_csv(file_path_2022)
df_berdo_reported_2023 = pd.read_csv(file_path_2023)
df_berdo_emissions_factors = pd.read_csv(file_path_emissions)

# Concatenated DataFrame for all BERDO Data
df_berdo_data = pd.concat([df_berdo_reported_2022, df_berdo_reported_2023], axis=0)

# Merged DataFrame including emissions factors data
df_berdo = pd.merge(df_berdo_data, df_berdo_emissions_factors, on=['Data Year'], how='left')

# Remove unneeded columns
df_berdo = df_berdo.drop(['Estimated Total GHG Emissions (kgCO2e)', 'Cooresponding Campus ID'], axis=1)

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

# Fill in all NaN's with 0's
df_berdo.fillna(0, inplace=True)

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

# Clean DataFrame of all whitespace and replace columns with just a comma with a zero
df_berdo = df_berdo.applymap(clean_cell)

# Remove rows with no value for Largest Property Type and replace it with All Property Types column
df_berdo['Largest Property Type'] = df_berdo.apply(
    lambda row: row['All Property Types'] if row['Largest Property Type'] == 0 else row['Largest Property Type'],
    axis=1)

# If All Property Types is now the same as Largest Property Type, then zero out the All Property Types
df_berdo['All Property Types'] = df_berdo.apply(
    lambda row: 0 if row['Largest Property Type'] == row['All Property Types'] else row['All Property Types'],
    axis=1)


df_berdo = df_berdo.sort_values(by=['Largest Property Type', 'All Property Types'], ascending=True)

df_berdo.to_csv('../data-files/preprocessed-data/berdo-emissions-data.csv', index=False)


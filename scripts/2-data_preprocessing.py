import pandas as pd

# File paths to separated BERDO data and emissions factors for all fuel types each year through 2050
file_path_2022 = '../data-files/2-berdo_reported_2022.csv'
file_path_2023 = '../data-files/2-berdo_reported_2023.csv'
file_path_emissions = '../data-files/1-emissions-factors.csv'

# DataFrames for BERDO data and emissions factors
df_berdo_reported_2022 = pd.read_csv(file_path_2022)
df_berdo_reported_2023 = pd.read_csv(file_path_2023)
berdo_emissions_factors = pd.read_csv(file_path_emissions)

# Concatenated DataFrame for all BERDO Data
df_berdo_data = pd.concat([df_berdo_reported_2022, df_berdo_reported_2023], axis=0)

# Merged DataFrame including emissions factors data
df_berdo = pd.merge(df_berdo_data, berdo_emissions_factors, on=['Data Year'], how='left')

print(df_berdo.head())
print(df_berdo.shape)

# Remove unneeded columns
df_berdo = df_berdo.drop(['Estimated Total GHG Emissions (kgCO2e)', 'Cooresponding Campus ID'], axis=1)

# Add columns for emissions by fuel source for each Data Year/BERDO ID
df_berdo['Electricity Emissions (MT CO2e)'] = (((df_berdo['Electricity Usage (kBtu)']
                                                 - df_berdo['Renewable System Electricity Usage Onsite (kBtu)']) / 1000)
                                               * df_berdo['Electricity Emissions']) / 1000

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

df_berdo.to_csv('../data-files/preprocessed-data/berdo-emissions-data.csv')


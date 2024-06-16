import pandas as pd


# Columns for the original BERDO DataFrame
columns_final = ['BERDO ID', 'Tax Parcel ID', 'Property Owner Name', 'Building Address', 'Building Address Zip Code',
                 'Parcel Address', 'Parcel Address Zip Code', 'Reported Gross Floor Area (Sq Ft)',
                 'Largest Property Type', 'All Property Types', 'Site EUI (Energy Use Intensity kBtu/ft2)',
                 'Total Site Energy Usage (kBtu)', 'Natural Gas Usage (kBtu)', 'Electricity Usage (kBtu)',
                 'Renewable System Electricity Usage Onsite (kBtu)', 'District Hot Water Usage (kBtu)',
                 'District Chilled Water Usage (kBtu)', 'District Steam Usage (kBtu)', 'Fuel Oil 1 Usage (kBtu)',
                 'Fuel Oil 2 Usage (kBtu)', 'Fuel Oil 4 Usage (kBtu)', 'Fuel Oil 5 and 6 Usage (kBtu)',
                 'Propane Usage (kBtu)', 'Diesel Usage (kBtu)', 'Kerosene Usage (kBtu)', 'Cooresponding Campus ID',
                 'Data Year', 'Electricity Emissions', 'Natural Gas Emissions', 'Fuel Oil #1 Emissions',
                 'Fuel Oil #2 Emissions', 'Fuel Oil #4 Emissions', 'Fuel Oil #5 & 6 Emissions', 'Diesel #2 Emissions',
                 'Propane Emissions', 'Kerosene Emissions', 'District Chilled Water Emissions',
                 'District Steam Emissions', 'Electricity Emissions (MT CO2e)', 'Natural Gas Emissions (MT CO2e)',
                 'Fuel Oil #1 Emissions (MT CO2e)', 'Fuel Oil #2 Emissions (MT CO2e)','Fuel Oil #4 Emissions (MT CO2e)',
                 'Fuel Oil #5 & #6 Emissions (MT CO2e)', 'Diesel #2 Emissions (MT CO2e)', 'Propane Emissions (MT CO2e)',
                 'Kerosene Emissions (MT CO2e)', 'District Chilled Water Emissions (MT CO2e)',
                 'District Steam Emissions (MT CO2e)', 'Total GHG Emissions (MT CO2e)', 'BERDO Property Type']

# File path to preprocessed emissions data
file_path_emissions_data = '../data-files/1-preprocessed-emissions-data/1-berdo-emissions-data.csv'
# File path to yearly BERDO thresholds by property type
file_path_property_thresholds = '../data-files/1-thresholds-berdo.csv'

# DataFrame for preprocessed BERDO data
df_berdo_thresholds = pd.read_csv(file_path_emissions_data)
# DataFrame for yearly BERDO thresholds by property type
df_property_thresholds = pd.read_csv(file_path_property_thresholds)

# Melt df_property_thresholds
df_property_thresholds_melted = df_property_thresholds.melt(
    id_vars=['Year'], var_name='BERDO Property Type', value_name='Threshold')

# Merge on 'Largest Property Type'
df_merged = df_berdo_thresholds.merge(df_property_thresholds_melted, on='BERDO Property Type', how='left')

# Pivot the DataFrame to have years as columns
df_pivot = df_merged.pivot_table(index=columns_final, columns='Year', values='Threshold').reset_index()

# Add the Threshold columns
df_pivot.columns = (columns_final + [f'Threshold {year}' for year in df_pivot.columns[51:]])

print(df_pivot.head())
print(df_pivot.shape)


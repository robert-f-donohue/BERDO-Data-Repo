import pandas as pd


def transform_energy_usage(df, start_year=2025, end_year=2050):
    """
    Transforms energy usage data into a long format and projects it over a range of years.

    This function takes a DataFrame with energy usage data, melts it into a long format,
    and replicates each energy usage entry for a range of years from `start_year` to
    `end_year`. Additionally, it maps `reporting_id` to a new `building_id` and arranges
    the columns in a specified order.

    Parameters:
    df (pandas.DataFrame): The input DataFrame containing energy usage data with
                           'reporting_id' and energy types as columns.
    start_year (int, optional): The starting year for the projection. Default is 2025.
    end_year (int, optional): The ending year for the projection. Default is 2050.

    Returns:
    pandas.DataFrame: A transformed DataFrame with columns ['building_id', 'year',
                       'energy_type', 'usage'].
    """
    # Melt the DataFrame to long format with 'energy_type' and 'usage'
    melted_df = pd.melt(df, id_vars=['reporting_id'], var_name='energy_type', value_name='usage')

    # Generate a DataFrame for the years range
    years_df = pd.DataFrame({'year': range(start_year, end_year + 1)})

    # Create a cartesian product of melted_df and years_df to replicate each energy usage for each year
    transformed_df = pd.merge(melted_df, years_df, how='cross')

    # Create a mapping for 'reporting_id' to 'building_id' starting with B1000 as 1
    reporting_id_map = {rid: index for index, rid in enumerate(sorted(df['reporting_id'].unique()), start=1)}
    transformed_df['building_id'] = transformed_df['reporting_id'].map(reporting_id_map)

    # Drop the 'reporting_id' as it's no longer needed
    transformed_df.drop('reporting_id', axis=1, inplace=True)

    # Select and order the columns as specified
    final_df = transformed_df[['building_id', 'year', 'energy_type', 'usage']]

    return final_df


def transform_emissions_factors(df):
    """
    Transforms a DataFrame of emissions factors into a long format.

    This function takes a DataFrame with emissions factors, where each row represents
    a year and each column (except 'Data Year') represents an energy type with its
    corresponding emissions factor. The function converts the DataFrame into a long
    format with columns ['year', 'energy_type', 'emissions_kgco2e_per_unit'].

    Parameters:
    df (pandas.DataFrame): The input DataFrame containing emissions factors. The first
                           column should be 'Data Year' and the subsequent columns
                           should represent different energy types.

    Returns:
    pandas.DataFrame: A transformed DataFrame with columns ['year', 'energy_type',
                       'emissions_kgco2e_per_unit'].
    """
    # Initialize a list to hold the transformed data
    transformed_data = []

    # Iterate over each row in the dataframe
    for index, row in df.iterrows():
        year = row['Data Year']
        # Iterate over each column, using .items() for compatibility
        for energy_type, emissions in row.items():
            if energy_type == 'Data Year':
                continue
            # Append the transformed row to the list, cleaning energy_type names
            transformed_data.append({
                'year': year,
                'energy_type': energy_type,
                'emissions_kgco2e_per_unit': emissions
            })

    # Convert the list of dicts to a DataFrame
    return pd.DataFrame(transformed_data)


def transform_emissions_thresholds(df):
    """
    Transforms a DataFrame of threshold values into a long format suitable for analysis.

    This function filters the input DataFrame to retain only the 'Reporting ID' and
    columns containing 'Threshold'. It then melts the DataFrame to unpivot the
    threshold columns, extracts the year from the column names, and renames the
    columns to match the target structure. Additionally, it maps 'Reporting ID' to
    a new 'building_id' and sorts the final DataFrame by 'building_id' and 'year'.

    Parameters:
    df (pandas.DataFrame): The input DataFrame containing 'Reporting ID' and
                           threshold columns with years in their names.

    Returns:
    pandas.DataFrame: A transformed DataFrame with columns ['building_id', 'year',
                       'threshold_mt_co2e'].
    """
    # Filter for columns of interest: Reporting ID and Threshold columns
    threshold_columns = [col for col in df.columns if 'Threshold' in col]
    df_filtered = df[['BERDO ID'] + threshold_columns].copy()

    # Melt the dataframe to unpivot the threshold columns
    df_melted = df_filtered.melt(id_vars=["BERDO ID"], var_name="Year", value_name="Threshold MT CO2e")

    # Extract the year from the "Year" column
    df_melted["Year"] = df_melted["Year"].str.extract(r'(\d+)').astype(int)

    # Rename columns to match the target structure
    df_melted.rename(columns={"BERDO ID": "building_id", "Year": "year", "Threshold MT CO2e": "threshold_mt_co2e"},
                     inplace=True)

    # Assuming "Building ID" needs to be mapped to a foreign key, this example will simply use a placeholder transformation.
    # In practice, you would map these IDs to the corresponding foreign keys in your database.
    building_id_mapping = {id_: i for i, id_ in enumerate(df_melted["building_id"].unique(), start=1)}
    df_melted["building_id"] = df_melted["building_id"].map(building_id_mapping)

    df_melted = df_melted.sort_values(by=['building_id', 'year'])

    return df_melted


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

# Columns for buildings_table
columns_building_table = ['BERDO ID', 'Building Address', 'Building Address Zip Code', 'Property Owner Name',
                          'Reported Gross Floor Area (Sq Ft)', 'Largest Property Type', 'All Property Types']

# Columns for energy_usage_table
columns_energy_usage_table = ['BERDO ID', 'Natural Gas Usage (kBtu)', 'Electricity Usage (kBtu)',
                              'District Chilled Water Usage (kBtu)', 'District Steam Usage (kBtu)',
                              'Fuel Oil 1 Usage (kBtu)', 'Fuel Oil 2 Usage (kBtu)', 'Fuel Oil 4 Usage (kBtu)',
                              'Fuel Oil 5 and 6 Usage (kBtu)', 'Propane Usage (kBtu)', 'Diesel Usage (kBtu)',
                              'Kerosene Usage (kBtu)']

# File path to preprocessed emissions data
file_path_emissions_data = '../data-files/1-preprocessed-emissions-data/1-berdo-emissions-data.csv'
# File path to yearly BERDO thresholds by property type
file_path_property_thresholds = '../data-files/1-thresholds-berdo.csv'
# File path to emissions factors
file_path_emissions_factors = '../data-files/1-emissions-factors.csv'

# DataFrame for preprocessed BERDO data
df_berdo_thresholds = pd.read_csv(file_path_emissions_data)
# DataFrame for yearly BERDO thresholds by property type
df_property_thresholds = pd.read_csv(file_path_property_thresholds)
# DataFrame for emissions factors
df_emissions_factors = pd.read_csv(file_path_emissions_factors)

# Melt df_property_thresholds
df_property_thresholds_melted = df_property_thresholds.melt(
    id_vars=['Year'], var_name='BERDO Property Type', value_name='Threshold')

# Merge on 'Largest Property Type'
df_merged = df_berdo_thresholds.merge(df_property_thresholds_melted, on='BERDO Property Type', how='left')

# Pivot the DataFrame to have years as columns
df_pivot = df_merged.pivot_table(index=columns_final, columns='Year', values='Threshold').reset_index()

# Add the Threshold columns
df_pivot.columns = (columns_final + [f'Threshold {year}' for year in df_pivot.columns[51:]])

# Create DataFrame for 'buildings' PostgreSQL table and reset index
df_buildings_table = df_pivot[columns_building_table].copy()
df_buildings_table.reset_index(drop=True, inplace=True)

# Convert data types for Zip Code and GFA to correct format
df_buildings_table['Building Address Zip Code'] = df_buildings_table['Building Address Zip Code'].astype(int)
df_buildings_table['Reported Gross Floor Area (Sq Ft)'] = (df_buildings_table['Reported Gross Floor Area (Sq Ft)']
                                                           .astype(int))

# Rename columns to match PostgreSQL Tables
df_buildings_table.rename(columns={
    'BERDO ID': 'reporting_id',
    'Building Address': 'address',
    'Building Address Zip Code': 'zip_code',
    'Property Owner Name': 'owner_name',
    'Reported Gross Floor Area (Sq Ft)': 'property_gfa',
    'Largest Property Type': 'primary_property_type',
    'All Property Types': 'all_property_types'
}, inplace=True)

# Send buildings table data to CSV for SQL upload
df_buildings_table.to_csv('../data-files/2-sql-tables/1-buildings-table.csv', index=False)

# Create DataFrame for 'energy_usage' PostgreSQL table and reset index
df_energy_usage_table = df_pivot[columns_energy_usage_table].copy()
df_energy_usage_table.reset_index(drop=True, inplace=True)

# Rename columns to match PostgreSQL Tables
df_energy_usage_table = df_energy_usage_table.rename(columns={
    'BERDO ID': 'reporting_id',
    'Electricity Usage (kBtu)': 'electricity',
    'Natural Gas Usage (kBtu)': 'natural_gas',
    'Fuel Oil 1 Usage (kBtu)': 'fuel_oil_1',
    'Fuel Oil 2 Usage (kBtu)': 'fuel_oil_2',
    'Fuel Oil 4 Usage (kBtu)': 'fuel_oil_4',
    'Fuel Oil 5 and 6 Usage (kBtu)': 'fuel_oil_5_and_6',
    'Propane Usage (kBtu)': 'propane',
    'Diesel Usage (kBtu)': 'diesel_2',
    'Kerosene Usage (kBtu)': 'kerosene',
    'District Chilled Water Usage (kBtu)': 'district_chilled_water',
    'District Steam Usage (kBtu)': 'district_steam'
})

# Apply the transformation function to the energy_usage DataFrame
df_transformed_energy_usage_table = transform_energy_usage(df_energy_usage_table)
df_transformed_energy_usage_table['usage'] = df_transformed_energy_usage_table['usage'].astype(int)

# Send energy_usage table data to CSV for SQL upload
df_transformed_energy_usage_table.to_csv('../data-files/2-sql-tables/2-energy-usage-table.csv', index=False)

# Fix naming of energy types to have same format as energy_usage table
df_emissions_factors = df_emissions_factors.rename(columns={
    'Electricity Emissions': 'electricity',
    'Natural Gas Emissions': 'natural_gas',
    'Fuel Oil #1 Emissions': 'fuel_oil_1',
    'Fuel Oil #2 Emissions': 'fuel_oil_2',
    'Fuel Oil #4 Emissions': 'fuel_oil_4',
    'Fuel Oil #5 & 6 Emissions': 'fuel_oil_5_and_6',
    'Diesel #2 Emissions': 'diesel_2',
    'Propane Emissions': 'propane',
    'Kerosene Emissions': 'kerosene',
    'District Chilled Water Emissions': 'district_chilled_water',
    'District Steam Emissions': 'district_steam'
})

# Transform DataFrame to match formatting of PostgreSQL table
df_transformed_emissions_factors = transform_emissions_factors(df_emissions_factors)

# Make the year column an integer instead of a float
df_transformed_emissions_factors['year'] = df_transformed_emissions_factors['year'].astype(int)
# Sort data so it only shows years 2025-2050
df_transformed_emissions_factors = df_transformed_emissions_factors[df_transformed_emissions_factors['year'] >= 2025]

print(df_transformed_emissions_factors.head())

# Send transformed emissions factor DataFrame to CSV to be uploaded to PostgreSQL
df_transformed_emissions_factors.to_csv('../data-files/2-sql-tables/3-emissions-factors-table.csv', index=False)

# Transform BERDO DataFrame (df_pivot) to melt the yearly emissions thresholds
df_transformed_emissions_thresholds = transform_emissions_thresholds(df_pivot)

# Send transformed emissions threshold DataFrame to CSV to be uploaded to PostgreSQL
df_transformed_emissions_thresholds.to_csv('../data-files/2-sql-tables/4-emissions-thresholds-table.csv', index=False)


# Add unique identifiers to each dataframe
df_transformed_energy_usage_table['usage_id'] = range(1, len(df_transformed_energy_usage_table) + 1)
df_transformed_emissions_factors['factor_id'] = range(1, len(df_transformed_emissions_factors) + 1)

# Merge the dataframes on 'year' and 'energy_type', ensuring to keep the unique identifiers
df_energy_and_factors = pd.merge(
    df_transformed_energy_usage_table,
    df_transformed_emissions_factors,
    on=['year', 'energy_type'],
    how='inner'
)

# Calculate emissions_mt_co2e
df_energy_and_factors['emissions_mt_co2e'] = ((df_energy_and_factors['usage'] / 1000) *
                                              (df_energy_and_factors['emissions_kgco2e_per_unit'] / 1000))

# Select the necessary columns for the final dataframe, including the foreign keys
df_calculated_emissions_table = df_energy_and_factors[['usage_id', 'factor_id', 'emissions_mt_co2e']]

df_calculated_emissions_table.to_csv('../data-files/2-sql-tables/5-calculated-emissions-table.csv', index=False)

print(df_calculated_emissions_table.head())
print(df_calculated_emissions_table.shape)
print(df_transformed_energy_usage_table.shape)

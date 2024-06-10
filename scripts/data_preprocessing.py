import pandas as pd

berdo_reported = pd.read_csv('../data-files/berdo_reported.csv')
berdo_emissions_factors = pd.read_csv('../data-files/1-emissions-factors.csv')

columns_to_drop_2022 = ['Building Address City', 'Parcel Address City', 'Reported Enclosed Parking Area (Sq Ft)',
                        'Energy Star Score', 'Water Usage Intensity (Gallons/ft2)', 'Compliance Status',
                        'BERDO Report Status', 'Community Choice Electricity Participation',
                        'Renewable Energy Purchased through a Power Purchase Agreement (',
                        'Renewable Energy Certificate (REC) Purchase', 'Backup Generator', 'Battery Storage',
                        'Electric Vehicle (EV) Charging', 'Notes']

# Gets rid of duplicate condition 500 Walk Hill St
# index_to_drop_Walk_Hill = df_berdo_reported[df_berdo_reported['_id'] == 2508].index
# df_berdo_reported = df_berdo_reported.drop(index_to_drop_Walk_Hill)
#
# # Corrects the BERDO ID for Tudor Place & Adds
# df_berdo_reported.loc[df_berdo_reported['_id'] == 5959, 'BERDO ID'] = 106338
# df_berdo_reported.loc[df_berdo_reported['_id'] == 5959, 'Property Owner Name'] = 'TUDOR PLACE CONDO TRUST'
#
# # Add property owner name to BERDO ID 105780 and delete the unneeded row
# df_berdo_reported.loc[df_berdo_reported['_id'] == 5949, 'Property Owner Name'] = 'SIMMONS UNIVERSITY'
# index_to_drop_Simmons = df_berdo_reported[df_berdo_reported['_id'] == 2734].index
# df_berdo_reported = df_berdo_reported.drop(index_to_drop_Simmons)
#
# # Add blank columns to 154 Ruskindale Rd and delete incomplete column
# ruskindale_new_values = {
#     'Property Owner Name': 'CITY OF BOSTON',
#     'Tax Parcel ID': 1803703000,
#     'Building Address': '154-164 Ruskindale Rd',
#     'Parcel Address': '154 156 ruskindale rd',
#     'Parcel Address Zip Code': 2136
# }
#
# df_berdo_reported.loc[df_berdo_reported['_id'] == 5934, ['Property Owner Name', 'Tax Parcel ID', 'Building Address',
#                                                          'Parcel Address', 'Parcel Address Zip Code']]\
#                                                         = ruskindale_new_values.values()
#
# index_to_drop_Ruskindale = df_berdo_reported[df_berdo_reported['_id'] == 639].index
# df_berdo_reported = df_berdo_reported.drop(index_to_drop_Ruskindale)
#
# # Drop column for 150 Mass Ave that appears to be a single building in a campus
# index_to_drop_Berklee = df_berdo_reported[df_berdo_reported['_id'] == 5918].index
# df_berdo_reported = df_berdo_reported.drop(index_to_drop_Berklee)
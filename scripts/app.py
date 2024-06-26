from dash import Dash, html, dash_table, dcc, callback, Output, Input, State
from dash_bootstrap_templates import load_figure_template
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px


load_figure_template('YETI')


GHG_Dict = {
    'Electricity': {
        '2021': 81, '2022': 79, '2023': 77, '2024': 75, '2025': 73, '2026': 71, '2027': 69, '2028': 67,
        '2029': 65, '2030': 62, '2031': 60, '2032': 58, '2033': 56, '2034': 54, '2035': 52, '2036': 50,
        '2037': 48, '2038': 46, '2039': 44, '2040': 42, '2041': 40, '2042': 37, '2043': 35, '2044': 33,
        '2045': 31, '2046': 29, '2047': 27, '2048': 25, '2049': 23, '2050': 21,
    },
    'Natural Gas': 53.11, 'Fuel Oil (No. 1)': 73.5, 'Fuel Oil (No. 2)': 74.21, 'Fuel Oil (No. 4)': 75.29,
    'Diesel Oil': 74.21, 'District Steam': 66.4, 'District Hot Water': 66.4, 'Electric Driven Chiller': 52.7,
    'Absorption Chiller (Natural Gas)': 73.89, 'Engine-Driven Chiller (Natural Gas)': 49.31
}

# Cost of Metric Ton CO2 Above Threshold
CO2_COST = 243


# Conversions to mmBtu for Different Fuel Types
kWh_to_MMBTU = 0.003412
THERM_to_MMBTU = 0.1
FO1_Gal_to_MMBTU = 0.135
FO2_Gal_to_MMBTU = 0.14
FO4_Gal_to_MMBTU = 0.146
DIESEL_Gal_to_MMBTU = 0.1387

# Incorporate data
df = pd.read_csv('Thresholds_1.csv')
df_1 = pd.read_csv('Thresholds_comparison.csv')

# Initialize the app - incorporate CSS
app = Dash(__name__, external_stylesheets=[dbc.themes.YETI])

# Formatting Colors
colors = {
    'enviGREEN': '#2EAE96',
    'enviGRAY': '#979696'
}

# App layout
app.layout = dbc.Container([
    dbc.Row([
        html.Div(
            className='row',
            children='BERDO 2.0 Emissions Threshold Summary',
            style={'padding': '10px',
                   'margin-left': '4px',
                   'textAlign': 'center',
                   'color': '#2EAE96',
                   'font-family': 'Helvetica',
                   'fontSize': 35,
                   'fontWeight': 'bold',
                   'fontStyle': 'italic'
                   }
                )
        ]),
    # Content Row
    dbc.Row([
        # Column Width 4
        dbc.Col([
            # Dropdown Component
            html.Div(className='row',
                     children=[
                        dcc.Dropdown(['Assembly', 'College/University', 'Education', 'Food Sales & Services', 'Healthcare',
                                      'Lodging', 'Manufacturing/Industrial', 'Multifamily Housing', 'Office', 'Retail',
                                      'Services', 'Storage', 'Technology/Science'], '', id='my-dropdown-final')
                              ]
                     ),
            # GSF Entry Section
            html.Div([
                html.Label('Building Square Footage:', style={}),
                dcc.Input(id='gsf', type='text', value='0', style={'marginLeft': '10px',
                                                                   'marginRight': '25px',
                                                                   'marginTop': '10px',
                                                                   'marginBottom': '10px'})
                ], style={'textAlign': 'right'}),

            # Scrollable Section
            html.Div([
                html.Div([
                    html.Label('Electricity (kWh):'),
                    dcc.Input(id='value1', type='text', value='0', style={'marginLeft': '10px', 'marginTop': '10px'})
                ]),
                html.Div([
                    html.Label('Natural Gas (Therm):'),
                    dcc.Input(id='value2', type='text', value='0', style={'marginLeft': '10px', 'marginTop': '10px'})
                ]),
                html.Div([
                    html.Label('Fuel Oil #1 (gal):'),
                    dcc.Input(id='value3', type='text', value='0', style={'marginLeft': '10px', 'marginTop': '10px'})
                ]),
                html.Div([
                    html.Label('Fuel Oil #2 (gal):'),
                    dcc.Input(id='value4', type='text', value='0', style={'marginLeft': '10px', 'marginTop': '10px'})
                ]),
                html.Div([
                    html.Label('Fuel Oil #4 (gal):'),
                    dcc.Input(id='value5', type='text', value='0', style={'marginLeft': '10px', 'marginTop': '10px'})
                ]),
                html.Div([
                    html.Label('Diesel Fuel (gal):'),
                    dcc.Input(id='value6', type='text', value='0', style={'marginLeft': '10px', 'marginTop': '10px'})
                ]),
                html.Div([
                    html.Label('District Steam (MMBtu):'),
                    dcc.Input(id='value7', type='text', value='0', style={'marginLeft': '10px', 'marginTop': '10px'})
                ]),
                html.Div([
                    html.Label('District Hot Water (MMBtu):'),
                    dcc.Input(id='value8', type='text', value='0', style={'marginLeft': '10px', 'marginTop': '10px'})
                ]),
                html.Div([
                    html.Label('Elec-Driven Chiller (MMBtu):'),
                    dcc.Input(id='value9', type='text', value='0', style={'marginLeft': '10px', 'marginTop': '10px'})
                ]),
                html.Div([
                    html.Label('Gas Absorp. Chiller (MMBtu):'),
                    dcc.Input(id='value10', type='text', value='0', style={'marginLeft': '10px', 'marginTop': '10px'})
                ]),
                html.Div([
                    dbc.Label('Gas-Driven Chiller (MMBtu):'),
                    dcc.Input(id='value11', type='text', value='0', style={'marginLeft': '10px', 'marginTop': '10px'})
                ])
            ], style={'text-align': 'right',
                      'maxHeight': '300px',
                      'overflowY': 'scroll',
                      'boxShadow': '0px 5px 15px -3px rgba(0, 0, 0, 0.2), 0px 4px 6px -2px rgba(0, 0, 0, 0.1)',
                      'padding': '10px'
                      }),

            # Button outside the scrollable section
            html.Div([
                dbc.Button('Calculate', id='calc_button', n_clicks=0,
                           color='light', style={'marginTop': '15px',
                                                 'boxShadow': '0px 5px 15px -3px rgba(0, 0, 0, 0.2), 0px 4px 6px -2px rgba(0, 0, 0, 0.1)'})
            ]),
            html.Br(),
            # Summary table below the output section
            html.Div(id='summary-table')
        ], width=3),

        # Another column of width 8 to the right of the charts
        dbc.Col([
            # First Graph
            html.Div(style={'display': 'flex'},
                     children=[dcc.Graph(figure={}, id='berdo-cei-graph-final', style={'flex': 1})]),

            # Second Graph
            html.Div(style={'display': 'flex'},
                     children=[dcc.Graph(figure={}, id='berdo-cost-graph-final', style={'flex': 1})])
        ], width=9)
        ])
    ], fluid=True
)


@callback(
    Output(component_id='berdo-cei-graph-final', component_property='figure'),
    Output(component_id='berdo-cost-graph-final', component_property='figure'),
    Output(component_id='summary-table', component_property='children'),
    Input(component_id='my-dropdown-final', component_property='value'),
    Input('calc_button', 'n_clicks'),
    State('gsf', 'value'),
    State('value1', 'value'),
    State('value2', 'value'),
    State('value3', 'value'),
    State('value4', 'value'),
    State('value5', 'value'),
    State('value6', 'value'),
    State('value7', 'value'),
    State('value8', 'value'),
    State('value9', 'value'),
    State('value10', 'value'),
    State('value11', 'value')
)
def update_graph(col_chosen, n_clicks, *input_values):
    values = []  # This list will store the user-defined inputs

    if n_clicks > 0:
        # Convert input values to floats and remove any non-numeric values
        values = [float(v) if v.replace('.', '', 1).isdigit() else 0.0 for v in input_values]

    if not values:
        return px.bar(title='<b>Building CEI vs. BERDO Threshold (kg CO2e/sf/yr)</b>'), \
               px.bar(title='<b>Building Cost Penalty ($)</b>'), \
               html.Div("Click Calculate to load graphics.")




    # Create a DataFrame of CEI per Fuel Type
    df_cei = pd.DataFrame({
        'X': ['2021', '2022', '2023', '2024', '2025', '2026', '2027', '2028',
              '2029', '2030', '2031', '2032', '2033', '2034', '2035', '2036',
              '2037', '2038', '2039', '2040', '2041', '2042', '2043', '2044',
              '2045', '2046', '2047', '2048', '2049', '2050'],
        'Electricity': [values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2021'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2022'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2023'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2024'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2025'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2026'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2027'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2028'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2029'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2030'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2031'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2032'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2033'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2034'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2035'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2036'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2037'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2038'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2039'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2040'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2041'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2042'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2043'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2044'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2045'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2046'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2047'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2048'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2049'] / values[0],
                        values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2050'] / values[0]],
        'Natural Gas': [values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0],
                        values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] / values[0]],
        'Fuel Oil #1': [values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0],
                        values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] / values[0]],
        'Fuel Oil #2': [values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0],
                        values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] / values[0]],
        'Fuel Oil #4': [values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0],
                        values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] / values[0]],
        'Diesel': [values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0],
                   values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] / values[0]],
        'District Steam': [values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0],
                           values[7] * GHG_Dict['District Steam'] / values[0]],
        'District Hot Water': [values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0],
                               values[8] * GHG_Dict['District Hot Water'] / values[0]],
        'Elec-Driven Chiller': [values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0],
                                values[9] * GHG_Dict['Electric Driven Chiller'] / values[0]],
        'Gas Absorption Chiller': [values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0],
                                   values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] / values[0]],
        'Engine-Driven Chiller': [values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0],
                                  values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)'] / values[0]],
    })

    # Create a DataFrame of CEI for all Fuel Types
    df_total_cei = pd.DataFrame({
        'X': ['2025', '2026', '2027', '2028',
              '2029', '2030', '2031', '2032', '2033', '2034', '2035', '2036',
              '2037', '2038', '2039', '2040', '2041', '2042', '2043', '2044',
              '2045', '2046', '2047', '2048', '2049', '2050'],
        'Building Carbon': [((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2025'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2026'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2027'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2028'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2029'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2030'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2031'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2032'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2033'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2034'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2035'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2036'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2037'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2038'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2039'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2040'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2041'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2042'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2043'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2044'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2045'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2046'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2047'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2048'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2049'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0]),

                            ((values[1] * kWh_to_MMBTU * GHG_Dict['Electricity']['2050'] +
                             values[2] * THERM_to_MMBTU * GHG_Dict['Natural Gas'] +
                             values[3] * FO1_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 1)'] +
                             values[4] * FO2_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 2)'] +
                             values[5] * FO4_Gal_to_MMBTU * GHG_Dict['Fuel Oil (No. 4)'] +
                             values[6] * DIESEL_Gal_to_MMBTU * GHG_Dict['Diesel Oil'] +
                             values[7] * GHG_Dict['District Steam'] +
                             values[8] * GHG_Dict['District Hot Water'] +
                             values[9] * GHG_Dict['Electric Driven Chiller'] +
                             values[10] * GHG_Dict['Absorption Chiller (Natural Gas)'] +
                             values[11] * GHG_Dict['Engine-Driven Chiller (Natural Gas)']) / values[0])

        ]
    })

    # Create a Condition for CEI Penalty
    condition = df_total_cei['Building Carbon'] > df_1[col_chosen]

    # Create an Empty DataFrame for Cost per Year
    df_cost = pd.DataFrame()

    # Populate the DataFrame with Cost Penalty using Conditional Statement
    df_cost['Cost Penalty'] = round(((df_total_cei['Building Carbon'] - df_1[col_chosen]) * CO2_COST * values[0] * condition / 1000), 0)

    # Create a DataFrame for Summary of Results
    df_summary = pd.DataFrame({
        'Period': ['2025-2029', '2030-2034', '2035-2039', '2040-2044', '2045-2049', '2050+'],
        'Building Emissions (kg CO2e/sf/yr)': [round(df_total_cei['Building Carbon'][0], 2), round(df_total_cei['Building Carbon'][5], 2),
                                               round(df_total_cei['Building Carbon'][10], 2), round(df_total_cei['Building Carbon'][15], 2),
                                               round(df_total_cei['Building Carbon'][20], 2), round(df_total_cei['Building Carbon'][25], 2)],
        'BERDO Threshold (kg CO2e/sf/yr)': [df_1[col_chosen][0], df_1[col_chosen][5],
                                            df_1[col_chosen][10], df_1[col_chosen][15],
                                            df_1[col_chosen][20], df_1[col_chosen][25]],
        'Cost Penalty ($/yr)': [df_cost['Cost Penalty'][0], df_cost['Cost Penalty'][5], df_cost['Cost Penalty'][10],
                                df_cost['Cost Penalty'][15], df_cost['Cost Penalty'][20], df_cost['Cost Penalty'][25]]
    })

    # Create Line Graph
    fig1 = px.line(df, x="Year", y=col_chosen, template='YETI', title='<b>Building CEI vs. BERDO Threshold (kg CO2e/sf/yr)</b>')
    fig1.update_traces(line=dict(width=3, color='#000000'), hovertemplate='<b>Year:</b> %{x}<br><b>Threshold CEI:</b> %{y}</b>')

    # Create Bar Graph
    fig2 = px.bar(x=df_total_cei['X'], y=df_cost['Cost Penalty'], template='YETI', title='<b>Building Cost Penalty ($)')
    fig2.update_traces(hovertemplate='<b>Year:</b> %{x}<br><b>Cost Penalty:</b> %{y}')

    # Create bar chart of emissions values
    fig1.add_trace(go.Bar(x=df_cei['X'], y=[round(val, 2) for val in df_cei['Engine-Driven Chiller']],
                          name='Engine-Driven Chiller',
                          marker={'color': '#00951D'},
                          hovertemplate='<b>Year:</b> %{x}<br><b>CEI:</b> %{y}'))
    fig1.add_bar(x=df_cei['X'], y=[round(val, 2) for val in df_cei['Gas Absorption Chiller']],
                 name='Gas Absorption Chiller',
                 marker={'color': '#0FE312'},
                 hovertemplate='<b>Year:</b> %{x}<br><b>CEI:</b> %{y}')
    fig1.add_bar(x=df_cei['X'], y=[round(val, 2) for val in df_cei['Elec-Driven Chiller']],
                 name='Elec-Driven Chiller',
                 marker={'color': '#7BF0A4'},
                 hovertemplate='<b>Year:</b> %{x}<br><b>CEI:</b> %{y}')
    fig1.add_bar(x=df_cei['X'], y=[round(val, 2) for val in df_cei['District Hot Water']],
                 name='District Hot Water',
                 marker={'color': '#0105F5'},
                 hovertemplate='<b>Year:</b> %{x}<br><b>CEI:</b> %{y}')
    fig1.add_bar(x=df_cei['X'], y=[round(val, 2) for val in df_cei['District Steam']],
                 name='District Steam',
                 marker={'color': '#01A3F5'},
                 hovertemplate='<b>Year:</b> %{x}<br><b>CEI:</b> %{y}')
    fig1.add_bar(x=df_cei['X'], y=[round(val, 2) for val in df_cei['Diesel']],
                 name='Diesel',
                 marker={'color': '#931E07'},
                 hovertemplate='<b>Year:</b> %{x}<br><b>CEI:</b> %{y}')
    fig1.add_bar(x=df_cei['X'], y=[round(val, 2) for val in df_cei['Fuel Oil #4']],
                 name='Fuel Oil #4',
                 marker={'color': '#9901F5'},
                 hovertemplate='<b>Year:</b> %{x}<br><b>CEI:</b> %{y}')
    fig1.add_bar(x=df_cei['X'], y=[round(val, 2) for val in df_cei['Fuel Oil #2']],
                 name='Fuel Oil #2',
                 marker={'color': '#E601F5'},
                 hovertemplate='<b>Year:</b> %{x}<br><b>CEI:</b> %{y}')
    fig1.add_bar(x=df_cei['X'], y=[round(val, 2) for val in df_cei['Fuel Oil #1']],
                 name='Fuel Oil #1',
                 marker={'color': '#F501A4'},
                 hovertemplate='<b>Year:</b> %{x}<br><b>CEI:</b> %{y}')
    fig1.add_bar(x=df_cei['X'], y=[round(val, 2) for val in df_cei['Natural Gas']],
                 name='Natural Gas',
                 marker={'color': '#E51212'},
                 hovertemplate='<b>Year:</b> %{x}<br><b>CEI:</b> %{y}')
    fig1.add_bar(x=df_cei['X'], y=[round(val, 2) for val in df_cei['Electricity']],
                 name='Electricity',
                 marker={'color': '#E4E948'},
                 hovertemplate='<b>Year:</b> %{x}<br><b>CEI:</b> %{y}')

    # Stack bar chart emissions values
    fig1.update_layout(barmode='stack',
                       title={'font': {'size': 12, 'family': 'Helvetica'}},
                       yaxis_title='CEI (kg CO2e/sf/yr)',
                       yaxis=dict(tickformat='.2f')
                       )
    fig2.update_layout(barmode='stack',
                       title={'font': {'size': 12, 'family': 'Helvetica'}},
                       xaxis_title='Year',
                       yaxis_title='Alternative Compliance Payment ($)',
                       yaxis=dict(tickprefix="$")
                       )

    return fig1, fig2, dash_table.DataTable(
        columns=[{'name': i, 'id': i} for i in df_summary.columns],
        data=df_summary.to_dict('records'),
        style_table={'height': '500px',
                     'overflowY': 'auto',
                     'padding': '10px'},
        style_cell={'text-align': 'center',
                    'font-family': 'Helvetica',
                    'width': '25px',
                    'minWidth': '25px',
                    'maxWidth': '25px',
                    'white-space': 'normal',
                    'height': 'auto'},
        style_header={'backgroundColor': '#979696',
                      'fontWeight': 'bold',
                      'height': '75px'
                      }
    )


# Run the app
if __name__ == '__main__':
    app.run(debug=True)
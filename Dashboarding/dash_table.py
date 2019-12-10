import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_table
from creacard_connectors.database_connector import connect_to_database
import pandas as pd

# extract available schema

engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()


query = """
    select "schema_name"
    from information_schema.schemata
    where "schema_name" !~* '^pg*.' and "schema_name" not in ('information_schema')
"""
data = pd.read_sql(query, con=engine)
list_schema = data["schema_name"].tolist()
del data

engine.close()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__)



app.layout = html.Div([
    html.Div([

        html.Div([
            dcc.Dropdown(
                id='schema_name',
                options=[{'label': i, 'value': i} for i in list_schema],
                value=list_schema[0]
            ),
        ],
        style={'width': '48%', 'display': 'inline-block'})
    ]),

    html.Div([

        html.Div([
            dcc.Dropdown(
                id='table_name'
            ),
        ],
            style={'width': '48%', 'display': 'inline-block'})
    ]),
    html.Div([
        dash_table.DataTable(
            id='table_type'),
    ], className=" twelve columns"),
])

@app.callback(
    [Output('table_name', 'options'),
     Output('table_name', 'value')],
    [Input('schema_name', 'value')]
)
def update_output_div(input_value):

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()

    query = """

    SELECT "table_name"
      FROM "information_schema"."tables"
     WHERE table_schema IN ('{}')

    """.format(input_value)

    data = pd.read_sql(query, con=engine)
    tmp_list = data["table_name"].tolist()
    del data

    if not tmp_list:
        val = ""
    else:
        val = tmp_list[0]

    options = [{'label': i, 'value': i} for i in tmp_list], val

    engine.close()

    return options

@app.callback(
    [Output('table_type', 'columns'),
     Output('table_type', 'rows')],
    [Input('schema_name', 'value'),
     Input('table_name', 'value')]
)
def update_output_div(schema,tlb_name):

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()

    query = """

    select column_name,data_type 
    from information_schema.columns 
    where table_name = '{}'
    and table_schema IN ('{}')


    """.format(schema,tlb_name)
    data = pd.read_sql(query, con=engine)

    engine.close()

    col_name = list({"name": i, "id": i} for i in data.columns)
    datas = data.to_dict('records')


    return col_name, datas



if __name__ == '__main__':
    app.run_server(debug=True)

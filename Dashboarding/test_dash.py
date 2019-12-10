import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import pandas as pd
import plotly.graph_objs as go
from creacard_connectors.database_connector import connect_to_database
from Creacard_Utils.import_credentials import credentials_extractor


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

engine = connect_to_database("Postgres","Creacard_Calypso").CreateEngine()



query_list = """
SELECT table_name FROM information_schema.tables WHERE table_schema='PROFILING'
"""

list_table = pd.read_sql(query_list, con=engine)


app.layout = html.Div([
    html.Div([

        html.Div([
            dcc.Dropdown(
                id='table_name',
                options=[{'label': i, 'value': i} for i in list(list_table["table_name"])],
                value=list_table["table_name"][0]
            ),
        ],
        style={'width': '48%', 'display': 'inline-block'})
    ]),

    dcc.Graph(id='clusters-distance')
])



@app.callback(
    Output('clusters-distance', 'figure'),
    [Input('table_name', 'value')])
def update_graph(_table_name):

    query_clusters = """
    select "ClusterID", "min_distance"
    from "PROFILING"."{}"
    """.format(_table_name)

    df = pd.read_sql(query_clusters, con=engine)

    data = []

    for i in range(0, len(pd.unique(df['ClusterID']))):
        data.append(go.Violin(
            x = df['ClusterID'][df['ClusterID'] == pd.unique(df['ClusterID'])[i]],
            y = df['min_distance'][df['ClusterID'] == pd.unique(df['ClusterID'])[i]],
            name = pd.unique(df['ClusterID'])[i]
        ))

    return {
        'data': data,
        'layout': go.Layout(
            xaxis={'title': 'ClusterID' },
            yaxis={'title': 'distance to the closest centroid'},
            margin={'l': 40, 'b': 40, 't': 10, 'r': 0},
            hovermode='closest'
        )
    }


if __name__ == '__main__':
    app.run_server(debug=True)

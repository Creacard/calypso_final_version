import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import *
import pandas as pd
import plotly.graph_objs as go
from creacard_connectors.creacard_connectors.database_connector import ConnectToPostgres
from Creacard_Utils.import_credentials import credentials_extractor
from six.moves.urllib.parse import quote



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


credentials = credentials_extractor().get_database_credentials("Postgres", "Creacard")
engine = ConnectToPostgres(credentials).CreateEngine()

query_list = """
SELECT table_name FROM information_schema.tables WHERE table_schema='PROFILING'
"""

list_table = pd.read_sql(query_list, con=engine)

query_cluster_id = """
select distinct "ClusterID" as "ClusterID" 
from "PROFILING"."{}"
""".format(list_table["table_name"][0])


clusters_list = pd.read_sql(query_cluster_id, con=engine)

app = dash.Dash()

app.layout = html.Div(children=[
    html.H2("Top Merchant per Cluster"),
    html.Div([
        html.Label('Choose the clustering analysis'),
        dcc.Dropdown(
            id='table_name',
            options=[{'label': i, 'value': i} for i in list(list_table["table_name"])],
            value=list_table["table_name"][0]
        ),
    ],
        style={'width': '48%', 'display': 'inline-block'}),

    html.Div([
        html.Label('Choose the cluster'),
        dcc.Dropdown(
            id='cluster_ID',
            options=[{'label': i, 'value': i} for i in list(clusters_list["ClusterID"])],
            value=clusters_list["ClusterID"][0]
        ),
    ],
        style={'width': '48%', 'display': 'inline-block'}),
    html.Div(id='intermediate-value', style={'display': 'none'}),
    dcc.Graph(id='clusters_merchants'),
    html.A(
        'Download Data',
        id='download-link',
        download="rawdata.csv",
        href="",
        target="_blank"
    )
    ])


@app.callback(
    Output('cluster_ID', 'options'),
    [Input('table_name', 'value')])
def update_clusterID(_table_name):

    query_cluster_id = """
    select distinct "ClusterID" as "ClusterID" 
    from "PROFILING"."{}"
    """.format(str(_table_name))
    ClusterID = pd.read_sql(query_cluster_id, con=engine)

    return [{'label': i, 'value': i} for i in list(ClusterID["ClusterID"])]


@app.callback(
    Output('intermediate-value', 'children'),
    [Input('cluster_ID', 'value'),
     Input('table_name', 'value')])
def update_df(_ClusterID, _table_name):


    query_clusters = """
    SELECT count(*) as "nb_transactions", T1."MerchantName"
    from "TRANSACTIONS"."POS_TRANSACTIONS"  as T1 
    INNER JOIN "PROFILING"."{}" as T2
    ON T1."CardHolderID" = T2."CardHolderID"
    WHERE T2."ClusterID" = '{}' and T1."TransactionTime" > '2017-08-01'
    GROUP BY T1."MerchantName"
    order by 1 DESC 
    LIMIT 10
    """.format(_table_name, _ClusterID)

    df = pd.read_sql(query_clusters, con=engine)

    return df.to_json(date_format='iso', orient='split')


@app.callback(
    Output('clusters_merchants', 'figure'),
    [Input('intermediate-value', 'children')])
def update_graph(jsonified_cleaned_data):

    data = []

    dff = pd.read_json(jsonified_cleaned_data, orient='split')

    data.append(go.Bar(
        y = dff['nb_transactions'],
        x = dff['MerchantName']
    ))

    return {
        'data': data,
        'layout': go.Layout(
            xaxis={'title': 'Merchant Name'},
            yaxis={'title': '# transactions'},
            title= 'Dash Data Visualization'

        )
    }


@app.callback(
    Output('download-link', 'href'),
    [Input('intermediate-value', 'children')])
def update_download_link(jsonified_cleaned_data):

    dff = pd.read_json(jsonified_cleaned_data, orient='split')

    csv_string = dff.to_csv(index=False, encoding='utf-8')
    csv_string = "data:text/csv;charset=utf-8,%EF%BB%BF" + quote(csv_string)

    return csv_string


#@app.callback(Output('table', 'children'), [Input('intermediate-value', 'children')])
#def update_table(jsonified_cleaned_data):

    #dff = pd.read_json(jsonified_cleaned_data, orient='split')

    #return

if __name__ == '__main__':
    app.run_server(debug=True)




from sklearn.cluster import KMeans
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class Kmeans_Method(object):
    _number_clusters = None
    _kmeans_model = None
    _clusters_center = None
    _clusters_labels_table = None
    _clusters_size = None
    _model_output = None

    """
    Inputs:
    _NumberOfClusters = Number of clusters for the kmeans
    _IDlines = pandas dataframe with the name of IDunique of a line
    Outputs:

    """

    def __init__(self, number_clusters=None):
        self._number_clusters = number_clusters

    def fit_transform(self,data,colnames,id_line):
        """
        :param X_Data: pandas data frame with only numerical values
        :return: the kmeans model, clusters labels, ClustersSize and cluster centers
        """
        self._kmeans_model = KMeans(n_clusters=self._number_clusters).fit(data)
        self._clusters_size = pd.DataFrame(self._kmeans_model.labels_, columns=['ClusterID']).groupby('ClusterID').size().reset_index(drop=False).rename(columns={0:"cluster_size"})
        self._clusters_labels_table = pd.DataFrame(np.unique(self._kmeans_model.labels_), columns=['ClusterID'])
        self._clusters_labels_table['ClusterLabels'] = ""
        self._clusters_center = pd.DataFrame(self._kmeans_model.cluster_centers_, columns=colnames)
        _model_output = pd.DataFrame(self._kmeans_model.labels_, columns=['ClusterID'])
        _model_output = pd.concat([id_line, _model_output], axis=1)
        _model_output['ClusterLabels'] = ""

        return _model_output


    def rename_cluster_id(self,cluster_name,new_value):

        self._clusters_labels_table.loc[self._clusters_labels_table.ClusterID == cluster_name, 'ClusterLabels'] = new_value
        self._clusters_size.loc[self._clusters_size.ClusterID == cluster_name, "ClusterID"] = new_value
        self._clusters_center = self._clusters_center .rename({cluster_name: new_value})


    def rename_cluster_label(self,cluster_name,new_value):

        self._clusters_labels_table.loc[self._clusters_labels_table.ClusterLabels == cluster_name, 'ClusterLabels'] = new_value
        self._clusters_size.loc[self._clusters_size.ClusterID == cluster_name, "ClusterID"] = new_value
        self._clusters_center = self._clusters_center.rename({cluster_name: new_value})


def apply_kmeans_model(model, data_clustering, id_line):

    clusters_distance = model._kmeans_model.transform(data_clustering)
    min_vector = clusters_distance.min(axis=1)
    mean_vector_other = (clusters_distance.sum(axis=1) - min_vector) / (clusters_distance.shape[1] - 1)
    std_vector = abs(mean_vector_other - min_vector)

    _final_clustering = None
    _final_clustering = pd.DataFrame(id_line)
    _final_clustering["ClusterID"] = model._kmeans_model.predict(data_clustering)
    _final_clustering["min_distance"] = min_vector
    _final_clustering["avg_distance_other_clusters"] = mean_vector_other
    _final_clustering["std_distance_other_clusters"] = std_vector

    _final_clustering = pd.merge(_final_clustering, model._clusters_labels_table, how="inner", on="ClusterID")

    return _final_clustering

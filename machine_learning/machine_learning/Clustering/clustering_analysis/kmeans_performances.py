from sklearn.ensemble import RandomForestClassifier
import matplotlib.pyplot as plt
import pandas as pd

def train_rf(data_clustering,clusters,num_cluster):

    clf = RandomForestClassifier(n_estimators=50, max_depth=num_cluster, random_state=0)
    clf.fit(data_clustering, clusters)

    return clf


def feature_importance(ml_model, _var_clustering):

    """Plot the importance of variables from a ml models

        Requiered Parameters
        -----------
        ml_model : object
            machine learning model
        data: float
            data used to train the ML model

    """

    _feature_imp = pd.DataFrame(ml_model.feature_importances_)
    _feature_imp.index = _var_clustering
    _feature_imp.columns = ["FeatureImportance"]
    _feature_imp = _feature_imp.sort_values(by='FeatureImportance', ascending=False)
    _feature_imp.iloc[0:10].plot.barh()
    plt.gca().invert_yaxis()

    return _feature_imp

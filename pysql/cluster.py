"""
Clustering.py
====================================
Clustering module
"""

from sklearn.cluster import KMeans
import pandas as pd
import numpy as np


class CustomerPopulation():
    """
    A customer population.

    :param df: Customer population, including consumption and NAICS code
    :type df: Pandas dataframe
    :param d_user: Mapping of column names to standard column names
    :type d_user: dict
    """
    def __init__(self, df, d_user):
        # required columns
        columns = ['CustomerID', 'NAICS', 'Consumption']

        # catch errors
        if(len([x for x in columns if x not in d_user]) > 0):
            raise ValueError('Required columns not specified.')

        # translate column names
        # d_user : user --> class
        # d_class : class --> user
        d_user = d_user
        d_class = dict(map(reversed, d_user.items()))

        df = df.rename(columns=d_class)

        # store class attributes
        self.df = df[columns]
        self.d_user = d_user
        self.d_class = d_class

    def computeDendrogram(self):
        """
        Computes the population dendrogram.
        """
        self.kmeansOnConsumption(n_clusters = 10)

        df = self.df[['CustomerID', 'NAICS', 'Consumption Cluster']].copy()

        # slice NAICS
        df['NAICS_6'] = df['NAICS']
        df['NAICS_4'] = df['NAICS'].apply(lambda x: x[:4] if x else None)
        df['NAICS_2'] = df['NAICS'].apply(lambda x: x[:2] if x else None)

        # form clusters as tuple (Consumption Cluster, NAICS)
        cluster_1 = df.apply(lambda x: (x['Consumption Cluster'], x['NAICS_6']), axis=1)
        cluster_2 = df.apply(lambda x: (x['Consumption Cluster'], x['NAICS_4']), axis=1)
        cluster_3 = df.apply(lambda x: (x['Consumption Cluster'], x['NAICS_2']), axis=1)

        # generate cluster ids
        cluster_id_1 = self.generateClusterIDs(cluster_1)
        cluster_id_2 = self.generateClusterIDs(cluster_2)
        cluster_id_3 = self.generateClusterIDs(cluster_3)
        cluster_id_4 = df['Consumption Cluster'].rename("ClusterID")

        customer_id = df['CustomerID']

        # generate dendrogram
        customer_cluster_1 = pd.concat([customer_id, cluster_id_1], axis=1)
        customer_cluster_2 = pd.concat([customer_id, cluster_id_2], axis=1)
        customer_cluster_3 = pd.concat([customer_id, cluster_id_3], axis=1)
        customer_cluster_4 = pd.concat([customer_id, cluster_id_4], axis=1)

        customer_cluster_1['Height'] = 1
        customer_cluster_2['Height'] = 2
        customer_cluster_3['Height'] = 3
        customer_cluster_4['Height'] = 4

        dendrogram = pd.concat([customer_cluster_1,
                                customer_cluster_2,
                                customer_cluster_3,
                                customer_cluster_4], sort=True)

        # filter out -1 values, corresponds with NULL NAICS
        dendrogram = dendrogram.loc[~(dendrogram["ClusterID"] == -1)].reset_index(drop=True).copy()

        # store as attribute
        self.dendrogram = dendrogram[['CustomerID', 'Height', 'ClusterID']]

    def kmeansOnConsumption(self, n_clusters):
        consumption = self.df[['Consumption']]

        log_consumption = consumption.apply(lambda x : np.log(x))

        # create X input
        X = np.array(log_consumption)

        # predict Y (clusters)
        Y = KMeans(n_clusters=n_clusters).fit_predict(X)

        self.df['Consumption Cluster'] = Y

    def generateClusterIDs(self, cluster_series):
        # unique clusters
        clusters = [x for x in set(cluster_series) if x[1] is not None]
        clusters.sort()

        # map clusters to IDs
        d = {}
        for c, value in enumerate(clusters, 1):
            d[value] = c

        cluster_ids = cluster_series.map(d).fillna(-1).apply(int).rename("ClusterID")

        return cluster_ids


    def getPopulation(self):
        """
        Returns the population consumption and NAICS information.
        """
        return self.df

    def getDendrogram(self):
        """
        Returns the population dendrogram.
        """
        return self.dendrogram
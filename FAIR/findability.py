"""
FAIR.findability
~~~~~~~~~~~~~~~~

This module implements the assesments of Findability principle of FAIR data.
"""

import sys

import pandas as pd
from SPARQLWrapper import SPARQLWrapper

import utils
sys.path.append('../project')


def findability_properties(connection: SPARQLWrapper, datasets_query: str, prefixes: str = None) -> pd.DataFrame:
    """
    Finds properties of Findability principle of FAIR data - currently the dataset's publisher's name
    (important note: it needs to be provided in the input query), its title in English (if present),
    its number of keywords, its date of being issued (if present), its type (if present)
    and the location associated with it (if present).
        :param connection: SPRAQLWrapper connection
        :param datasets_query: SPARQL query to get the datasets
        :param prefixes: prefixes to be used in the SPARQL query
        :return: pd.DataFrame with the dataset URIs and the values of the properties
    """
    query = """
        SELECT DISTINCT ?datasetURI ?publisher
        (IF(LANG(?datasetTitle) = "" || LANG(?datasetTitle) = "en", ?datasetTitle, "") as ?englishTitle)
         (COUNT(?keyword) AS ?keywords) ?issuedDate ?datasetType ?datasetLocation
         WHERE
         {
            {   
            """ + datasets_query + """
            }
            OPTIONAL {?datasetURI dct:title ?datasetTitle.}
            OPTIONAL { ?datasetURI dcat:keyword ?keyword. }
            OPTIONAL { ?datasetURI dct:issued ?issuedDate. }
            OPTIONAL {?datasetURI dct:type ?datasetType. }
            OPTIONAL {?datasetURI dct:spatial ?datasetLocation. }
         }
    """
    return utils.SPARQL_to_df(connection, query, prefixes)

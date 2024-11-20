"""
FAIR.interoperability
~~~~~~~~~~~~~~~~~~~~~

This module implements the assesments of Interoperability principle of FAIR data.
"""
import sys

import pandas as pd
from SPARQLWrapper import SPARQLWrapper

import utils
sys.path.append('../project')


def interoperability_properties(connection: SPARQLWrapper, datasets_query: str, prefixes: str = None) -> pd.DataFrame:
    """
    Finds properties of Interoperability principle of FAIR data - currently the number of resources
    (publications, etc.) pointing to it, its language (if present), access rights associated with it
    and the dataset for which it is a version, edition or an adaptation of (if present).
        :param connection: SPRAQLWrapper connection
        :param datasets_query: SPARQL query to get the datasets
        :param prefixes: prefixes to be used in the SPARQL query
        :return: pd.DataFrame with the dataset URIs and the values of the properties
    """
    query = """
        SELECT DISTINCT ?datasetURI 
        (COUNT(?isReferencedBy) as ?referencesNumber) 
        ?datasetLanguage ?accessRights ?isVersionOf
        
         WHERE
         {
            {   
            """ + datasets_query + """
            }
            OPTIONAL {?datasetURI dct:isReferencedBy ?isReferencedBy. }
            OPTIONAL {?datasetURI dct:language ?datasetLanguage. }
            OPTIONAL {?datasetURI dct:accessRights ?accessRights. }
            OPTIONAL {?datasetURI dct:isVersionOf ?isVersionOf. }
         }
    """
    return utils.SPARQL_to_df(connection, query, prefixes)

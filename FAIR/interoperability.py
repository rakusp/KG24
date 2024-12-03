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


def interoperability_aggregated_properties(connection: SPARQLWrapper, datasets_query: str,
                                           prefixes: str = None, aggregate_variable: str = "publisher") -> pd.DataFrame:
    """
        Finds aggregated properties of Interoperability principle of FAIR data.
         (the same properties as interoperability_properties)
        """
    query = f"""
    SELECT ?{aggregate_variable}
        (COUNT(DISTINCT ?dataset) AS ?totalDatasets)
        (AVG(?referencesNumber) AS ?avgReferences)
        ((100.0 * SUM(?hasLanguage) / COUNT(DISTINCT ?dataset)) AS ?percentageWithLanguage)
        ((100.0 * SUM(?hasAccessRights) / COUNT(DISTINCT ?dataset)) AS ?percentageWithAccessRights)
        ((100.0 * SUM(?hasVersionOf) / COUNT(DISTINCT ?dataset)) AS ?percentageBeingVersionOf)
        ((100.0 * SUM(?hasIdentifier) / COUNT(DISTINCT ?dataset)) AS ?percentageWithIdentifier)
        ((100.0 * SUM(?hasRightsHolder) / COUNT(DISTINCT ?dataset)) AS ?percentageWithRightsHolder)
    WHERE {{
        {{
            SELECT ?dataset ?{aggregate_variable}
                (COUNT(DISTINCT ?isReferencedBy) AS ?referencesNumber)
                ?hasLanguage ?hasAccessRights ?hasVersionOf ?hasIdentifier ?hasRightsHolder
            WHERE {{
                {datasets_query}
                
                OPTIONAL {{ ?dataset dct:isReferencedBy ?isReferencedBy. }}
                OPTIONAL {{ ?dataset dct:language ?language }}
                OPTIONAL {{ ?dataset dct:accessRights ?accessRights }}
                OPTIONAL {{ ?dataset dct:isVersionOf ?isVersionOf }}
                OPTIONAL {{ ?dataset dct:identifier ?identifier }}
                OPTIONAL {{ ?dataset dct:rightsHolder ?rightsHolder }}
                
                BIND (IF(BOUND(?language), 1, 0) AS ?hasLanguage)
                BIND (IF(BOUND(?accessRights), 1, 0) AS ?hasAccessRights)
                BIND (IF(BOUND(?isVersionOf), 1, 0) AS ?hasVersionOf)
                BIND (IF(BOUND(?identifier), 1, 0) AS ?hasIdentifier)
                BIND (IF(BOUND(?rightsHolder), 1, 0) AS ?hasRightsHolder)
            }}
            GROUP BY ?dataset ?{aggregate_variable} ?hasLanguage ?hasAccessRights ?hasVersionOf ?hasIdentifier ?hasRightsHolder
        }}
    }}
    GROUP BY ?{aggregate_variable} 
    """
    return utils.SPARQL_to_df(connection, query, prefixes)

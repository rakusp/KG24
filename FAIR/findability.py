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
         (COUNT(?keyword) AS ?keywords) ?issuedDate ?datasetType ?datasetLocation ?isPartOf
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
            OPTIONAL {?datasetURI dct:isPartOf ?isPartOf. }
         }
    """
    return utils.SPARQL_to_df(connection, query, prefixes)


def findability_aggregated_properties(connection: SPARQLWrapper, datasets_query: str,
                                      prefixes: str = None, aggregate_variable: str = "publisher") -> pd.DataFrame:
    """
    Finds aggregated properties of Findability principle of FAIR data.
    (the same properties as findability_properties)
    """
    query = f"""
    SELECT ?{aggregate_variable}
        (COUNT(DISTINCT ?dataset) AS ?totalDatasets)
        (AVG(?keywordCount) AS ?avgKeywords)
        ((100.0 * SUM(?hasTitle) / COUNT(DISTINCT ?dataset)) AS ?percentageWithTitle)
        ((100.0 * SUM(?hasIssuedDate) / COUNT(DISTINCT ?dataset)) AS ?percentageWithIssuedDate)
        ((100.0 * SUM(?hasLocation) / COUNT(DISTINCT ?dataset)) AS ?percentageWithLocation)
        ((100.0 * SUM(?hasType) / COUNT(DISTINCT ?dataset)) AS ?percentageWithType)
        ((100.0 * SUM(?hasPartOf) / COUNT(DISTINCT ?dataset)) AS ?percentageWithPartOf)
    WHERE {{
        {{
            SELECT ?dataset ?{aggregate_variable} 
                (COUNT(DISTINCT ?keyword) AS ?keywordCount)
                ?hasTitle ?hasIssuedDate ?hasLocation ?hasType ?hasPartOf
            WHERE {{
                {datasets_query}
                
                OPTIONAL {{ ?dataset dct:title ?title }}
                OPTIONAL {{ ?dataset dct:issued ?issuedDate }}
                OPTIONAL {{ ?dataset dct:type ?datasetType }}
                OPTIONAL {{ ?dataset dct:spatial ?datasetLocation }}
                OPTIONAL {{ ?dataset dct:isPartOf ?isPartOf }}
                
                BIND (IF(BOUND(?title), 1, 0) AS ?hasTitle)
                BIND (IF(BOUND(?issuedDate), 1, 0) AS ?hasIssuedDate)
                BIND (IF(BOUND(?datasetLocation), 1, 0) AS ?hasLocation)
                BIND (IF(BOUND(?datasetType), 1, 0) AS ?hasType)
                BIND (IF(BOUND(?isPartOf), 1, 0) AS ?hasPartOf)
            }}
            GROUP BY ?dataset ?{aggregate_variable} ?keywordCount ?hasTitle ?hasIssuedDate ?hasLocation ?hasType ?hasPartOf
        }}
    }}
    GROUP BY ?{aggregate_variable}
    """
    return utils.SPARQL_to_df(connection, query, prefixes)

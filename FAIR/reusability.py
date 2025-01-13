"""
FAIR.reusability
~~~~~~~~~~~~~~~~

This module implements the assesments of Reusability principle of FAIR data.
"""

import sys

import pandas as pd
from SPARQLWrapper import SPARQLWrapper

import utils
sys.path.append('../project')


def publisher_provenance_contant(connection: SPARQLWrapper, datasets_query: str, prefixes: str = None) -> pd.DataFrame:
    """
    Check if the distributions contain publisher, provenance and contact information
    :param connection: SPRAQLWrapper connection
    :param datasets_query: SPARQL query to get the datasets
    :param prefixes: prefixes to be used in the SPARQL query
    :return: pd.DataFrame with the dataset URIs and the accessURLs and downloadURLs
    """

    query = """
        SELECT (?d AS ?dataset) ?publisher ?provenance ?contact WHERE
        {
            {
            """ + datasets_query + """
            }
            OPTIONAL {
                ?d dct:publisher ?publisher .
            }
            OPTIONAL {
                ?d dct:provenance ?provenance
            }
            OPTIONAL {
                ?d dcat:contactPoint ?contact
            }
        }
        """
    return utils.SPARQL_to_df(connection, query, prefixes)


def reusability_aggregated_properties(connection: SPARQLWrapper, datasets_query: str,
                                      prefixes: str = None, aggregate_variable: str = "publisher") -> pd.DataFrame:
    """
    Finds aggregated properties of Reusability principle of FAIR data.
    """
    query = f"""
    SELECT ?{aggregate_variable}
        (COUNT(DISTINCT ?dataset) AS ?totalDatasets)
        ((100.0 * SUM(?hasConformsTo) / COUNT(DISTINCT ?dataset)) AS ?percentageWithConformsTo)
        ((100.0 * SUM(?hasDescription) / COUNT(DISTINCT ?dataset)) AS ?percentageWithDescription)
        ((100.0 * SUM(?hasProvenance) / COUNT(DISTINCT ?dataset)) AS ?percentageWithProvenance)
        ((100.0 * SUM(?hasIdentifier) / COUNT(DISTINCT ?dataset)) AS ?percentageWithIdentifier)
        ((100.0 * SUM(IF(?issuedCount > ?distributionCount, ?distributionCount, ?issuedCount)) / SUM(?distributionCount)) AS ?percentageWithIssued)
        ((100.0 * SUM(IF(?rightsCount > ?distributionCount, ?distributionCount, ?rightsCount)) / SUM(?distributionCount)) AS ?percentageWithRights)
        ((100.0 * SUM(IF(?formatCount > ?distributionCount, ?distributionCount, ?formatCount)) / SUM(?distributionCount)) AS ?percentageWithFormat)
    WHERE {{
        {{
            SELECT ?dataset ?{aggregate_variable}
                (COUNT(DISTINCT ?distribution) AS ?distributionCount)
                ?hasConformsTo ?hasDescription ?hasProvenance ?hasIdentifier
                (SUM(?hasIssued) AS ?issuedCount)
                (SUM(?hasRights) AS ?rightsCount)
                (SUM(?hasFormat) AS ?formatCount)
            WHERE {{
                {datasets_query}
                
                OPTIONAL {{?dataset dct:conformsTo ?conformsTo}}
                OPTIONAL {{?dataset dct:description ?description}}
                OPTIONAL {{?dataset dct:provenance ?provenance}}
                OPTIONAL {{?dataset dct:identifier ?identifier}}
                OPTIONAL {{
                    ?dataset dcat:distribution ?distribution 
                    OPTIONAL {{?distribution dct:issued ?issued}}
                    OPTIONAL {{?distribution dct:rights ?rights}}
                    OPTIONAL {{?distribution dct:format ?format}}
				}}

                BIND (IF(BOUND(?conformsTo), 1, 0) AS ?hasConformsTo)
                BIND (IF(BOUND(?description), 1, 0) AS ?hasDescription)
                BIND (IF(BOUND(?provenance), 1, 0) AS ?hasProvenance)
                BIND (IF(BOUND(?identifier), 1, 0) AS ?hasIdentifier)
                BIND (IF(BOUND(?issued), 1, 0) AS ?hasIssued)
                BIND (IF(BOUND(?rights), 1, 0) AS ?hasRights)
                BIND (IF(BOUND(?format), 1, 0) AS ?hasFormat)
            }}
            GROUP BY ?dataset ?{aggregate_variable} ?hasConformsTo ?hasDescription ?hasProvenance ?hasIdentifier
        }}
    }}
    GROUP BY ?{aggregate_variable}
    """

    return utils.SPARQL_to_df(connection, query, prefixes)
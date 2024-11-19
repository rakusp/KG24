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

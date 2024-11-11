"""
FAIR.accessibility
~~~~~~~~~~~~~~~~~~

This module implements the assesments of Accessibility principle of FAIR data.
"""
import sys
import utils
import requests
import pandas as pd

sys.path.append('../project')


def is_url_ok(url):
    """
    Check if a URL is valid and accessible.
    Allows redirects (HTTP 301 and 302).
    """
    try:
        r = requests.head(url, allow_redirects=True)
        return r.status_code == 200
    except requests.ConnectionError:
        return False


def distribution_links(connection, datasets_query, prefixes=None):
    """
    Check if the distributions contain accessURLs
    and optionally if they can be downloaded directly
    :param connection: SPRAQLWrapper connection
    :param datasets_query: SPARQL query to get the datasets
    :param prefixes: prefixes to be used in the SPARQL query
    :return: pd.DataFrame with the dataset URIs and the accessURLs and downloadURLs
    """

    query = """
        SELECT (?d AS ?dataset) (?distr AS ?distribution) ?accessURL ?downloadURL WHERE
        {
            {
            """ + datasets_query + """
            }
            OPTIONAL {
                ?d dcat:distribution ?distr
                OPTIONAL {
                    ?distr dcat:accessURL ?accessURL
                    OPTIONAL {
                        ?distr dcat:downloadURL ?downloadURL .
                }
              }
          }
        }
        """
    return utils.SPARQL_to_df(connection, query, prefixes)

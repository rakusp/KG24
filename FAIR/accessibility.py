"""
FAIR.accessibility
~~~~~~~~~~~~~~~~~~

This module implements the assesments of Accessibility principle of FAIR data.
"""
import sys

from SPARQLWrapper import SPARQLWrapper

import utils
import requests
import pandas as pd

sys.path.append('../project')


def is_url_ok(url: str) -> bool:
    """
    Check if a URL is valid and accessible.
    Allows redirects (HTTP 301 and 302).
    """
    try:
        r = requests.head(url, allow_redirects=True)
        return r.status_code == 200
    except requests.ConnectionError:
        return False


def distribution_links(connection: SPARQLWrapper, datasets_query: str, prefixes: str = None) -> pd.DataFrame:
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


def accessibility_aggregated_properties(connection: SPARQLWrapper, datasets_query: str,
                                      prefixes: str = None, aggregate_variable: str = "publisher") -> pd.DataFrame:
    """
    Finds aggregated properties of Accessibility principle of FAIR data.
    """

    query = f"""
    SELECT ?{aggregate_variable}
           (COUNT(DISTINCT ?dataset) AS ?totalDatasets)
           (AVG(?distributionCount) AS ?avgDistributionCount)
           ((100.0 * SUM(IF(?licenseCount > ?distributionCount, ?distributionCount, ?licenseCount)) / SUM(?distributionCount)) AS ?percentageWithLicense)
           ((100.0 * SUM(IF(?accessRightsCount > ?distributionCount, ?distributionCount, ?accessRightsCount)) / SUM(?distributionCount)) AS ?percentageWithAccessRights)
           ((100.0 * SUM(IF(?accessURLCount > ?distributionCount, ?distributionCount, ?accessURLCount)) / SUM(?distributionCount)) AS ?percentageWithAccessURL)
           ((100.0 * SUM(IF(?downloadURLCount > ?distributionCount, ?distributionCount, ?downloadURLCount)) / SUM(?distributionCount)) AS ?percentageWithDownloadURL)
    WHERE {{
        {{
            SELECT ?dataset ?{aggregate_variable}
                   (COUNT(DISTINCT ?distribution) AS ?distributionCount)
                   (SUM(?hasLicense) AS ?licenseCount)
                   (SUM(?hasAccessRights) AS ?accessRightsCount)
                   (SUM(?hasAccessURL) AS ?accessURLCount)
                   (SUM(?hasDownloadURL) AS ?downloadURLCount)
            WHERE {{
                {datasets_query}
    
                OPTIONAL {{
                    ?dataset dcat:distribution ?distribution
                    OPTIONAL {{
                        ?distribution dcat:accessURL ?accessURL
                    }}
                    OPTIONAL {{
                        ?distribution dcat:downloadURL ?downloadURL
                    }}
                    OPTIONAL {{
                        ?distribution dct:license ?license
                    }}
                    OPTIONAL {{
                        ?distribution dct:accessRights ?accessRights
                    }}
                }}
                OPTIONAL {{
                    ?dataset dct:accessRights ?accessRightsGlobal
                }}
    
                BIND (IF(BOUND(?license), 1, 0) AS ?hasLicense)
                BIND (IF(BOUND(?accessRights) || BOUND(?accessRightsGlobal), 1, 0) AS ?hasAccessRights)
                BIND (IF(BOUND(?distribution), 1, 0) AS ?hasDistribution)
                BIND (IF(BOUND(?accessURL), 1, 0) AS ?hasAccessURL)
                BIND (IF(BOUND(?downloadURL), 1, 0) AS ?hasDownloadURL)
            }}
            GROUP BY ?dataset ?{aggregate_variable}
        }}
    }}
    GROUP BY ?{aggregate_variable}
    """
    return utils.SPARQL_to_df(connection, query, prefixes)
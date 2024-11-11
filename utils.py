from SPARQLWrapper import JSON
import pandas as pd


def SPARQL_to_df(connection, query, prefixes=None):
    """
    Execute a SPARQL query and return the result as a pandas DataFrame.
    """
    def get_value(x): # in case of missing values
        try:
            return x['value']
        except TypeError:
            return None

    connection.setQuery(prefixes + query)
    connection.setReturnFormat(JSON)
    result = connection.query().convert()

    return pd.DataFrame(result['results']['bindings']).map(get_value)

from google.cloud import bigquery

class GCPConnections:
    # Set up Big Query Connection
    def conn_bigquery_client(self):
        bigquery_client = bigquery.Client()
        return bigquery_client
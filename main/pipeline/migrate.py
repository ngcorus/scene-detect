"""
Update to parse csv file and migrate
"""
#
# from main.scripts.connectors import GCPConnections
# import pandas as pd
# from google.cloud import bigquery
#
# class MigrateSDClass:
#     def __init__(self):
#         # Creat BQ client
#         self.client = GCPConnections.conn_bigquery_client(self)
#
#     def migratescenedetectdata(self):
#         data = pd.read_csv('../parsed-data/test.csv', header = 'infer')
#         bigquery_table = 'content-dev-c058.raw_context_target.test-scene-detection-data'
#         job_config = bigquery.LoadJobConfig(
#             schema=[
#                 bigquery.SchemaField('Id', bigquery.enums.SqlTypeNames.STRING),
#                 bigquery.SchemaField('Scenes', bigquery.enums.SqlTypeNames.STRING),
#                 bigquery.SchemaField('Frames', bigquery.enums.SqlTypeNames.STRING)
#             ]
#         )
#         job = self.client.load_table_from_dataframe(data, bigquery_table, job_config=job_config)
#         job.result()
#         table = self.client.get_table(bigquery_table)
#         print("Loaded {} rows and {} columns to {}.".format(table.num_rows, len(table.schema), table))
#
#
# migrateSDClass = MigrateSDClass()
# migrateSDClass.migratescenedetectdata()
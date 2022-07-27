import time
import os
import pandas as pd
import boto3
import csv
import botocore
import configparser
from main.scripts.scenedetector import SceneDetect

class SceneDetectIngest:

    # Grab guids from a file and append to tracker file
    def setTracker(self,station_):
        print('getting guids...')
        trackerfile = '../parsed-data/tracker.csv'
        if os.path.exists(trackerfile):
            os.remove(trackerfile)
        get_station = pd.read_csv(f'../parsed-data/{station_}.csv')
        group_by_guid = get_station.groupby(['Id'], sort=False)
        for id, allcolumns in group_by_guid:
            with open(trackerfile, 'a') as f:
                f.write(f'{id}\n')

    # Create list of tokens against which the current file is checked to identify if it has already been processed
    def tokens(self,station_):
        self.setTracker(station_)
        tracker = pd.read_csv('../parsed-data/tracker.csv', header=None)
        group_by_guid = tracker.groupby([0], sort=False)
        tokens = []
        for guid, allcolumns in group_by_guid:
            tokens.append(guid)
        return tokens


    def scenedetectingest(self):
        config = configparser.ConfigParser()
        config.read(r'../config/config.ini')

        # AWS Credentials
        aws_access_key_id = config.get('aws', 'aws_access_key_id')
        aws_secret_access_key = config.get('aws', 'aws_secret_access_key')
        aws_session_token = config.get('aws', 'aws_session_token')
        region_name = config.get('aws', 'region_name')

        # S3 Bucket
        station = 'adult swim'
        bucket = "aiptaxonomy"
        prefix = f'final-rekognition/{station}/'

        # Connect to AWS s3 resource
        s3 = boto3.client( 's3',
                            region_name = region_name,
                            aws_access_key_id = aws_access_key_id,
                            aws_secret_access_key = aws_secret_access_key,
                            aws_session_token = aws_session_token
        )

        # get token list - run this function if the ingest was previously interrupted
        get_tokens = self.tokens(station)
        paginator = s3.get_paginator('list_objects')
        # count = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            # scenesdetected = []
            # station_name = ''
            for i in range(len(page['Contents'])):
                filename = page['Contents'][i]['Key']
                if filename.endswith('.mp4'):
                    scenesdetected = []
                    # guid = filename.split('/')[3]
                    filename_split = filename.split('/')
                    guid = filename_split[3]
                    station_name = filename_split[1]
                    if guid not in get_tokens:
                        try:
                            print(filename, guid)
                            s3.download_file(bucket, filename, f'../videos/{station_name}.mp4')
                            data = SceneDetect.detectscene(self,f'../videos/{station_name}.mp4', guid)
                            scenesdetected.append(data)
                        except botocore.exceptions.ClientError as e:
                            if e.response['Error']['Code'] == "404":
                                print("The object does not exist.")
                            else:
                                raise
                        self.savetoCSV(scenesdetected, station_name)
                        # count +=1
                        # print(count)
                        # if count >= 3:
                            # self.savetoCSV(scenesdetected, station_name)
                            # break
            # print(scenesdetected)
            # self.savetoCSV(scenesdetected, station_name)
            # break

    # Export to csv file
    def savetoCSV(self, scenesdetected, station_name):
        fields = ["Id", "Scenes", "Frames"]
        for i in range(len(scenesdetected)):
            print('Writing to CSV..')
            # if (i == 0):
            #     # writing to csv file
            #     with open(f'../parsed-data/test.csv', 'w', encoding="utf-8", newline='') as csvfile:
            #         # creating a csv dict writer object
            #         writer = csv.DictWriter(csvfile, fieldnames=fields)
            #         writer.writeheader()
            #         writer.writerow(scenesdetected[i])
            # else:
            #     # writing to csv file
            #     with open(f'../parsed-data/test.csv', 'a', encoding="utf-8", newline='') as csvfile:
            #         # creating a csv dict writer object
            #         writer = csv.DictWriter(csvfile, fieldnames=fields)
            #         writer.writerow(scenesdetected[i])
            write_csv_to = f'../parsed-data/{station_name}.csv'
            if not os.path.isfile(write_csv_to):
                with open(write_csv_to, 'w', encoding="utf-8", newline='') as csvfile:
                    # creating a csv dict writer object
                    writer = csv.DictWriter(csvfile, fieldnames=fields)
                    writer.writeheader()
                    writer.writerow(scenesdetected[i])
            else:
                with open(write_csv_to, 'a', encoding="utf-8", newline='') as csvfile:
                    # creating a csv dict writer object
                    writer = csv.DictWriter(csvfile, fieldnames=fields)
                    writer.writerow(scenesdetected[i])
        return

scenes = SceneDetectIngest()
scenes.scenedetectingest()
import boto3, os
import datetime, time
import uuid
from boto3.s3.transfer import TransferConfig
from dateutil.tz import tzutc
from dotenv import load_dotenv

class S3Downloader:
    def __init__(self, bucket, key, aws_id, aws_key, destPathPrefix):
        self.bucket = bucket
        self.key = key
        self.aws_id = aws_id
        self.aws_key = aws_key
        self.destPathPrefix = destPathPrefix

    def download_dir(self, client, flatFolder=True):
        """
        params:
        - local: local path to folder in which to place files
        - bucket: s3 bucket with target contents
        - client: initialized s3 client object
        """
        keys = []
        dirs = []
        next_token = ''
        scraperMetadata = []
        base_kwargs = {
            'Bucket':self.bucket,
            'Prefix':self.key,
        }
        while next_token is not None:
            kwargs = base_kwargs.copy()
            if next_token != '':
                kwargs.update({'ContinuationToken': next_token})
            results = client.list_objects_v2(**kwargs)
            contents = results.get('Contents')
            for i in contents:
                k = i.get('Key')
                if k[-1] != '/':
                    keys.append(k)
                else:
                    dirs.append(k)
            next_token = results.get('NextContinuationToken')
        for d in dirs:
            dest_pathname = os.path.join(local, d)
            if not os.path.exists(os.path.dirname(dest_pathname)) and flatFolder == False:
                os.makedirs(os.path.dirname(dest_pathname))
        for k in keys:
            if flatFolder == False:
                dest_pathname = os.path.join(local, k)
            else:
                dest_pathname = os.path.join(local, k.split('/')[-1].strip())
            
            #Get File Metadata
            response = client.head_object(Bucket=bucket, Key=k)
            response.update({'bucket':bucket, 'key':k})
            scraperMetadata.append({'key':k, 'metadata':response})
            
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
            client.download_file(bucket, k, dest_pathname)

        return scraperMetadata

    def download(self):
        
        s3 = boto3.client('s3', aws_access_key_id=self.aws_id, 
                                aws_secret_access_key=self.aws_key)

        scraperMetadata = self.download_dir(s3)
        return (scraperMetadata, True)
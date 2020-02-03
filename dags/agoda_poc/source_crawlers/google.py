import requests
import os
import re

class GoogleDriveDownloader:
    def __init__(self, fileId, destPathPrefix):
        self.fileId = fileId
        self.destPathPrefix = destPathPrefix
        if not os.path.isdir(self.destPathPrefix):
            os.makedirs(self.destPathPrefix)

    def downloadFileInChunks(self, downloadURL):
        with requests.get(downloadURL, stream=True) as r:
            r.raise_for_status()
            with open(self.destPathPrefix+'/tempFile', 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
                        # f.flush()
        cd = r.headers['content-disposition']
        fname = re.findall("filename=(.+)", cd)[0].split(';')[0].replace('\"','')
        os.rename(f"{self.destPathPrefix}/tempFile", f"{self.destPathPrefix}/{fname}")
        r.close()
        return fname

    def download(self):
        downloadURL = f'https://drive.google.com/uc?id={self.fileId}'
        fname = self.downloadFileInChunks(downloadURL)
        return fname, True

import requests
import os
import re
import urllib
from base64 import b64encode as b64e
import json
from urllib.parse import unquote

class OneDriveDownloader:
    def __init__(self, shareLink, destPathPrefix):
        self.shareLink = shareLink
        self.destPathPrefix = destPathPrefix
        if not os.path.isdir(self.destPathPrefix):
            os.makedirs(self.destPathPrefix)

    def getEncodedSharingUrl(self):
        encoded = self.shareLink.encode()
        return 'u!'+b64e(encoded).decode().rstrip('=').replace('/','_').replace('+','-')

    def downloadFileInChunks(self, downloadUrl):
        with requests.get(downloadUrl, stream=True) as r:
            r.raise_for_status()
            with open(self.destPathPrefix+'/tempFile', 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
                        # f.flush()
        cd = r.headers['content-disposition']
        fname = re.findall("filename(.+)", cd)[0].replace('*=UTF-8\'\'','')
        fname = unquote(fname)
        print(fname)
        os.rename(f"{self.destPathPrefix}/tempFile", f"{self.destPathPrefix}/{fname}")
        r.close()
        return fname

    def download(self):
        encodedSharingUrl = self.getEncodedSharingUrl()
        downloadUrl = f"https://api.onedrive.com/v1.0/shares/{encodedSharingUrl}/root/content"
        fname = self.downloadFileInChunks(downloadUrl)
        return fname, True

import youtube_dl
import os


class YoutubeDownloader:
    def __init__(self, url, destPathPrefix):
        self.download_url = url
        self.destPathPrefix = destPathPrefix
        if not os.path.isdir(self.destPathPrefix):
            os.makedirs(self.destPathPrefix)

    def download(self):
        ydl_opts = {'outtmpl': self.destPathPrefix+'/%(title)s%(ext)s'}
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            ydl.download([self.download_url])
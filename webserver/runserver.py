import sys
import io
import requests
import json
from flask_cors import CORS, cross_origin
import os
from flask import Flask, make_response, request, current_app
from flask import jsonify, Response, render_template
from flask_restful import Resource, Api


app = Flask(__name__)
api = Api(app)

airflowURL = 'http://localhost:8080/airflow/api/experimental/dags/source-crawler/dag_runs'
tempDownloads = os.path.join(os.getcwd()+'/tempDownloads')

@app.route('/agoda', methods=['GET'])
@cross_origin()
def getWebPage():
    return render_template("index.html")

@app.route('/agoda/putvideo', methods=['POST'])
@cross_origin()
def addtoqueue():
    payload = {'conf':{'resource_key':request.get_json(force=True)}}
    headers = {
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        }

    response = requests.request("POST", airflowURL, data=json.dumps(payload), headers=headers)
    return response.text

@app.route('/agoda/content', methods=['GET'])
@cross_origin()
def getContentList():
    contentList = os.listdir(tempDownloads)
    contentListString = f"Showing directory list of {tempDownloads}<br><br>"

    for f in contentList:
      if os.path.isfile(f"{tempDownloads}/{f}"):
        contentListString = contentListString + f'<b>{f}</b> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; {os.path.getsize(f"{tempDownloads}/{f}")} <br>'
      else:
        contentListString = contentListString + f'<b>{f}</b> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Folder <br>'

    return contentListString


@app.route('/agoda/health', methods=['GET'])
@cross_origin()
def health():
    return "OK"

#Flask Server Code
DEBUG = 1
port = int(os.environ.get('PORT',8083))
host = '127.0.0.1' if DEBUG else '0.0.0.0'

if __name__ == "__main__":
    if DEBUG:
      app.run(host=host,port=port,debug = DEBUG)
      print ("Voice Auth server started on port " + port)
    else:
      context = ('/etc/apache2/ssl/knolskape_public.crt', '/etc/apache2/ssl/knolskape_private.key')
      app.run(host='0.0.0.0',port=8083,debug = DEBUG, ssl_context=context)
      print ("Voice Auth server started on port " + port)

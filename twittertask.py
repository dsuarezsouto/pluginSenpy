import datetime
from datetime import date,timedelta
import json
import random
import imp
import re
import requests
import os
import time
from rdflib import Graph, plugin
from rdflib.serializer import Serializer

import luigi
from luigi.contrib.esindex import CopyToIndex
import subprocess
from twitter import *

import requests

from time import localtime,strftime
global keys
keys=["AIzaSyARXfonr3RDXQcx8WW1DH4XVbtyw6gpPdE","AIzaSyDbNb9mF_4gcsjj5nBzaDsGAa_2BjyBXL8",
      "AIzaSyA4Z1aY6AqMupXK2PEUtGhE6tlsZNWSjYE","AIzaSyC4I3lQ3TVBbdCHExY9nG9q8eA1HQM7_Ak",
      "AIzaSyCffeDCEhhxrzOZSElEOCOVLo74Tvpc7mg","AIzaSyCCR7MTO2JvjCC6i6BDuoBoqNav3P4pjVU",
      "AIzaSyDHQFvlUCpLUumNpjCDelXJOcymTOEPqgI","AIzaSyD7b7Kb2PpX0t4BPWb5IsMD_dd0OrMaGaU",
      "AIzaSyBt0g9YBfueoDVlhtoF0iWfP6kOssZEOco","AIzaSyBAafBm_xxV0RUNQphUbVJ3pcb96LI0RBw",
      "AIzaSyDIVrCq7PpWDFWpXafHzLdcoumwGxBkBEE","AIzaSyDZmhApFqZkopZ1LqNzRbDoCOz6m-I-ec8",
      "AIzaSyBtMlO5VhQ8zF_SndzAaCG8oq0AaRR3qAY","AIzaSyBgv5082XwL7hBU8E5-BgxZcRvNMxZrOrk"]

global index
index = 0
global key
key=keys[index]
global count
count=0


def analizeInsomniaSenpy(tweet):
        r = requests.get("http://localhost:5000/api?algo=insomniaDetector&i={}".format(tweet))
        response=r.json()
        is_insomniac=response['entries'][0]['is_insomniac']

        if is_insomniac:
            theme=response['entries'][0]['theme']
            return is_insomniac,theme
        else:
            return None
def analizeSentimentSenpy(tweet):
        r = requests.get("http://senpy.cluster.gsi.dit.upm.es/api/?algo=sentiText&i={}&language=es".format(tweet))
        response=r.json()
        sentiment=response['entries'][0]['sentiments']

        if sentiment:
            return sentiment
        else:
            return None

def analizeLocation(tweet):
    global keys
    global index
    global key
    global count
    latitud= 0.0
    longitud= 0.0
    location=tweet["user"]["location"]
    
    try:
        r = requests.get("https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}".format(location,key))
        #time.sleep(1)
        response=r.json()        
        latitud=response['results'][0]["geometry"]["location"]["lat"]
        longitud=response['results'][0]["geometry"]["location"]["lng"]
        
    except:
        print("Error al geolocalizar {} :(".format(location))
        print(response)

        latitud="ERROR AL GEOLOCALIZAR"
        longitud="ERROR AL GEOLOCALIZAR"
    #else:
        
        #geolocationText = "{},{}".format(geolocationi["lat"],geolocationi["lng"])
   
    count +=1
    if (count ==2495):
        if(index == len(keys)-1):
            print("Usadas todas las keys")
            return 
        index +=1
        key=keys[index]
        print("Cambio a la key {}".format(index+1))
        count = 0
    
    #geoLocatedTweets["geoLocation"]=geolocation
    tweet["Longitud"]=longitud
    tweet["Latitud"]=latitud
    return tweet

class ScrapyTask(luigi.Task):
    """
    Generates a local file containing 5 elements of data in JSON format.
    """

    #: the date parameter.

    #date = luigi.DateParameter(default=datetime.date.today())
    #field = str(random.randint(0,10000)) + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    id = luigi.Parameter()
    
    query = luigi.Parameter()

    num = luigi.Parameter()

    user_twitter=luigi.Parameter() 
    datetime=luigi.Parameter()

    def run(self):
        """
        Writes data in JSON format into the task's output target.
        The data objects have the following attributes:
        * `_id` is  the default Elasticsearch id field,
        * `text`: the text,
        * `date`: the day when the data was created.
        """
        #today = datetime.date.today()
        #filePath = 'tweets/_scrapy-%s.json' % self.id
        #scraperImported = imp.load_source(self.website, 'scrapers/%s.py' % (self.website))
        #scraperImported.startScraping(self.url, filePath)
        #local_time_obj = localtime()
        #datetime = strftime("%Y_%m_%d_%H_%M_%S", local_time_obj)

        if self.user_twitter:
            filePath='timeline/{}.json'.format(self.user_twitter)
            retrieve_timeline(self.user_twitter,filePath,self.num)
        else:
            filePath='tweets/{}.json'.format(self.datetime)
            retrieve_tweets(self.query, filePath, self.num)
        self.set_status_message("Scraped!")


    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        if self.user_twitter :
            return luigi.LocalTarget(path='timeline/{}.json'.format(self.user_twitter))
        else:
            return luigi.LocalTarget(path='tweets/{}.json'.format(self.datetime))

class AnalizeTask(luigi.Task): 
    #: date task parameter (default = today)
    id = luigi.Parameter()

    query = luigi.Parameter()

    num = luigi.Parameter()

    user_twitter=luigi.Parameter() 

    datetime= luigi.Parameter()
    def requires(self):
        """
        This task's dependencies:
        * :py:class:`~.SenpyTask`
        :return: object (:py:class:`luigi.task.Task`)
        """
        return ScrapyTask(self.id, self.query, self.num,self.user_twitter,self.datetime) 
    
    def output(self):
        if self.user_twitter:
            return luigi.LocalTarget(path='TimelinesClasificados/{}.json'.format(self.user_twitter))
        else:
            return luigi.LocalTarget(path='tweetsClasificados/{}.json'.format(self.datetime))
    def run(self):
        with self.input().open() as fin, self.output().open('w') as fout:
            for line in fin:
                tweet=json.loads(line) 
                analisisInsomnia=analizeInsomniaSenpy(tweet['text'])
                if analisisInsomnia:
                    analisisSentiment=analizeSentimentSenpy(tweet['text'])
                    tweet=analizeLocation(tweet)
                    if tweet["Longitud"]!="ERROR AL GEOLOCALIZAR":
                        # Create JSON object for the tweet: created_at,id,user,user_id,text,is_insomniac
                        #tweet_user=json.loads(tweet["user"])
                        tweetDic={'_id':tweet["id"],'created_at':tweet["created_at"],'id_str':tweet["id_str"],'user':tweet["user"]["id"],'text':tweet["text"],'long':tweet["Longitud"],'lat':tweet["Latitud"],'sentiment':analisisSentiment,'is_insomniac':analisisInsomnia[0],'theme': analisisInsomnia[1]}
                        tweetJson=json.dumps(tweetDic)
                        fout.write(tweetJson +'\n')

    

class Elasticsearch(CopyToIndex):
    """
    This task loads JSON data contained in a :py:class:`luigi.target.Target` into an ElasticSearch index.
    This task's input will the target returned by :py:meth:`~.Senpy.output`.
    This class uses :py:meth:`luigi.contrib.esindex.CopyToIndex.run`.
    After running this task you can run:
    .. code-block:: console
        $ curl "localhost:9200/example_index/_search?pretty"
    to see the indexed documents.
    To see the update log, run
    .. code-block:: console
        $ curl "localhost:9200/update_log/_search?q=target_index:example_index&pretty"
    To cleanup both indexes run:
    .. code-block:: console
        $ curl -XDELETE "localhost:9200/example_index"
        $ curl -XDELETE "localhost:9200/update_log/_query?q=target_index:example_index"
    """
    #: date task parameter (default = today)
    id = luigi.Parameter(default=time.time())

    query = luigi.Parameter()

    num = luigi.Parameter()

    # user twitter id
    user_twitter=luigi.Parameter(default=None) 
    # date Parameter
    #local_time_obj = localtime()
    local_time_obj=date.today()
    #local_time_obj=local_time_obj-timedelta(1)
    datetime=local_time_obj.strftime("%Y_%m_%d_%H_%M_%S")

    #: the name luiof the index in ElasticSearch to be updated.
    index = luigi.Parameter()
    #: the name of the document type.
    doc_type = luigi.Parameter()
    #: the host running the ElasticSearch service.
    host = 'localhost'
    #: the port used by the ElasticSearch service.
    port = 9200
    #: settings used in ElasticSearch index creation.
    settings = {"index.mapping.total_fields.limit":6000, "number_of_shards": 1, "number_of_replicas": 1}
    #: timeout for ES post
    timeout = 100

    def requires(self):
        """
        This task's dependencies:
        * :py:class:`~.SenpyTask`
        :return: object (:py:class:`luigi.task.Task`)
        """
        #return ScrapyTask(self.id, self.query, self.num)
        return AnalizeTask(self.id,self.query,self.num,self.user_twitter,self.datetime)
if __name__ == "__main__":
    #luigi.run(['--task', 'Elasticsearch'])
    luigi.run()

#!/usr/bin/python

import requests
import json
import re


# Data is structured as -
#
# http://localhost:9200/deeplinks/app  # parent
# http://localhost:9200/deeplinks/data  # child


A_APPNAME="HuffingtonPost"
A_DLFIELD="dlvalue"
A_DLSCHEME=["huffpost://section/.../"]
A_DFIMGURL=''
A_GEOLOC=0
A_TS=1
A_TAGS=[]
A_DESC=""
A_CAT=[]
ESURL='http://localhost:9200/deeplinks/'
APP_DATA_FEED='http://mapi.huffingtonpost.com/mobile/v1/editions?device=v6,android,small,midres'

def get_app_id():
   q_get_app = {
      "query": {
         "filtered": {
            "query" : {
               "match_all": {}
            },
            "filter": {
               "term" : {"name" : A_APPNAME.lower()}
            }
         }
      }
   }
   resp = requests.get(ESURL+"app/_search?", data=json.dumps(q_get_app))
   if resp:
      rjson = json.loads(resp.text)
      if rjson['hits']['total'] != 0:
         return rjson['hits']['hits'][0]['_id']
   q_create_new_app = {
      "name": A_APPNAME,
      "dlfield": A_DLFIELD,
      "dlscheme": A_DLSCHEME,
      "dfimgurl": A_DFIMGURL,
      "geoloc": A_GEOLOC,
      "ts": A_TS,
      "tags": A_TAGS,
      "description": A_DESC,
      "catagory": A_CAT
   }
   resp = requests.post(ESURL+"app/", data=json.dumps(q_create_new_app))
   rjson = json.loads(resp.text)
   return rjson['_id']

def import_dl_from_feed(url):
   try:
      respJ = json.loads(requests.get(url).text)
   except:
      return

   for result in respJ['results']:
      try:
         uid = result["id"]
      except:
         continue
      try:
         cDate = result["entry_modified_on"]
      except:
         try:
            cDate = result["entry_created_on"]
         except:
            cDate = "1970-01-01 00:00:00"
      #check for duplicate
      fdatadup = {
         "query": {
            "filtered": {
               "filter": {
                  "and" : [
                     {
                        "term" : {"uid" : uid},
                     },
                     {
                        "has_parent" : {
                            "type" : "app",
                            "filter" : {
                                "ids" : {
                                    "type" : "app",
                                    "values" : [appid]
                                }
                            }
                        }
                     }
                  ]
               }
            }
         }
      }
      rdup = requests.get(ESURL+"data/_search?", data=json.dumps(fdatadup))
      rdupjson = json.loads(rdup.text)
      _dupid = ""
      if rdupjson['hits']['total'] >= 1:
         if rdupjson['hits']['hits'][0]['_source']['cDate'] == cDate:
#              print "### duplicate ###"
#              print uid
#              print rdupjson['hits']['hits'][0]['_source']
            continue
         else:
#              print "### overwrite ###"
#              print uid
#              print rdupjson['hits']['hits'][0]['_source']
            _dupid = rdupjson['hits']['hits'][0]['_id']+"/"

      if uid in uids_this_session:
         continue
      uids_this_session.append(uid)
      # check for duplicate end

      if "huffpost_url" not in result:
         continue
#     print json.dumps(result)+"\n\n"
      try:
         dlvalue = re.sub(r'^https?://', "", result["huffpost_url"])
      except:
         continue
      try:
         title = result["full_title"]
      except:
         title = "No title"
      try:
         imgURL = result["images"]["image_small"]
      except:
         imgURL = ""
      try:
         catid = [result["edition_id"]] # array of category id used internally by app
      except:
         catid = [] # array of category id used internally by app
      try:
         tags = result["internal_tags"] # array of tags used by app or added in by atl
      except:
         tags = [] # array of tags used by app or added in by atl
      #geoloc = str(result["lat"])+","+str(result["lng"])  # geo location if they have it
      fdata = {'uid': uid, 'dlvalue': dlvalue, 'cDate': cDate, 'text': title, 'imgurl': imgURL, 'tags': tags, 'cat_ids': catid}
      respF = requests.post(ESURL+"data/"+_dupid+"?parent="+appid, data=json.dumps(fdata))
#     print "### new result"
#      print uid
#     print rdupjson
#     print json.dumps(result)+"\n\n"
#      print str(fdata)+"\n"
   return


uids_this_session=[]
appid=get_app_id()

try:
   feed_urls_1 = json.loads(requests.get(APP_DATA_FEED).text)['results']
except:
   feed_urls_1 = None

for feed_url_1 in feed_urls_1:
   try:
      feed_urls_2 = json.loads(requests.get(feed_url_1["sections_url"]).text)['results']
   except:
      feed_urls_2 = None
   for feed_url_2 in feed_urls_2:
      import_dl_from_feed(feed_url_2["entries_url"])

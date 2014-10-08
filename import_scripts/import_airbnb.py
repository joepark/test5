#!/usr/bin/python

import requests
import json
import re
#import fcntl
import sys

A_APPNAME="Airbnb"
A_DLFIELD="uid"
A_DLSCHEME=["airbnb://rooms/", "airbnb://s/", "airbnb://users/"]
A_DFIMGURL=''
A_GEOLOC=1
A_TS=0
A_TAGS=[]
A_DESC=""
A_CAT=[]
ESURL='http://localhost:9200/deeplinks/'
APP_DATA_FEED='https://api.airbnb.com/v1/listings/search?ib_add_photo_flow=true&locale=en&guests=1&client_id=3092nxybyb0otqw18e8nh5nty&min_num_pic_urls=1&currency=USD'

#def lockFile(lockfile):
#   fp = open(lockfile, 'w')
#   try:
#      fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
#   except IOError:
#      return False
#   return True
#
#this_file_name = re.sub(r'.*/', "", __file__)
#if not lockFile("/tmp/"+this_file_name+".lock"):
#   sys.exit(0)

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

   try:
      listings = respJ['listings']
   except:
      #print "### ERR :"+str(respJ)
      #print "### ERR :"+url
      listings = []

   for item_p in listings:
      item=item_p['listing']
      try:
         uid = item["id"]
      except:
         continue

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
         #print "--"+str(uid)
         continue

      if uid in uids_this_session:
         continue
      uids_this_session.append(uid)
      # check for duplicate end


      try:
         title = item["name"]
      except:
         title = "No title"
      try:
         imgURL = item["thumbnail_url"]
      except:
         imgURL = ""
      catid = [] # array of category id used internally by app
      try:
         tag1 = item["market"]
      except:
         tag1 = []
      try:
         tag2 = item["neighborhood"]
      except:
         tag2 = []
      try:
         tag3 = item["property_type"]
      except:
         tag3 = []
      try:
         tag4 = item["room_type"]
      except:
         tag4 = []
      try:
         tag5 = item["room_type_category"]
      except:
         tag5 = []
      tags = [tag1, tag2, tag3, tag4, tag5] # array of tags used by app or added in by atl
      try:
         geoloc = str(item["lat"])+","+str(item["lng"])  # geo location if they have it
      except:
         continue;
      fdata = {'uid': uid, 'text': title, 'imgurl': imgURL, 'tags': tags, 'cat_ids': catid, 'geoloc': geoloc}
      respF = requests.post(ESURL+"data/"+_dupid+"?parent="+appid, data=json.dumps(fdata))
      #print "++"+str(uid)
#         print "### new item"
#      print uid
#         print rdupjson
#         print json.dumps(item)+"\n\n"
#      print str(fdata)+"\n"
   return


uids_this_session=[]
appid=get_app_id()

#cities=[ "Los Angeles", "Aspen", "Bahamas"]
cities=[ "Amsterdam", "Anchorage", "Aspen", "Bahamas", "Bangkok", "Barcelona", "Beijing", "Berlin", "Bogota", "Bora Bora", "Buenos Aires", "Budapest", "Cairo", "Cape Cod", "Cape Town", "Charleston", "Chicago", "Copenhagen", "Crete", "Delhi", "Dhaka", "Dublin", "Edinburgh", "Florence", "Guangzhou", "Hong Kong", "Honolulu", "Istanbul", "Jackson Hole", "Jakarta", "Jerusalem", "Karachi", "Kolkata", "Lagos", "Las Vegas", "Lisbon", "London", "Los Angeles", "Madrid", "Maldives", "Manila", "Maui", "Mexico City", "Montreal", "Moscow", "Mumbai", "New Orleans", "New York City", "Oahu", "Orlando", "Osaka", "Paris", "Prague", "Puerto Rico", "Rio de Janeiro", "Rome", "Sao Paulo", "San Diego", "San Francisco", "Seattle", "Seoul", "Shanghai", "St. Petersburg", "Sydney", "Tianjin", "Tokyo", "Tuscany", "U.S. Virgin Islands", "Vancouver", "Venice", "Vienna", "Washington D.C.", "Yellowstone", "Yosemite", "Zurich" ]

max_per_city=1000
#max_per_city=500
result_chunk=10


for city in cities:
   curr_offset=0
   while curr_offset < max_per_city:
      import_dl_from_feed(APP_DATA_FEED+'&items_per_page='+str(result_chunk)+'&location='+str(city)+'&offset='+str(curr_offset))
      curr_offset+=result_chunk

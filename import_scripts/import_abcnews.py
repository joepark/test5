#!/usr/bin/python

import requests
import json
import re
#import fcntl
#import sys


A_APPNAME="ABCNews"
A_DLFIELD="dlvalue"
A_DLSCHEME=["abcnews://"]
A_DFIMGURL=''
A_GEOLOC=0
A_TS=1
A_TAGS=[]
A_DESC=""
A_CAT=[]
ESURL='http://localhost:9200/deeplinks/'
APP_DATA_FEED='http://a.abcnews.go.com/xmldata/config/androidV2'

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
   for channel in respJ['channels']:
      for item in channel['items']:
         try:
            uid = item['guid']['pcdata']
         except:
            continue
         try:
            cDate = item["lastModDate"]
         except:
            try:
               cDate = item["pubDate"]
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
#               print rdupjson['hits']['hits'][0]['_source']
               continue
            else:
#               print "### overwrite ###"
#               print uid
#               print rdupjson['hits']['hits'][0]['_source']
               _dupid = rdupjson['hits']['hits'][0]['_id']+"/"

         if uid in uids_this_session:
            continue
         uids_this_session.append(uid)
         # check for duplicate end

         if "link" not in item:
            continue
         try:
            dlvalue = re.sub(r'^https?://', "", item["link"])
         except:
            continue
         try:
            title = item["title"]
         except:
            title = "No title"
         try:
            subtitle = item["abcn:subtitle"]
         except:
            subtitle = ""
         try:
            imgURL = item["abcn:images"][0]["abcn:image"]['url']
         except:
            imgURL = ""
         catid = [] # array of category id used internally by app
         try:
            tags = item["abcn:section"] # array of tags used by app or added in by atl
         except:
            tags = ""
         #geoloc = str(item["lat"])+","+str(item["lng"])  # geo location if they have it
         fdata = {'uid': uid, 'dlvalue': dlvalue, 'cDate': cDate, 'text': title, 'textl': subtitle, 'imgurl': imgURL, 'tags': tags, 'cat_ids': catid}
         respF = requests.post(ESURL+"data/"+_dupid+"?parent="+appid, data=json.dumps(fdata))
#         print "### new item"
#         print uid
#         print rdupjson
#         print json.dumps(item)+"\n\n"
#         print str(fdata)+"\n"
   return


uids_this_session=[]
appid=get_app_id()

try:
   feed_urls = json.loads(requests.get(APP_DATA_FEED).text)['config']
except:
   feed_urls = None

if feed_urls is not None:
   try:
      alerts_feed_url = feed_urls['alerts']['url']
   except:
      alerts_feed_url = None
   try:
      bestReads_feed_url = feed_urls['bestReads']['url']
   except:
      bestReads_feed_url = None
   try:
      rewindUnwind_feed_url = feed_urls['rewindUnwind']['url']
   except:
      rewindUnwind_feed_url = None
   try:
      topStories_feed_url = feed_urls['topStories']['url']
   except:
      topStories_feed_url = None
   try:
      sections_feed_url_array = feed_urls['sections']
   except:
      sections_feed_url_array = None
   try:
      shows_feed_url_array = feed_urls['shows']
   except:
      shows_feed_url_array = None

   import_dl_from_feed(alerts_feed_url)
   import_dl_from_feed(bestReads_feed_url)
   import_dl_from_feed(rewindUnwind_feed_url)
   import_dl_from_feed(topStories_feed_url)

   for section in sections_feed_url_array:
      if 'url' in section:
         feed_url=section['url']
      elif 'sectionsURL' in section:
         feed_url=section['sectionsURL']
      else:
         continue
      import_dl_from_feed(feed_url)

   for show in shows_feed_url_array:
      if 'url' in show:
         feed_url=show['url']
      else:
         continue
      import_dl_from_feed(feed_url)

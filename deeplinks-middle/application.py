from flask import request
from flask import Flask
from flask import g
import requests
import json
 
application = Flask(__name__)
 
################
# Configuration
################

# Path to AWS Elastic Load Balancer of backend Elasticsearch search index
ES_URL='http://es-lb-1393360866.us-east-1.elb.amazonaws.com/'
ES_INDEX='dl'
ES_TYPE_DEEPLINKS='data'
ES_TYPE_APP='app'

# Set cache max age of response in sec.
CACHE_MAX_AGE=300

# Set Access-Control-Allow-Origin.
ALLOW_ORIGIN="*"

RELATED_MAX=4

#########
# Routes
#########

#
# Main API call
#
#
#
# Parameters:
#    appid : Current app id
#    uid : Unique id of current article
#    sponsors : Optional.  Array of app ids.  In addtion to current app, related deep-link search will be done against given apps
#
#    Example:
#
#    http://dds.quixey.com/1.0/rdl?appid=mY82YaKqQs62t1k_WGiHbg&uid=25230384&sponsors=[%22MmPfoJMgS-2j4R3QtXkblQ%22]
#
#
#
# Responds:
#    Array of app and its deeplinks.  First app is always current app.  Subsequent apps are optional sponsors app and its deeplinks.
# 
#    Example: 
# 
#    [
#        {
#            "app": {
#                "_id": "mY82YaKqQs62t1k_WGiHbg", "catagory": [], "description": "", "dfimgurl": "", "dlfield": "dlvalue", "dlscheme": [ "abcnews://" ], "geoloc": 0, "name": "ABCNews", "tags": [], "ts": 1 
#            },
#            "deeplinks": {
#                "hits": 167,
#                "results": [
#                    { "_id": "ic6RxVrORaubSTjgm1hJLA", "cDate": "9/4/2014 12:51:01 GMT", "cat_ids": [], "dlvalue": "abcnews.go.com/Travel/12-rooftop-bars-perfect-indian-summer-nights/story?id=25230384", "imgurl": "http://a.abcnews.com/images/Travel/HT_palazzo_manfredi_jef_140903_16x9_608.jpg", "tags": "travel", "text": "12 Rooftop Bars Perfect for Indian Summer Nights", "textl": "While the cooler months \u2014 spent with friends crowding in cozy bars and huddling by the fire \u2014 are fast approaching, there\u2019s still time for some rooftop boozin\u2019 \u2014 and fortunately, enjoying the sun and stars with a drink in hand is made easy at some of our favorite hotels.", "uid": 25230384 },
#                    { "_id": "7hs9744FS7-XgNpJ4k3nGA", "cDate": "9/22/2014 20:55:08 GMT", "cat_ids": [], "dlvalue": "abcnews.go.com/GMA/video/mighty-ducks-stars-reunite-form-perfect-flying-25682390", "imgurl": "http://a.abcnews.com/images/GMA/140922_dvo_pop_ducks_16x9_608.jpg", "tags": "", "text": "'Mighty Ducks' Stars Reunite, Form Perfect Flying 'V'", "textl": "Everyone's favorite ice hockey team is back together celebrating 20 years since the release of \"D2: The Mighty Ducks.\"", "uid": 25682390 },
#                    { "_id": "heCr38yhRxyhb8djoTa5ZA", "cDate": "9/9/2014 14:11:20 GMT", "cat_ids": [], "dlvalue": "abcnews.go.com/International/harvest-moon-lights-night-sky/story?id=25371205", "imgurl": "http://a.abcnews.com/images/US/AP_MOON4_140909_DG_16x9_608.jpg", "tags": "international", "text": "Harvest Moon Lights Up the Night Sky", "textl": "A \"super moon\" lit up the sky Monday, the third such occurrence this summer.", "uid": 25371205 },
#                    { "_id": "NsD8gbC6SwiXq3atCFdZUQ", "cDate": "9/10/2014 17:30:19 GMT", "cat_ids": [], "dlvalue": "abcnews.go.com/Entertainment/friday-night-lights-star-adrianne-palicki-engaged/story?id=25407905", "imgurl": "http://a.abcnews.com/images/Entertainment/GTY_adrianne_palicki_jef_140910_16x9_608.jpg", "tags": "entertainment", "text": "'Friday Night Lights' Star Adrianne Palicki Is Engaged", "textl": "Adrianne Palicki is engaged.", "uid": 25407905 }
#                ]
#            }
#        },
#        {
#            "app": { 
#                "_id": "MmPfoJMgS-2j4R3QtXkblQ", "catagory": [], "description": "", "dfimgurl": "", "dlfield": "uid", "dlscheme": [ "airbnb://rooms/", "airbnb://s/", "airbnb://users/" ], "geoloc": 1, "name": "Airbnb", "tags": [], "ts": 0
#            },
#            "deeplinks": {
#                "hits": 885,
#                "results": [
#                    { "_id": "jndd3UoyQcekVNWVSWBiZw", "cat_ids": [], "geoloc": "31.7565257532,35.2025339268", "imgurl": "https://a1.muscache.com/pictures/37792152/small.jpg", "tags": [ "Jerusalem", null, "Apartment", "Entire home/apt", "entire_home" ], "text": "Perfect family summer rental!", "uid": 2595309 },
#                    { "_id": "ZGAMSvhERnS25TpqWPOD1Q", "cat_ids": [], "geoloc": "41.9468663658,-87.6488575204", "imgurl": "https://a2.muscache.com/pictures/25294453/small.jpg", "tags": [ "Chicago", "Lakeview", "Apartment", "Entire home/apt", "entire_home" ], "text": "Amazing rooftop decks for summer ", "uid": 1741176 },
#                    { "_id": "e2uOkb1ATliRmdLEXYoPyg", "cat_ids": [], "geoloc": "55.9668928914,-3.18744431983", "imgurl": "https://a0.muscache.com/pictures/33595529/small.jpg", "tags": [ "Edinburgh", null, "House", "Private room", "private_room" ], "text": "Twin Room with Free Wi-Fi & T V", "uid": 1741346 },
#                    { "_id": "RM983DMOTMuHBkmO-wEDtw", "cat_ids": [], "geoloc": "38.713581762,-9.13419952745", "imgurl": "https://a1.muscache.com/pictures/2409130/small.jpg", "tags": [ "Lisbon", "S\u00e3o Crist\u00f3v\u00e3o e S\u00e3o Louren\u00e7o", "Apartment", "Entire home/apt", "entire_home" ], "text": "Lisbon\u2019s Perfect Location Studio", "uid": 250288 }
#                ]
#            }
#        }
#    ]
#
#
@application.route('/1.0/rdl')
def main():
   # Set Cache control
   @after_this_request
   def set_cache_control(response):
      response.cache_control.max_age = CACHE_MAX_AGE
      response.cache_control.no_transform = True
      response.cache_control.public = "public"
      response.headers['Access-Control-Allow-Origin'] = ALLOW_ORIGIN
      return response

   # Request params check
   if 'uid' not in request.args or 'appid' not in request.args:
      return '[]'
   uid = request.args.get('uid')
   appid = request.args.get('appid')
   sponsors = ''
   if 'sponsors' in request.args:
      try:
         sponsors = json.loads(request.args.get('sponsors'))
      except:
         sponsors = ''
   partner_id = request.args.get('partner_id')
   partner_secret = request.args.get('partner_secret')

   # Current app's settings (i.e. URI scheme) are queried and returned.
   current_app = getApp(appid)
   rjson = getItem(uid,appid)
   out = [
            {
               "app": current_app
            }
         ]
   if rjson['hits'] > 0:
      # For now, get related item based on title text and tags.  Should be optimized to count for categories and 
      # other future fields.
      current_item = rjson['results'][0]
      current_keywords = current_item['text']+", "+", ".join(current_item['tags'])
      related_items = getRelated([appid], current_keywords)
      out[0]["deeplinks"]=related_items

      # Sponsors was not part of Phase 1 but was needed for demo purpose.  Need more clean up
      if isinstance(sponsors, list):
         related_sponsors = getRelated(sponsors, current_keywords)
         app_to_deeplinks={}
         for related_sponsor in related_sponsors['results']:
            parentApp = getParentApp(related_sponsor['_id'])
            parentAppId = parentApp['results'][0]['_id']
            if parentAppId not in app_to_deeplinks:
               app_to_deeplinks[parentAppId]={}
               app_to_deeplinks[parentAppId]["app"]=parentApp['results'][0]
               app_to_deeplinks[parentAppId]["deeplinks"]={
                  "hits": related_sponsors['hits'], 
                  "results": []
               }
            app_to_deeplinks[parentAppId]["deeplinks"]["results"].append(related_sponsor)
         for sponsored_deeplink in app_to_deeplinks:
            out.append(app_to_deeplinks[sponsored_deeplink])
   else:
      # Need to queue current item to import list.
      # Requires App partnership to gain access to their API in order to call item by id.
      out[0]["deeplinks"]=getDefault(appid)

   return json.dumps(out)



#
# 
#
# Parameters:
#    appid : Current app id
#    uid : Unique id of current article
#
#    Example:
#
#    http://dds.quixey.com/1.0/item?appid=mY82YaKqQs62t1k_WGiHbg&uid=25230384
#
#
#
# Responds:
#    Returns a single article item and its app environment
# 
#    Example: 
# 
#    [
#        {
#            "app": {
#                "_id": "mY82YaKqQs62t1k_WGiHbg",
#                "catagory": [],
#                "description": "",
#                "dfimgurl": "",
#                "dlfield": "dlvalue",
#                "dlscheme": [
#                    "abcnews://"
#                ],
#                "geoloc": 0,
#                "name": "ABCNews",
#                "tags": [],
#                "ts": 1
#            },
#            "deeplinks": {
#                "hits": 1,
#                "results": [
#                    {
#                        "_id": "ic6RxVrORaubSTjgm1hJLA",
#                        "cDate": "9/4/2014 12:51:01 GMT",
#                        "cat_ids": [],
#                        "dlvalue": "abcnews.go.com/Travel/12-rooftop-bars-perfect-indian-summer-nights/story?id=25230384",
#                        "imgurl": "http://a.abcnews.com/images/Travel/HT_palazzo_manfredi_jef_140903_16x9_608.jpg",
#                        "tags": "travel",
#                        "text": "12 Rooftop Bars Perfect for Indian Summer Nights",
#                        "textl": "While the cooler months \u2014 spent with friends crowding in cozy bars and huddling by the fire \u2014 are fast approaching, there\u2019s still time for some rooftop boozin\u2019 \u2014 and fortunately, enjoying the sun and stars with a drink in hand is made easy at some of our favorite hotels.",
#                        "uid": 25230384
#                    }
#                ]
#            }
#        }
#    ]
#
#
@application.route('/1.0/item')
def item():
   # Set Cache control
   @after_this_request
   def set_cache_control(response):
      response.cache_control.max_age = CACHE_MAX_AGE
      response.cache_control.no_transform = True
      response.cache_control.public = "public"
      response.headers['Access-Control-Allow-Origin'] = ALLOW_ORIGIN
      return response
   if 'uid' not in request.args or 'appid' not in request.args:
      return '[]'
   uid = request.args.get('uid')
   appid = request.args.get('appid')
   partner_id = request.args.get('partner_id')
   partner_secret = request.args.get('partner_secret')
   current_app = getApp(appid)
   rjson = getItem(uid,appid)
   if rjson['hits'] == 0:
      return '[]'
   out = [
            {
               "app": current_app,
               "deeplinks": {
                  "hits": rjson['hits'],
                  "results": rjson['results']
               }
            }
         ]
   return json.dumps(out)


def after_this_request(func):
   if not hasattr(g, 'call_after_request'):
      g.call_after_request = []
   g.call_after_request.append(func)
   return func




@application.after_request
def per_request_callbacks(response):
   for func in getattr(g, 'call_after_request', ()):
      response = func(response)
   return response



def searchES(es_type, fdata):
   resp = requests.post(ES_URL+ES_INDEX+'/'+es_type+'/_search', data=json.dumps(fdata))
   rjson = json.loads(resp.text)
   rel_articles = []
   for ele in rjson['hits']['hits']:
      ele['_source']['_id']=ele['_id']
      rel_articles.append(ele['_source'])
   out = { 
            "hits": rjson['hits']['total'], 
            "results": rel_articles
         }
   return out





def getApp(appid):
   resp = requests.get(ES_URL+ES_INDEX+'/'+ES_TYPE_APP+'/'+appid)
   rjson = json.loads(resp.text)
   if not rjson['found']:
      return {}
   rjson['_source']['_id']=rjson['_id']
   return rjson['_source']



def getParentApp(_id):
   fdata = {
      "size": 1,
      "query": {
         "filtered": {
            "filter": {
               "has_child" : {
                   "type" : "data",
                   "filter" : {
                       "ids" : {
                           "type" : "data",
                           "values" : [_id]
                       }
                   }
               }
            }
         }
      }
   }
   return searchES(ES_TYPE_APP, fdata)



def getItem(uid, appid):
   fdata = {
      "size": 1,
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
   return searchES(ES_TYPE_DEEPLINKS, fdata)




def getRelated(appid, keywords):
   fdata = {
      "size": RELATED_MAX,
      "query": {
         "filtered": {
            "query" : {
               "flt" : {
                  "fields" : ["tags", "text"],
                  "like_text" : keywords
               }
            },
            "filter": {
               "has_parent" : {
                  "type" : "app",
                  "filter" : {
                     "ids" : {
                        "type" : "app",
                        "values" : appid
                     }
                  }
               }
            }
         }
      }
   }
   return searchES(ES_TYPE_DEEPLINKS, fdata)


def getDefault(appid):
   fdata = {
      "size": RELATED_MAX,
      "query": {
         "filtered": {
            "filter": {
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
         }
      }
   }
   return searchES(ES_TYPE_DEEPLINKS, fdata)

 
if __name__ == '__main__':
   application.run(host='0.0.0.0', debug=True)

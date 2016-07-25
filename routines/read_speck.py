import pandas as pd
import urllib2, json
import os

def get_feeds_info():
    url = "https://esdr.cmucreatelab.org/api/v1/feeds?where=userId=358&fields=id,name,exposure,minTimeSecs,maxTimeSecs,latitude,longitude"
    response = urllib2.urlopen(url)
    data = json.loads(response.read())
    print type(data)

def main():
    get_feeds_info()
    # df = pd.read_json(os.getcwd() + '/input/feeds.json')
    # df.info()
    # print df.head()
    # df.to_csv(os.getcwd() + '/input/feed_name.csv')

    return

main()

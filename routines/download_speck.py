import pandas as pd
import urllib2, json
import requests
import os

# helper start #
def parent_dir(dirname):
    return dirname[:dirname.find('/routines')]
# helper end #

def get_feeds_pub_private():
    return

# possibly deprecated
def get_feeds_info(token):
    url = "https://esdr.cmucreatelab.org/api/v1/feeds?where=userId=358&fields=id,name,exposure,minTimeSecs,maxTimeSecs,latitude,longitude"
    request = urllib2.Request(url)
    request.add_header("Authorization", "Bearer {0}".format(token))
    response = urllib2.urlopen(request)
    print response
    data = json.loads(response.read())
    # print (data['data']['rows'])
    with open (os.getcwd() + '/input/feeds.json', 'w+') as outfile:
        json.dump(data['data']['rows'], outfile)

def read_feeds_info():
    df = pd.read_json(os.getcwd() + '/input/feeds.json')
    df['maxTime'] = pd.to_datetime(df['maxTimeSecs'], unit = 's')
    df['minTime'] = pd.to_datetime(df['minTimeSecs'], unit = 's')
    df['duration'] = df['maxTime'] - df['minTime']
    df.to_csv(os.getcwd() + '/input/feed_info.csv', index=False)

def download_feeds(token):
    df = pd.read_csv(os.getcwd() + '/input/feed_info.csv')
    feeds = df['id']
    df.set_index('id', inplace=True, drop=False)

    for feed in feeds:
        print feed
        request = urllib2.Request('https://esdr.cmucreatelab.org/api/v1/feeds/{0}/channels/humidity,particle_concentration,particle_count,raw_particles,temperature/export'.format(feed))
        request.add_header("Authorization", "Bearer {0}".format(token))
        response = urllib2.urlopen(request)
        df_feed = pd.read_csv(response)
        if len(df_feed) == 0:
            print 'Empty feed: {0}'.format(feed)
            continue
        name = df.ix[feed, 'name']
        start_date = df.ix[feed, 'minTime'][:10]
        end_date = df.ix[feed, 'maxTime'][:10]
        outfilename = '{0}_{1}_to_{2}.csv'.format(name, start_date,
                                                  end_date)
        df.ix[feed, 'filename'] = outfilename
        outfile = parent_dir(os.getcwd()) + '/DataBySensor/Speck/raw_data/round_all_bulkdownload/{0}'.format(outfilename)
        print outfilename
        df_feed.to_csv(outfile, index=False)
    df.to_csv(os.getcwd() + '/input/feed_info_withname.csv', index=False)

def main():
    # Note, need to refresh token every week
    token = "5710bcf11dc4cdc25e3c89f11208360d6427ef4d387fd38470eef17672077325"
    get_feeds_info(token)
    read_feeds_info()
    download_feeds(token)
    return

main()

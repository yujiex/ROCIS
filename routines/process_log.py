import pandas as pd
import os
import glob
import util
import re
import datetime

def read_formatted():
    # df_lookup = pd.read_csv(os.getcwd() + '/input/log_rename.csv')
    # df_lookup.set_index('oldname', inplace=True)
    home_id_dict = util.get_homeid_dict()
    files = glob.glob(util.get_path('daily_reading', 'raw_data', 'all') + 'xlsx_formatted/*.xlsx')
    lastline_dict = \
        {'LCMP Log_Observation-Incident Report_V6.JJN.xlsx': 32,
         'Copy of D4-CMU-Daily Readings_RTto24jan2016.xlsx': 16,
         'D4-Hartkopf_Loftness-Daily Readings.xlsx': 14}
    sheets_dict = {'D4-Hartkopf_Loftness-Daily Readings.xlsx': [0, 1],
        'Copy of D4-CMU-Daily Readings_RTto24jan2016.xlsx': [0]}
    for f in files:
        sheetlist = [1]
        filename = f[f.rfind('/') + 1:]
        tokens = util.split_string([' ', '_', '.', '-'], filename)
        if filename in sheets_dict:
            sheetlist = sheets_dict[filename]
        home_id = 'UNKNOWN'
        for x in tokens:
            if x in home_id_dict:
                home_id = home_id_dict[x]
        for s in sheetlist:
            idx_lastline = 33
            df = pd.read_excel(f, sheetname=s)
            if filename in lastline_dict:
                idx_lastline = lastline_dict[filename]
            df2 = df.transpose().iloc[:, [1, 2, idx_lastline]]
            df2.dropna(subset=[idx_lastline], inplace=True)
            df2.rename(columns={1: 'date', 2: 'time', 33: 'activity'}, inplace=True)
            df2.drop(df2.index[0], axis=0, inplace=True)
            timestr = df2.ix[-1, 'date'].strftime('%m-%d-%Y')
            outfile = 'activity_{0}_{1}.csv'.format(home_id, timestr)
            print 'write to {0}'.format(outfile)
            df2.to_csv(util.get_path('daily_reading', 'activity_stamp',
                                    'all') + '{0}'.format(outfile),
                    index=False)
    return

def remove_ampm(string):
    if type(string) == datetime.time:
        return string
    if 'am' in string:
        return string.replace('am', '') + ':00'
    elif 'pm' in string:
        string = string.replace('pm', '')
        head = int(string[:string.find(':')]) + 12
        tail = string[string.find(':') + 1:]
        return '{0}:{1}:00'.format(head, tail)
    else:
        return string

def read_unformatted():
    home_id_dict = util.get_homeid_dict()
    files = glob.glob(util.get_path('daily_reading', 'raw_data', 'all') + 'xlsx_unformatted/*.xlsx')
    for f in files[:1]:
        filename = f[f.rfind('/') + 1:]
        tokens = util.split_string([' ', '_', '.', '-'], filename)
        home_id = 'UNKNOWN'
        for x in tokens:
            if x in home_id_dict:
                home_id = home_id_dict[x]
        df = pd.read_excel(f, sheetname=0)
        if filename == 'TC_D2_Romer_Manual readings -logs_ - Sail boat_11-1_11-18.xlsx':
            df.rename(columns={'Date': 'date', 'Time': 'time',
                               'Action: observations and behaviour':
                               'activity'}, inplace=True)
            df.dropna(axis=0, how='all', inplace=True)
            pattern = re.compile('[0-9]{1,2}')
            df['date'] = df['date'].ffill()
            df['time'] = df['time'].ffill()
            df.dropna(subset=['date', 'time'], axis=0, how='any',
                      inplace=True)
            df['time'] = df['time'].map(remove_ampm)
            df['date'] = df['date'].map(lambda x: 'Nov' if x == 'Nov' else '2016-11-{0}'.format(re.match(pattern, x).group()))
            df = df[['date', 'time', 'activity']]
        if filename == 'DHP Log - DHP-HP-Daily Readings week of Jan 11, 2016.xlsx':
            df = df.ix[19:23, [0, 1, 8]]
            df.rename(columns={'Date': 'date', 'Unnamed: 1': 'time',
                               datetime.datetime(2016, 1, 14, 0, 0):
                               'activity'}, inplace=True)
            df['time'] = df['time'].map(remove_ampm)
            df.info()
            print df.head()
        lastdate = df['date'].tolist()[-1]
        print type(lastdate)
        if type(lastdate) == datetime.datetime:
            timestr = lastdate.strftime('%m-%d-%Y')
        elif type(lastdate) == pd.tslib.Timestamp:
            timestr = '{0}-{1}-{2}'.format(lastdate.month, lastdate.day, lastdate.year)
        else:
            timestr = lastdate
        outfile = 'activity_{0}_{1}.csv'.format(home_id, timestr)
        print 'write to {0}'.format(outfile)
        df.to_csv(util.get_path('daily_reading', 'activity_stamp',
                                'all') + '{0}'.format(outfile),
                  index=False)

def main():
    # read_formatted()
    read_unformatted()
    return
    
main()

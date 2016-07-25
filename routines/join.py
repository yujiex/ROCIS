import pandas as pd
import os
import glob
import numpy as np
from shutil import copyfile

def parent_dir(dirname):
    return dirname[:dirname.find('/routines')]

dylos_summary_path = parent_dir(os.getcwd()) + '/DataBySensor/Dylos/dropdup/round_all/summary/'
speck_summary_path = parent_dir(os.getcwd()) + '/DataBySensor/Speck/raw_data/round_all_bulkdownload/summary/'
speck_raw_path = parent_dir(os.getcwd()) + '/DataBySensor/Speck/raw_data/'
speck_raw_auto_path = speck_raw_path + 'round_all_bulkdownload/'
speck_raw_manual_path = speck_raw_path + 'manual_download/'
speck_concat_path = parent_dir(os.getcwd()) + '/DataBySensor/Speck/concat/round_all_bulkdownload/'
dylos_concat_path = parent_dir(os.getcwd()) + '/DataBySensor/Dylos/concat/round_all/'
dylos_speck_by_id_path = parent_dir(os.getcwd()) + '/DataBySensor/dylosSpeck/round_all/'

def join_speck_dylos(join_type, checkOrWrite):
    print 'joining speck and dylos data ...'
    dylos_f_list = glob.glob(dylos_concat_path + '*.csv')
    missing_files = []
    for f in dylos_f_list:
        filename = f[f.rfind('/') + 1:]
        speck_file_name = filename.replace('_R', '_I')
        speck_file = speck_concat_path + speck_file_name
        try:
            df_speck = pd.read_csv(speck_file)
        except IOError:
            print '{0} does not exist'.format(speck_file_name)
            missing_files.append(speck_file_name)
            continue
        if checkOrWrite == 'write':
            df_dylos = pd.read_csv(f, parse_dates = ['Date/Time'])
            df_speck['Date/Time'] = pd.to_datetime(df_speck['EpochTime'],
                                                unit = 's')
            df_speck['Date/Time'] = df_speck['Date/Time'].map(lambda x: x.replace(second=0))
            df_all = pd.merge(df_dylos, df_speck, on='Date/Time',
                            how=join_type)
            print 'write to {0} joint file: {1}'.format(join_type,
                                                        filename)
            df_all.to_csv('{0}{1}/{2}'.format(dylos_speck_by_id_path,
                                            join_type, filename),
                        index=False)
    return list(set(missing_files))

def join_speck_dylos_summary():
    loc_dict = {'-': '-', 'I': 'I', 'O': 'O', 'R': 'I'}
    df_speck_summary = \
        pd.read_csv('{0}{1}'.format(speck_concat_path, 'stat_summary/temperature_round_all_bulkdownload.csv'))
    df_speck_summary = df_speck_summary[['filename']]
    df_speck_summary['home_id_standard'] = df_speck_summary['filename'].map(lambda x: x[:x.find('_')])
    df_speck_summary['indoor outdoor'] = df_speck_summary['filename'].map(lambda x: x[x.find('_') + 1: -4])
    df_dylos_summary = pd.read_csv(dylos_summary_path + \
                                   'Small_round_all_range.csv')
    df_dylos_summary = df_dylos_summary[['filename', 'round', 'home_id_standard', 'general_location_standard']]
    df_dylos_summary['indoor outdoor'] = df_dylos_summary['general_location_standard'].map(lambda x: loc_dict[x])
    df_all = pd.merge(df_dylos_summary, df_speck_summary,
                      on=['home_id_standard', 'indoor outdoor'], 
                      how='left', suffixes=('_dylos', '_speck'))
    df_all.to_csv(parent_dir(os.getcwd()) + '/DataBySensor/dylosSpeck/round_all/summary/summary_before_join.csv', index=False)
    print 'end'
    # home_id_0 = df_dylos_summary[['home_id_standard', 'round']]
    # home_id_0.drop_duplicates(inplace=True)
    # home_id_0['home_id_dylos'] = home_id_0['home_id_standard']

    # home_id_1 = df_speck_summary[['home_id_standard', 'round']]
    # home_id_1 = home_id_1[home_id_1['round'] != str(6)]
    # home_id_1.drop_duplicates(inplace=True)
    # home_id_1['home_id_speck'] = home_id_1['home_id_standard']
    # df_id = pd.merge(home_id_0, home_id_1, on='home_id_standard', how='outer', suffixes=('_dylos', '_speck'))
    # df_id.to_csv(parent_dir(os.getcwd()) + '/DataBySensor/dylosSpeck/round_all/summary/label_diff.csv', index=False)
    return
    
# autoOrManual
def concat_speck(autoOrManual):
    if autoOrManual == 'auto':
        df = pd.read_csv(speck_summary_path + \
                        'temperature_round_all_bulkdownload.csv')
        filepath = speck_raw_auto_path
        outpath = speck_concat_path
    else:
        filepath = speck_raw_manual_path
        df = pd.read_csv(filepath + 'summary/summary.csv')
        outpath = speck_concat_path.replace('round_all_bulkdownload',
                                            'manual_download')
    gr = df.groupby(['home_id_standard', 'general_location_standard'])
    
    for name, group in gr:
        print name
        if '-' in name:
            continue
        filelist = group['filename'].tolist()
        filelist = ['{0}{1}'.format(filepath, f) for f in filelist]
        dfs = [pd.read_csv(f) for f in filelist]
        dfs = [df.rename(columns=lambda x: x if x.rfind('.') == -1 else x[x.rfind('.') + 1:]) for df in dfs]
        df_all = pd.concat(dfs, ignore_index=True)
        print ('{0}_{1}.csv'.format(name[0], name[1]))
        df_all.sort(columns='EpochTime', inplace=True)
        df_all.to_csv('{0}{1}_{2}.csv'.format(outpath, name[0],
                                              name[1]), index=False)
    
def check_dup_speck():
    filelist = glob.glob(speck_concat_path + '*.csv')
    for f in filelist:
        filename = f[f.rfind('/') + 1:]
        df = pd.read_csv(f)
        unique_values = len(df['EpochTime'].unique())
        if len(df) > unique_values:
            print '{0}: length {1}, unique values {2}'.format(filename, len(df), unique_values)
            
def get_file_from_manual():
    print 'search missing files in manual downloads ...'
    summary_path = '/media/yujiex/work/ROCIS/DataBySensor/Speck/raw_data/round_all/summary/'
    df_summary = pd.read_csv(summary_path + \
                             'temperature_round_all.csv')
    missing_list = join_speck_dylos('inner', 'check')
    def get_id_loc(string):
        sep_idx = string.find('_')
        return (string[:sep_idx], string[sep_idx + 1])
    # missing_pair = [get_id_loc(f) for f in missing_list]
    df_summary['name_file'] = df_summary.apply(lambda r: ('{0}_{1}.csv'.format(r['home_id_standard'], r['general_location_standard'])), axis=1)
    df_summary = df_summary[df_summary['name_file'].isin(missing_list)]
    df_summary.to_csv(speck_raw_manual_path + 'summary/summary.csv', 
                      index=False)
    missing_files = df_summary['filename'].tolist()
    for f in missing_files:
        infile = speck_raw_path + 'round_all/' + f
        outfile = speck_raw_manual_path + f
        copyfile(infile, outfile)
    return

def main():
    # join_speck_dylos_summary() # note this is before joining
    # get_file_from_manual()
    # concat_speck('manual')
    print join_speck_dylos('outer', 'write')
    print join_speck_dylos('inner', 'write')
    # check_dup_speck()           # no duplicate time 
    return
    
main()

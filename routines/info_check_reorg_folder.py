# BOOKMARK: updated the summary file and dropdup file in chop_wrong_start
# double check and improve summary routine
# regen summary report and uploading

import pandas as pd
import os
import glob
import numpy as np
import datetime
import re
import shutil
import time
from pandas.tseries.offsets import *
import random
import cleaning_dylos as cd
import util

step_size = 100
start_year = 2014
end_year = 2018

def timing(ori, current, funname):
    print '{0} takes {1}s...'.format(funname, current - ori)
    return current

def parent_dir(dirname):
    return dirname[:dirname.find('/routines')]

dylos_summary_path = parent_dir(os.getcwd()) + '/DataBySensor/Dylos/dropdup/round_all/summary/'
dylos_cleaned_path = parent_dir(os.getcwd()) + '/DataBySensor/Dylos/dropdup/round_all/'
dylos_concat_path = parent_dir(os.getcwd()) + '/DataBySensor/Dylos/concat/round_all/'
speck_summary_path = parent_dir(os.getcwd()) + '/DataBySensor/Speck/raw_data/round_all_bulkdownload/summary/'
speck_raw_data_path = parent_dir(os.getcwd()) + '/DataBySensor/Speck/raw_data/round_all_bulkdownload/'
speck_concat_path = parent_dir(os.getcwd()) + '/DataBySensor/Speck/concat/round_all_bulkdownload/'

# convert particles per cubic foot to hundred cubic foot
def get_multiplier_nskip(f):
    with open (f, 'r') as rd:
        lines = rd.readlines()
        nskip = 0
        m = 1
        for i in range(min(len(lines), 30)):
            if 'Date/Time' in lines[i]:
                nskip = i
            elif 'Particles per' in lines[i]:
                if lines[5] == 'Particles per cubic foot\n':
                    m = 0.01
                else:
                    m = 1
        return (m, nskip)

# Turn a describe df from float to int
def int_describe(df):
    for col in df:
        df[col] = df[col].map(lambda x: x if np.isnan(x) else (int(round(x, 0))))
    return df

# if exactly two comma are in the string, return True, else return False
def contain_two_comma(string):
    comma_1 = string.find(',')
    comma_2 = string.rfind(',')
    return ((comma_1 != -1 and comma_1 != comma_2) and \
            string[comma_1 + 1: comma_2].find(',') == -1)

# get the first line of putty log
def get_first_puttyline(puttylines):
    length = len(puttylines)
    for i in range(length):
        if contain_two_comma(puttylines[i]):
            return i

# subfolder is '/...'
def insert_sub_folder(path, subfolder):
    lastslash = path.rfind('/')
    return path[:lastslash] + subfolder + path[lastslash:]

# separate two types of dylos logger,
# if one file contains mixed logger type, separate them to single files
def separate(folder, suffix):
    print '-- separating files by logger type start --'
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    files = []
    logger_types = []
    is_processed = []
    putty_starts = []
    dylos_starts = []
    outfile_sep = (parent_dir(os.getcwd()) + folder.replace('raw_data', 'separate') + 'separate_summary/separate.csv')
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        files.append(filename)
        if ('.xlsx' in filename) or ('.xls' in filename):
            is_processed.append(False)
            logger_types.append('-')
            putty_starts.append('-')
            dylos_starts.append('-')
            continue
        # print filename
        is_processed.append(True)

        with open (f, 'r') as rd:
            lines = rd.readlines()
        puttylines = []
        dyloslines = []
        unknonlines = []
        putty_start = []
        dylos_start = []
        putty = False
        dylos = False

        outfile = f.replace('raw_data', 'separate')
        if not '.' in outfile:
            outfile += '.txt'
        idx_dot = outfile.rfind('.')
        outfile_dylos = outfile[:idx_dot] + '_dylos' + outfile[idx_dot:]
        outfile_putty = outfile[:idx_dot] + '_putty' + outfile[idx_dot:]
        outfile_unknown = outfile[:idx_dot] + '_unknown' + outfile[idx_dot:]
        length = len(lines)

        for i in range(length):
            lines[i] = lines[i].replace(', Small, Large', ',Small,Large')
            if 'PuTTY log' in lines[i]:
                putty = True
                putty_start.append(i)
            if 'Dylos Logger' in lines[i]:
                dylos = True
                dylos_start.append(i)
            if putty:
                puttylines.append(lines[i])
            elif dylos:
                dyloslines.append(lines[i])

        if putty and not dylos:
            logger_types.append('Putty')
        elif dylos and not putty:
            logger_types.append('Dylos')
        elif dylos and putty:
            logger_types.append('Putty & Dylos')
        else:
            logger_types.append('Unknown')

        if len(dyloslines) != 0:
            with open (outfile_dylos, 'w+') as wt:
                wt.write(''.join(dyloslines))
        if len(puttylines) != 0:
            with open (outfile_putty, 'w+') as wt:
                wt.write(''.join(puttylines))
        if not putty and not dylos:
            with open (outfile_unknown, 'w+') as wt:
                wt.write(''.join(lines))
        putty_starts.append(putty_start)
        dylos_starts.append(dylos_start)

    df = pd.DataFrame({'filename': files,
                       'logger_type': logger_types,
                       'putty_start_lines': putty_starts,
                       'dylos_start_lines': dylos_starts,
                       'processed': is_processed})
    df.to_csv(outfile_sep, index=False)
    print '-- separating files by logger type end --'

def no_char_in_str(string):
    pattern = re.compile(r'[!a-zA-Z]')
    match = re.search(pattern, string)
    return match == None

def format_match(string):
    pattern = re.compile('^([0-9]{2}/){2}[0-9]{2} [0-9]{2}:[0-9]{2}, *[0-9]{0,12}, *[0-9]{0,6}\r\n')
    match = re.match(pattern, string)
    return match != None

# print format_match('01/01/00 00:12,1821,60')
# print format_match('01/00 00:12,1821,60')
# print format_match('01/01/00 00:12,1821,60\r\n')
# print format_match('01/08/16 13:07, 649000, 15700\r\n')

def reformat(folder, suffix, loggertype):
    print '-- reformatting file header start --'
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    files = []
    is_processed = []
    ill_format_lines = []
    logger_types = []
    units = []
    multipliers = []
    outfile_reform = (parent_dir(os.getcwd()) + folder.replace('separate', 'reformat') + 'reform_summary/reform_{0}.csv'.format(loggertype))
    emptyfile_counter = 0
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        files.append(filename)
        # print filename
        is_processed.append(True)
        logger_types.append(filename[filename.rfind('_') + 1: \
                                     filename.rfind('.')])
        with open (f, 'r') as rd:
            lines = rd.readlines()
        datalines = []
        ill_format_linenum = []
        outfile = f.replace('separate', 'reformat')
        outfile_err = f.replace('separate', 'ErrMessage_reformat')
        unit = 'Particles per cubic foot / 100'
        multiplier = 1.0
        length = len(lines)
        for i in range(length):
            if 'Particles per cubic foot\r\n' in lines[i]:
                unit = 'Particles per cubic foot'
                multiplier = 0.01

            #if ((contain_two_comma(lines[i])) and (no_char_in_str(lines[i]))):
            if (format_match(lines[i])):
                datalines.append(lines[i])
            else:
                # headers are also logged to here
                ill_format_linenum.append(i)
        ill_format_lines.append(ill_format_linenum)
        units.append(unit)
        multipliers.append(multiplier)

        if len(datalines) == 0:
            emptyfile_counter += 1
            print 'empty file: ' + filename

        if len(datalines) != 0:
            with open (outfile, 'w+') as wt:
                datalines = ['Date/Time,Small,Large\r\n'] + datalines
                wt.write(''.join(datalines))
            if len(ill_format_linenum) != 0:
                with open (outfile_err, 'w+') as wt:
                    err_lines = [lines[i] for i in ill_format_linenum]
                    wt.write(''.join(err_lines))
    df = pd.DataFrame({'filename': files,
                       'unit': units,
                       'multiplier': multipliers,
                       'logger_type': logger_types,
                       'lines taken out': ill_format_lines})
    df.to_csv(outfile_reform, index=False)
    print emptyfile_counter
    print '-- reformatting file header end --'

# requires 'folder' is at the same level with 'H:/ROCIS/routines'
def print_timerange(folder, suffix):
    pd.options.display.show_dimensions = False
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        if ('.xlsx' in filename) or ('.xls' in filename):
            continue
        #print filename
        (m, nskip) = get_multiplier_nskip(f)
        df = pd.read_csv(f, skiprows=nskip)
        df.dropna(inplace=True)
        if (len(df)) == 0:
            print filename
            continue
        if ' Small' in df:
            df[' Small'] = df[' Small'] * m
            df[' Large'] = df[' Large'] * m
        else:
            df['Small'] = df['Small'] * m
            df['Large'] = df['Large'] * m
        #print 'Period: {0} to {1}'.format(df['Date/Time'].min(), df['Date/Time'].max())
        #print int_describe(df.describe())

def print_excel_sheetname(folder, suffix):
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        print filename
        excel = pd.ExcelFile(f)
        sheets = excel.sheet_names
        print 'sheets:'
        for s in sheets:
            print '    ' + s

def standardize_equipid(equipid, rd, kind):
    return '{2}{0}{1}'.format(rd, equipid[-2:], kind[0].upper())

def standardize_spe_location(field, home_id_standard, equip_id):
    df = pd.read_csv(os.getcwd() + '/input/home_equip_loc.csv')
    spe_dict = dict(zip(zip(df['home_id_standard'], df['equip_id']), df['specific_location']))
    living_list = ['LIVINGROOM', 'LIVING', 'LIVROOM', 'LIVRM', 'COLDZONE', 'UNTREATEDZONE']
    kitchen_list = ['KIT', 'KITCHEN', 'KIRCHEN', 'CLEANZONE']
    f1_list = ['1ST', '1STFL', '1STFLOOR']
    f2_list = ['2ND', '2NDFL', '2NDFLOOR']
    bedroom_list = ['BBEDROOM', 'BOYSBEDROOM', 'BEDROOM', 'BED']
    dining_list = ['DINING', 'DININGROOM']
    porch_list = ['PORCH', 'BACKPORCH']

    if field in living_list:
        return 'LIVINGROOM'
    elif field in kitchen_list:
        return 'KITCHEN'
    elif field in f1_list:
        return '1STFLOOR'
    elif field in f2_list:
        return '2NDFLOOR'
    elif field in bedroom_list:
        return 'BEDROOM'
    elif field in dining_list:
        return 'DININGROOM'
    elif field in porch_list:
        return 'PORCH'
    elif field == 'IRFRM':
        return 'FRONTROOM'
    elif field == 'CONF':
        return 'CONFERENCE'
    elif (home_id_standard, equip_id) in spe_dict:
        return spe_dict[(home_id_standard, equip_id)]
    # elif home_id_standard in spe_dict:
    #     if equip_id in spe_dict[home_id_standard]:
    #         return spe_dict[home_id_standard][equip_id]
    else:
        return field

def standardize_gen_location(field, specific, indoor_list,
                             outdoor_list, home_id_standard, equip_id):
    # print (field, specific, home_id_standard, equip_id)
    df = pd.read_csv(os.getcwd() + '/input/home_equip_ior.csv')
    gen_dict = dict(zip(zip(df['home_id_standard'], df['equip_id']), df['general_location']))
    df = pd.read_csv(os.getcwd() + '/input/home_room_ior.csv')
    room_dict = dict(zip(zip(df['home_id_standard'], df['room']), df['general_location']))
    if field in ['I', 'O', 'R']:
        # over write I/O/R location
        if (home_id_standard, equip_id) in gen_dict:
            return gen_dict[(home_id_standard, equip_id)]
        elif (home_id_standard, specific) in room_dict:
            return room_dict[(home_id_standard, specific)]
        else:
            return field
    if (home_id_standard, equip_id) in gen_dict:
        return gen_dict[(home_id_standard, equip_id)]
    elif specific in outdoor_list:
        return 'O'
    elif specific in indoor_list:
        if (home_id_standard, equip_id) in gen_dict:
            return gen_dict[(home_id_standard, equip_id)]
        elif (home_id_standard, specific) in room_dict:
            return room_dict[(home_id_standard, specific)]
        return 'I'
    else:
        # specific loc empty
        field = field[0]
        if field == 'F':
            return 'R'
        if field == '0':
            return 'O'
        elif (home_id_standard, equip_id) in gen_dict:
            return gen_dict[(home_id_standard, equip_id)]
        elif (home_id_standard, specific) in room_dict:
            return room_dict[(home_id_standard, specific)]
        return field

# parse file name
# kind: 'dylos', 'speck'
def parse_filename(filename, initial_list, home_id_dict, round_dict,
                   nb_dict, kind):
    d = {'equip_id': '-', 'home_id': '-', 'general_location': '-',
         'specific_location': '-', 'home_id_guess': '-',
         'equip_id_standard': '-', 'home_id_standard': '-',
         'specific_location_standard': '-',
         'general_location_standard': '-'}
    if filename == '':
        return d
    d['filename'] = filename

    # order is important
    df_replace = pd.read_csv(os.getcwd() + '/input/rename.csv')
    rename_dict = dict(zip(df_replace['old'], df_replace['new']))
    filename_process = filename
    for key in rename_dict:
        filename_process = filename_process.replace(str(key), str(rename_dict[key]))
    filename_process = filename_process.upper()
    df_replace_upper = pd.read_csv(os.getcwd() + \
                                   '/input/rename_upper.csv')
    rename_dict_upper = dict(zip(df_replace_upper['old'],
                                 df_replace_upper['new']))
    for key in rename_dict_upper:
        filename_process = filename_process.replace(str(key), str(rename_dict_upper[key]))
    field_list = filename_process.split('_')
    # print field_list

    if kind == 'dylos':
        prog_equip = re.compile('D[0-9]{3,4}')
    elif kind == 'speck':
        prog_equip = re.compile('S[0-9]{3,4}')
    prog_ID = re.compile('^[A-Z]{2,3}$')
    general_loc = ['inside', 'in', 'indoors', 'indoor', 'I',
                   'IDining', 'Oporch', 'outside', 'out', 'outdoors',
                   'outdoor', 'O', '0', 'roamer', 'roam', 'rover',
                   'R', 'F']
    general_loc_upper = [x.upper() for x in general_loc]
    specific_loc = ['garage', 'office', 'livingroom', 'kitchen',
                    'kirchen', 'kit', 'bbedroom',
                    'bedroom','1stfloor', '1stfl', '1st', '2nd',
                    '2ndfl', '2ndfloor', 'porch', 'living',
                    'kitchen2', 'kitchen1', 'boysbedroom', 'basement',
                    'Dfab', 'library', 'IRFRM', 'CONF', 'conference',
                    'K', 'dining', 'DiningRoom', 'front room',
                    'frontroom', 'upstairs', 'supplyAir', 'cleanzone',
                    'terrace', 'greenroof', 'storage', 'patio',
                    'coldzone', 'FamilyRoom', 'BackPorch', 'master',
                    'untreatedzone', 'office1', 'office2', 'roof']
    equip_id_lookup = pd.read_csv(os.getcwd() + \
                                  '/input/mis_equip_id_lookup.csv')
    mis_equip_id_dict = dict(zip(equip_id_lookup['filename'],
                                 equip_id_lookup['equip_id_standard']))
    specific_loc_upper = [x.upper() for x in specific_loc]
    outdoor_list = ['GARAGE', 'PORCH']
    indoor_list = [x for x in specific_loc_upper if not x in \
                   outdoor_list]
    field_list = [x.upper() for x in field_list]
    for x in field_list:
        # print x
        if x in general_loc_upper:
            # print 'general_loc_upper'
            d['general_location'] = x
            continue
        elif x in specific_loc_upper:
            # print 'specific_loc_upper'
            # print '1'
            d['specific_location'] = x
            continue
        elif re.search(prog_equip, x):
            # print 'equip_id'
            d['equip_id'] = re.search(prog_equip, x).group()
            continue
        elif x in initial_list:
            d['home_id'] = home_id_dict[x]
            # print d['home_id'], '22222222222222222222222222222222'
            continue
        elif (re.match(prog_ID, x) and d['home_id'] == '-'):
            d['home_id_guess'] = x
            continue
    d['home_id_standard'] = d['home_id']
    if d['home_id_standard'] == '-':
        d['home_id_standard'] = d['home_id_guess']
    id_correction_lookup = {'FS': 'BC', 'LMW': 'LIP', 'LMC': 'MLC',
                            'BS': 'WPS'}
    gen_file_lookup = pd.read_csv(os.getcwd() + '/input/file_ior.csv')
    file_gen_dict = dict(zip(gen_file_lookup['filename'],
                             gen_file_lookup['general_location']))
    for key in id_correction_lookup:
        d['home_id_standard'] = \
                d['home_id_standard'].replace(key, id_correction_lookup[key])
    # print d['home_id_standard'], '22222222222222222222222222222222'
    if d['home_id_standard'] in round_dict:
        d['round'] = round_dict[d['home_id_standard']]
    else:
        d['round'] = '-'
    if d['filename'] in mis_equip_id_dict:
        d['equip_id_standard'] = mis_equip_id_dict[d['filename']]
    else:
        d['equip_id_standard'] = standardize_equipid(d['equip_id'], d['round'], kind)

    d['specific_location_standard'] = \
            standardize_spe_location(d['specific_location'],
                                     d['home_id_standard'],
                                     d['equip_id_standard'])
    d['general_location_standard'] = \
            standardize_gen_location(d['general_location'],
                                     d['specific_location_standard'],
                                     indoor_list, outdoor_list,
                                     d['home_id_standard'],
                                     d['equip_id_standard'])
    if d['filename'] in file_gen_dict:
        d['general_location_standard'] = file_gen_dict[d['filename']]
    # print '{0},{1},{2},{3},{4}'.format(d['home_id_standard'],filename,
    #                                    d['general_location'],
    #                                    d['specific_location_standard'],
    #                                    d['home_id_standard'],
    #                                    d['equip_id_standard'])
    if kind == 'speck':
        if d['home_id_standard'] in nb_dict:
            d['neighborhood'] = nb_dict[d['home_id_standard']]
        else:
            d['neighborhood'] = '-'
    return d

def test_parse_filename():
    df = pd.read_csv(parent_dir(os.getcwd()) + ('/reformat/Dylos/round_test/name.csv'))
    keys = ['equip_id',
            'home_id', 'home_id_guess',
            'general_location', 'specific_location',
            'equip_id_standard', 'home_id_standard',
            'general_location_standard', 'specific_location_standard']
    df.fillna('', inplace=True)
    df.info()
    cols = ['round_{0}'.format(i) for i in range(1, 5)]
    for col in cols[1:2]:
        for key in keys:
            df['{0}_{1}'.format(col, key)] = df[col].map(lambda x: \
                    parse_filename(x)[key])

    df.to_csv(parent_dir(os.getcwd()) + ('/reformat/Dylos/round_test/name_cvt.csv'), index=False)

def convert_unit(folder, suffix):
    lookupfiles = glob.glob(parent_dir(os.getcwd()) + folder + \
            'reform_summary/*.csv')
    df_unitlookup = pd.concat([pd.read_csv(f) for f in lookupfiles],
                              ignore_index=True)
    # print df_unitlookup.head()
    m_dict = dict(zip(df_unitlookup['filename'], df_unitlookup['multiplier']))

    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    categories = list(pd.read_csv(filelist[0]))
    categories.remove('Date/Time')
    print categories
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        # print filename
        if filename in m_dict:
            m = m_dict[filename]
        else:
            m = 1
        df = pd.read_csv(f)
        for cate in categories:
            df[cate] = df[cate].map(lambda x: int(round(x * m, 0)))
        outfile = f.replace('reformat', 'convert_unit')
        df.to_csv(outfile, index=False)
    return

def summary_stat(sensor, step, x, suffix):
    filelist = glob.glob(util.get_path(sensor, step, x) + suffix)
    filelist = [z for z in filelist if '.' in z]
    # filelist = filelist[-3:]
    # print (filelist)
    # print filelist.index(util.get_path(sensor, step, x) + 'LIP_I_D053_01-09-17_Kltchen_putty.txt')
    categories = list(pd.read_csv(filelist[0]))
    categories.remove('Date/Time')
    if 'date_iso' in categories:
        categories.remove('date_iso')
    suf = 'round_{0}'.format(x)
    df_dict = dict(zip(categories, [[] for i in
                                    range(len(categories))]))
    is_exist = os.path.isfile(util.get_path(sensor, step, x) + 'stat_summary/Small_round_{0}.csv'.format(x))
    if is_exist:
        summarys = {c: pd.read_csv(util.get_path(sensor, step, x) + 'stat_summary/{0}_round_{1}.csv'.format(c, x)) for c in categories}
        existing = summarys['Small']['filename'].unique()
    else:
        existing = []
    # print df_dict
    counter = 0
    emptys = ['filename\n']
    for f in filelist:
        filename = f[f.rfind('/') + 1:]
        if filename in existing:
            continue
        else:
            print counter, filename
            counter += 1
        df = pd.read_csv(f)
        if len(df) == 0:
            df_cate = df.copy()
            emptys.append(filename + '\n')
            # df_disc_cate['filename'] = filename
            continue
        # df['Date/Time'] = df['Date/Time'].map(util.correct_month) 
        df['Date/Time'] = pd.to_datetime(df['Date/Time'])
        for cate in categories:
            df_cate = df.copy()
            df_cate = df_cate[['Date/Time', cate]]
            df_disc_cate = int_describe(df_cate.describe())
            df_disc_cate = df_disc_cate.transpose()
            df_disc_cate['filename'] = filename
            df_disc_cate['Raw Start Time'] = df['Date/Time'].min()
            df_disc_cate['Raw End Time'] = df['Date/Time'].max()
            df_disc_cate['Raw duration/[min]'] = \
                    (df_disc_cate['Raw End Time'] - \
                     df_disc_cate['Raw Start Time']) / np.timedelta64(1,'m') + 1
            df_disc_cate['missing data count'] = \
                    df_disc_cate['Raw duration/[min]'] - df_disc_cate['count']
            df_dict[cate].append(df_disc_cate)

    for cate in categories:
        if is_exist:
            summarys[cate].rename(columns={'0.25': '25%', '0.5': '50%', '0.75': '75%'}, inplace=True)
            summarys[cate].info()
            df_all = pd.concat(df_dict[cate] + [summarys[cate]], ignore_index=True)
        else:
            df_all = pd.concat(df_dict[cate], ignore_index=True)
        df_all.drop_duplicates(cols=['filename'], inplace=True)
        df_all.to_csv(util.get_path(sensor, step, x) + \
                      'stat_summary/{0}_{1}.csv'.format(cate, suf),
                      index=False)
    with open (util.get_path(sensor, step, x) + \
               'stat_summary/empty_{0}.csv'.format(cate), 'w+') as wt:
        wt.write(''.join(emptys))

def remove_suffix(s):
    idx1 = s.rfind('_')
    return s[:idx1] + s[-4:]

# kind: 'dylos', 'speck'
def summary_label(folder, suffix, kind):
    print 'summary_label'
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    dfs_label = []
    df_lookup = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv')
    df_lookup.sort('ROUND', inplace=True)
    df_lookup.to_csv(util.get_path('Dylos', 'temp', 'all') + 'lookup.csv', index=False)
    df_round = df_lookup.copy()
    df_round.drop_duplicates(cols=['HOME ID CORRECT'], take_last=False, inplace=True)
    df_round.to_csv(util.get_path('Dylos', 'temp', 'all') + 'round.csv', index=True)
    initial_list = list(set(df_lookup['INITIALS'].tolist()))
    initial_list = [x.replace(' ', '') for x in initial_list]
    correct_list = list(set(df_lookup['HOME ID CORRECT']))
    initial_list = initial_list + correct_list
    home_id_dict = dict(zip(df_lookup['INITIALS'], df_lookup['HOME ID CORRECT']) + zip(df_lookup['HOME ID CORRECT'], df_lookup['HOME ID CORRECT']))
    round_dict = dict(zip(df_round['HOME ID CORRECT'], df_round['ROUND']))
    nb_dict = dict(zip(df_lookup['HOME ID CORRECT'],
                        df_lookup['NEIGHBORHOOD']))
    if np.nan in round_dict:
        del round_dict[np.nan]
    for key in round_dict:
        round_dict[key] = int(round_dict[key])

    # filelist = [x for x in filelist if 'MJM I S611 HIPK_2016-04-30_to_2016-05-23.csv' in x]
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        df_lb = pd.DataFrame(parse_filename(filename, initial_list,
                                            home_id_dict, round_dict,
                                            nb_dict, kind),
                             index=[0])
        # print df_lb.transpose()
        dfs_label.append(df_lb)

    df_label = pd.concat(dfs_label, ignore_index = True)
    cols = list(df_label)
    cols.remove('filename')
    cols.insert(0, 'filename')
    df_overwrite = pd.read_csv('/media/yujiex/work/ROCIS/ROCIS/routines/input/location/csv coorections by LW_01-28-17.csv')
    df_overwrite = df_overwrite[['file', 'LW Correction']]
    df_overwrite['file'] = df_overwrite['file'].map(remove_suffix)
    ori_dict = dict(zip(df_label['filename'], df_label['general_location_standard']))
    overwrite_dict = dict(zip(df_overwrite['file'], df_overwrite['LW Correction']))
    ori_dict.update(overwrite_dict)
    df_label['general_location_standard'] = df_label['filename'].map(ori_dict)
    df_label = df_label[cols]
    df_label.to_csv(parent_dir(os.getcwd()) + \
              folder + 'label_summary/label.csv', index=False)

def summary_all_dylos_reformstep(folder):
    dirname = (parent_dir(os.getcwd()) + folder)
    files_reform = glob.glob(dirname + 'reform_summary/*.csv')
    df_reform = pd.concat([pd.read_csv(x) for x in files_reform],
                          ignore_index=True)
    df_label = pd.read_csv(dirname + 'label_summary/label.csv')
    files_stat = glob.glob(dirname + 'stat_summary/*.csv')
    for f in files_stat:
        filename = f[f.rfind('/') + 1:]
        df_stat = pd.read_csv(f)
        df_1 = pd.merge(df_reform, df_label, on='filename', how='outer')
        df_2 = pd.merge(df_1, df_stat, on='filename', how='outer')
        df_2.drop('oldUnit', axis=1, inplace=True)
        df_2.to_csv(dirname + 'summary/{0}'.format(filename), index=False)

def summary_all_general(folder, kind, x):
    dirname = (parent_dir(os.getcwd()) + folder)
    df_label = pd.read_csv(dirname + 'label_summary/label.csv')
    files_stat = glob.glob(dirname + 'stat_summary/*_{0}.csv'.format(x))
    def filename_standard_d(row):
        try: 
            result = '{0}_{1}_{2}_{3}'.format(row['home_id_standard'],
                row['general_location_standard'],
                row['equip_id_standard'],
                (row['Raw End Time']).strftime('%m-%d-%y'))
        except ValueError:
            print row['filename']
            result = '{0}_{1}_{2}_{3}'.format(row['home_id_standard'],
                row['general_location_standard'],
                row['equip_id_standard'],
                (row['Raw End Time']).strftime('%m-%d-%Y'))
        return result
    for f in files_stat:
        filename = f[f.rfind('/') + 1:]
        datecols = ['Raw Start Time', 'Raw End Time']
        df_stat = pd.read_csv(f, parse_dates=datecols)
        # df_1 = pd.merge(df_stat, df_label, on='filename', how='outer')
        # keep non-empty files
        df_1 = pd.merge(df_stat, df_label, on='filename', how='inner')
        if kind == 'dylos':
            df_1.to_csv(util.get_path('Dylos', 'temp', x) + 'merge.csv', index=False)
            df_1['filename_standard'] = df_1.apply(lambda row: \
                filename_standard_d(row), axis=1)
        else:
            df_1['filename_standard'] = df_1.apply(lambda row: \
                    '{0}_{1}_{2}_{3}'.format(row['home_id_standard'],
                                            row['general_location_standard'],
                                            row['equip_id_standard'],
                                            row['neighborhood']), axis=1)
        df_1.to_csv(dirname + 'summary/{0}'.format(filename), index=False)
    print 'summary_all_gen'

def summary_dylos(folder, suffix):
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    print filelist[0]
    categories = list(pd.read_csv(filelist[0]))
    categories.remove('Date/Time')
    print categories

    df_lookup = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_02-05-15_10 PM.csv')
    for cate in categories:
        dfs = []
        dfs_label = []
        for f in filelist:
            filename = f[(f.find(folder) + len(folder)):]
            df = pd.read_csv(f, parse_dates = ['Date/Time'])
            df = df[['Date/Time', cate]]
            df_disc = int_describe(df.describe())
            df_disc = df_disc.transpose()
            # NOTE: supper slow to convert to datetime, but to take max and min
            # this seems necessary
            # find ways to speed this up later
            print df.head()
            df_disc['filename'] = filename
            df_disc['Raw Start Time'] = (df['Date/Time'].min())
            df_disc['Raw End Time'] = (df['Date/Time'].max())
            df_disc['Raw duration/[min]'] = \
                    (df_disc['Raw End Time'] - df_disc['Raw Start Time']) / \
                    np.timedelta64(1,'m') + 1
            df_disc['missing data count'] = \
                    df_disc['Raw duration/[min]'] - df_disc['count']
            df_disc['Units'] = 'hundredth cubic feet'
            dfs.append(df_disc)
            suf = folder[folder[:-1].rfind('/') + 1:][:-1]
            df_lb = pd.DataFrame(parse_filename(filename, initial_list, home_id_dict, round_dict))
            dfs_label.append(df_lb)

        df_stat = pd.concat(dfs, ignore_index = True)
        df_nameinfo = pd.concat(dfs_label, ignore_index = True)
        df_all = pd.merge(df_stat, df_nameinfo, on='filename')
        df_all['filename_standard'] = df_all.apply(lambda row: \
                '{0}_{1}_{2}_{3}'.format(row['home_id_standard'],
                                         row['general_location_standard'],
                                         row['equip_id_standard'],
                                         (row['Raw End Time']).strftime('%m-%d-%y')), axis=1)

        df_all.to_csv(parent_dir(os.getcwd()) + \
                  folder + '/summary/{0}_{1}.csv'.format(cate, suf),
                  index=False)

def dropdup(folder, suffix):
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    for f in filelist:
        filename = f[f.rfind('/') + 1:]
        # print filename
        df = pd.read_csv(f)
        df['Date/Time'] = df['Date/Time'].map(util.correct_month) 
        df.drop_duplicates(cols='Date/Time', take_last=True, inplace=True)
        outfile = f.replace('convert_unit', 'dropdup')
        df.to_csv(outfile, index=False)
        
def resample_speck(folder, step):
    filelist = glob.glob(folder + '*.csv')
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        df = pd.read_csv(f)
        df.rename(columns={'sample_timestamp_unix_time_secs':
                           'EpochTime'}, inplace=True)
        df['Date/Time'] = (pd.to_datetime(df['EpochTime'], unit = 's'))
        df.set_index('Date/Time', inplace=True)
        df.drop('EpochTime', inplace=True, axis=1)
        df_r = df.resample(step, how='mean', label='right')
        df_r.to_csv('{0}{1}/{2}'.format(folder, step, filename))
        print filename
    return

# print Speck specifications
def summary_speck_stat(folder, suffix):
    pd.options.display.show_dimensions = False
    filelist = glob.glob(folder + suffix)
    categories = list(pd.read_csv(filelist[0]))
    if 'EpochTime' in categories:
        categories.remove('EpochTime')
        categories = [col[col.rfind('.') + 1:] for col in categories]
    else:
        categories.remove('sample_timestamp_unix_time_secs')
    print categories
    for cate in categories:
        dfs = []
        for f in filelist:
            filename = f[(f.find(folder) + len(folder)):]
            df = pd.read_csv(f)
            df.rename(columns={'sample_timestamp_unix_time_secs':
                               'EpochTime'}, inplace=True)
            colname = ''
            for col in df:
                if cate in col:
                    colname = col
            if colname == '':
                continue
            df = df[['EpochTime', colname]]
            df['Date/Time'] = pd.to_datetime(df['EpochTime'], unit = 's')
            df.drop('EpochTime', inplace=True, axis=1)
            cols = list(df)
            newcols = [col[col.rfind('.') + 1:] if col != 'Date/Time' \
                                                else col for col in cols]

            df.columns = newcols
            df_disc = int_describe(df.describe())
            df_disc = df_disc.transpose()
            df_disc['filename'] = filename
            df_disc['Raw Start Time'] = df['Date/Time'].min()
            df_disc['Raw End Time'] = df['Date/Time'].max()
            order_cols = list(df_disc)
            order_cols = order_cols[-3:] + order_cols[:-3]
            df_disc = df_disc[order_cols]
            dfs.append(df_disc)
            suf = folder[folder[:-1].rfind('/') + 1:][:-1]
        outfile = '{0}stat_summary/{1}_{2}.csv'.format(folder, cate, suf)
        print outfile
        pd.concat(dfs, ignore_index = True).to_csv(outfile,index=False) 

def iso2sensor(string):
    string = string.replace(' ', '_')
    string = string.replace('-', '_')
    tokens = string.split('_')
    return '{0}/{1}/{2} {3}'.format(tokens[1], tokens[2],
                                    tokens[0][2:], tokens[3][:5])

def copy_excel():
    files = glob.glob(util.get_path('Dylos', 'raw_data', 'excel') + '*.csv')
    for f in files:
        df = pd.read_csv(f)
        df['Date/Time'] = df['Date/Time'].map(lambda x: iso2sensor(x))
        df['Small'] = df['Small'].map(int)
        df['Large'] = df['Large'].map(int)
        outfile = f.replace('excel', 'all')
        outfile = outfile.replace('.csv', '.txt')
        # outfile = outfile.replace('raw_data', 'reformat')
        outfile = outfile.replace('raw_data', 'reform_')
        print 'copy file {0}'.format(outfile)
        df.to_csv(outfile, index=False)

def copy_o_bhw2kd():
    def rename(string):
        string = string.replace('MOM', 'FARM')
        string = string.replace('BHW', 'KD')
        return string
    for kind in ['Small', 'Large']:
        df_summary = pd.read_csv(util.get_path('Dylos', 'chop_start', 'all') + 'summary/{0}_round_all_unique.csv'.format(kind))
        df = df_summary.copy()
        df = df[(df['general_location_standard'] == 'O') & (df['home_id_standard'] == 'BHW')]
        df['filename'] = df['filename'].map(rename)
        print df[['filename']]
        df_all = pd.concat([df_summary, df], ignore_index=True)
        df_all.to_csv(util.get_path('Dylos', 'chop_start', 'all') + 'summary/{0}_round_all_unique_copyfile.csv'.format(kind), index=False)
    filenames = df['filename'].tolist()
    for x in filenames:
        infile = util.get_path('Dylos', 'chop_start', 'all') + x
        outfile = rename(infile)
        if infile != outfile:
            shutil.copy(infile, outfile)
            print '{0} -> {1}'.format(infile[infile.rfind('/') + 1:], outfile[outfile.rfind('/') + 1:])
    return

def cleaning_dylos(x):
    # manual modifications:
    # 1. copy the files from 'round_unknown' to 'round_all'
    # 2. copy the files of 'MOM_0*' or 'MOM_O*' and rename MOM to FARM
    # as is outdoor Dylos downloads of the MOM files (BHW) matches
    # outdoor files for Farm or KD.
    # 3. copy from round_excel to round_all
    # copy_excel()

    # need to replace 'new_data' folder with newly downloaded
    newfiles = glob.glob(util.get_path('Dylos', 'new_data') + '*')
    for infile in newfiles:
        shutil.copyfile(infile, infile.replace('new_data', 'raw_data/round_all'))
        print 'copy {0}'.format(infile[infile.rfind('/') + 1:])
    # # use this if need to process all raw data
    # # files = glob.glob(util.get_path('Dylos', 'raw_data', 'all') + '*')
    files = [z.replace('new_data', 'raw_data/round_all') for z in newfiles]
    mlines = ['filename,multiplier\n']
    for i, f in enumerate(files):
        if i % step_size == 0:
            print i
        m = cd.cleaning(f, f.replace('raw_data', 'reform_'))
        mlines.append('{0},{1}\n'.format(f[f.rfind('/') + 1:], m))
    with open (util.get_path('Dylos', 'reform_', 'all') + 'missing_summary/m.csv', 'w+') as wt:
        wt.write(''.join(mlines))

    summary_stat('Dylos', 'reform_', 'all', '*')
    summary_label('/DataBySensor/Dylos/reform_/round_{0}/'.format(x), '*.[a-z][a-z][a-z]', 'dylos')
    summary_all_general('/DataBySensor/Dylos/reform_/round_{0}/'.format(x), 'dylos', 'all')
    remove_dup_files('reform_', x)
    correct_time()
    chop_wrong_time()
    summary_stat('Dylos', 'chop_start_', 'all', '*.[a-z][a-z][a-z]')
    summary_label('/DataBySensor/Dylos/chop_start_/round_all/',
                  '*.[a-z][a-z][a-z]', 'dylos')
    summary_all_general('/DataBySensor/Dylos/chop_start_/round_all/',
                        'dylos', 'all')
    remove_dup_files('chop_start_', 'all')
    copy2dropbox()
    return

# not using copy_o_bhw2kd(), because Linda is manually copying

def check_dup_subfolder(folder, suffix):
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    print filelist[0]
    for f in filelist:
        filename = f[f.rfind('/') + 1:]
        print filename
        df = pd.read_csv(f)
        num_rows = len(df)
        unique_time = len(df['Date/Time'].unique())
        print (num_rows, unique_time)
        assert(num_rows == unique_time)

def check_dup(x):
    check_dup_subfolder('/dropdup/Dylos/round_{0}/'.format(x),
                        '*.[a-z][a-z][a-z]')
    return

def summary_label_round(x, kind):
    summary_label('/DataBySensor/Dylos/dropdup/round_{0}/'.format(x), '*.[a-z][a-z][a-z]', kind)

def summary_all(x):
    summary_all_general('/dropdup/Dylos/round_{0}/'.format(x), 'dylos')

def excel2csv(folder, suffix, nskip, header, datasheets, add_filename):
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        excel = pd.ExcelFile(f)
        file_sheets = excel.sheet_names
        print file_sheets
        if datasheets == None:
            sheets = file_sheets
        else:
            sheets = datasheets
        for sheet in sheets:
            print sheet
            if 'Sheet' in sheet:
                continue
            df = pd.read_excel(f, sheetname=sheet, skiprows=nskip,
                               header=header)
            if add_filename:
                filename = filename.replace('.xlsx', '')
                outfilename = (parent_dir(os.getcwd()) + folder + \
                        '/csv/{0}_{1}.csv'.format(filename, sheet))
            else:
                outfilename = (parent_dir(os.getcwd()) + folder + \
                        '/csv/{0}.csv'.format(sheet))
            df.to_csv(outfilename, index=False)

def add_suf(path, suf):
    dot_idx = path.rfind('.')
    return path[:dot_idx] + suf + path[dot_idx:]

def sep_csv(folder, suffix):
    d = {'Dylos - week 1.csv': [[1, 2, 3], [6, 7, 8, 9], [12, 13, 14]],
         'Dylos - week 2.csv': [[1, 2, 3], [6, 7, 8], [11, 12, 13]],
         'Dylos-week3.csv': [[1, 2, 3], [6, 7, 8], [11, 12, 13]]}
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    print filelist
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        df = pd.read_csv(f)
        length = (len(d[filename]))
        print length
        for i in range(length):
            df_1 = df.copy()
            print d[filename][i]
            df_1 = df_1[d[filename][i]]
            outfile = f.replace('/csv/', '/sep/')
            outfile = add_suf(outfile, '_{0}'.format(i))
            df_1.to_csv(outfile, index=False)

def reform_unitconvert_excel(folder, suffix, multiply):
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        outfile = f.replace('/csv/', '/csv_unit_convert/')
        print filename
        df_check = pd.read_csv(f)
        df_check.info()
        if len(list(df_check)) == 3:
            df = pd.read_csv(f)
        else:
            df = pd.read_csv(f, parse_dates = [[0, 1]])
        col_list = list(df)
        df.rename(columns={col_list[0]: 'Date/Time',
                           col_list[1]: 'Small',
                           col_list[2]: 'Large'}, inplace=True)
        for col in ['Small', 'Large']:
            df[col] = df[col] * multiply
        df.dropna(axis=1, how='all', inplace=True)
        df.to_csv(outfile, index=False)
        cp_file = util.get_path('Dylos', 'raw_data', 'excel') + filename
        df.to_csv(cp_file, index=False)

def parse_excel():
    kind = 'dylos'
    print 'start parsing excel'
    # excel2csv('/DataBySensor/Dylos/round_excel/PIZ/', '*.xlsx', 7, 7, None, False)
    # reform_unitconvert_excel('/DataBySensor/Dylos/round_excel/PIZ/csv/', '*.csv', 1/100)
    # excel2csv('/DataBySensor/Dylos/raw_data/round_excel/DM/', '*.xlsx', 7, 7, None, True)
    reform_unitconvert_excel('/DataBySensor/Dylos/raw_data/round_excel/DM/csv/', '*.csv', 1)
    # excel2csv('/DataBySensor/Dylos/round_excel/JAZ/', '*.xlsx', 0, 0,
    #          ['Dylos - week 1', 'Dylos - week 2', 'Dylos-week3'])
    # sep_csv('/DataBySensor/Dylos/round_excel/JAZ/csv/', '*.csv')
    # reform_unitconvert_excel('/RawData Unsorted/Dylos/round_excel/JAZ/sep/', '*.csv')
    # summary_stat('/RawData Unsorted/Dylos/round_{0}/'.format('excel'), '*.csv')
    # summary_label('/RawData Unsorted/Dylos/round_{0}/'.format('excel'), '*.csv', kind)
    # summary_all_general('/RawData Unsorted/Dylos/round_{0}/'.format('excel'))
    return

# remove duplicate files in the summary folder by statistic and home ID
def remove_dup_files(step, x):
    gb_factor = ['home_id_standard', '50%', 'max', 'std', 'Raw Start Time', 'Raw End Time']
    files_todrop = ['LIP_O_Porch_106_12-19-15_BHW_ PORCH_putty.txt']
    for kind in ['Large', 'Small']:
        f = util.get_path('Dylos', step, x) + \
            'summary/{0}_round_{1}.csv'.format(kind, x)
        df = pd.read_csv(f)
        print
        print len(df)
        df.sort(['Raw Start Time', 'Raw End Time', 'general_location_standard'], inplace=True)
        df3 = df.groupby(gb_factor).last()
        print len(df3)
        df3.reset_index(inplace=True)
        cols = set(list(df3))
        df3['equip_id_no_cohort'] = df3['equip_id_standard'].map(lambda x: x[:1] + '0' + x[-2:])
        head = ['home_id_standard', 'filename', 'equip_id_no_cohort',
                'general_location_standard',
                'specific_location_standard', 'equip_id_standard',
                'Raw Start Time', 'Raw End Time']
        tail = cols.difference(set(head))
        newcols = head + list(tail)
        df3 = df3[newcols]
        df3.sort(['home_id_standard', 'Raw End Time', 'general_location_standard'], inplace=True)
        df3 = df3[~df3['filename'].isin(files_todrop)]
        df3.to_csv(f.replace('.csv', '_unique.csv'), index=False)
    return
                                                                    
def join_latlong():
    filelist = glob.glob(parent_dir(os.getcwd()) + '/DataBySensor/Speck/round_all_bulkdownload/summary/*.csv')
    df_latlong = pd.read_csv(os.getcwd() + '/input/feed_info_withname.csv')
    df_latlong = df_latlong[['filename', 'latitude', 'longitude']]
    for f in filelist:
        df = pd.read_csv(f)
        df_all = pd.merge(df, df_latlong, how='left', on='filename')
        df_all.to_csv(f, index=False)

def drop_small_count(dirname):
    filelist = glob.glob(parent_dir(os.getcwd()) + '/DataBySensor/Speck/raw_data/round_{0}/summary/*.csv'.format(dirname))
    for f in filelist:
        df = pd.read_csv(f)
        df = df[df['count'] > 60]
        cols = list(df)
        head = ['home_id_standard', 'filename', 'general_location_standard', 'specific_location_standard', 'equip_id_standard']
        tail = [x for x in cols if x not in head]
        new_cols = head + tail
        df = df[new_cols]
        df.replace({'home_id_standard': {'CSV': '-'}}, inplace=True)
        df.sort(['home_id_standard', 'Raw End Time', 'general_location_standard'], inplace=True)
        df.to_csv(f.replace('.csv', '_keepLargeCount.csv'),
                            index=False)
    return

def join_static_rounddate(kind, round):
    print 'join cohort start and end date info'
    if kind == 'dylos':
        filelist = glob.glob(dylos_summary_path.replace('all', round) \
                             + '*.csv')
    else:
        filelist = glob.glob(speck_summary_path + '*.csv')
    df_round = pd.read_csv(os.getcwd() + '/input/cohort_time.csv')
    df_round['round'] = df_round['round'].map(lambda x: str(x))
    for f in filelist:
        df = pd.read_csv(f)
        df2 = pd.merge(df, df_round, on='round', how='left')
        df2.to_csv(f, index=False)
    return

# gb_list is a list of columns to group by
def concat_dylos(gb_list,cohort=None, history=True, home=None):
    print 'concatenating dylos files by home id standard ...'
    df_summary = pd.read_csv(util.get_path('Dylos', 'chop_start_', 'all') + 'summary/Small_round_all_unique.csv')

    if not cohort is None:
        if not history:
            df_id = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv')
            ids = df_id[df_id['ROUND'] == cohort]['HOME ID CORRECT']
        else:
            df_id = pd.read_csv(os.getcwd() + '/input/participant_cohort_ongoing.csv')
            ids = df_id[df_id['cohort'] == cohort]['home_id_standard']
        df_summary = df_summary[df_summary['home_id_standard'].isin(ids)]
    if not home is None:
        df_summary = df_summary[df_summary['home_id_standard'] == home]
    gr = df_summary.groupby(['home_id_standard'] + gb_list)
    suf = '_'.join(map(lambda x: x[:3], gb_list))
    path = util.get_path('Dylos', 'concat_{0}'.format(suf), 'all')
    for name, group in (list(gr)):
        print 'write to home: {0}'.format(name)
        files = group['filename'].tolist()
        dfs = []
        for f in files:
            df = pd.read_csv(util.get_path('Dylos', 'chop_start_', 'all') + f)
            # df['filename'] = f
            dfs.append(df)
        df_all = pd.concat(dfs, ignore_index=False)
        df_all['Date/Time'] = pd.to_datetime(df_all['Date/Time'])
        df = df_all.sort(columns=['Date/Time'])
        df.drop_duplicates(cols=['Date/Time'], inplace=True)
        print path
        df.to_csv('{0}{1}.csv'.format(path,
                                       '_'.join(name)), index=False)
    # summary_stat('/DataBySensor/Dylos/concat/round_all/', '*.[a-z][a-z][a-z]')
    return

def merge_loc(concat_path,cohort=None,history=True, home=None):
    files = glob.glob(util.get_path('Dylos', concat_path, 'all') + '*.csv')
    if not cohort is None:
        if not history:
            df_id = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv')
            ids = df_id[df_id['ROUND'] == cohort]['HOME ID CORRECT']
        else:
            df_id = pd.read_csv(os.getcwd() + '/input/participant_cohort_ongoing.csv')
            ids = df_id[df_id['cohort'] == cohort]['home_id_standard']
    if not home is None:
        ids = [home]
    files = reduce(lambda x, y: x + y, [[x for x in files if m in x] for m in ids])
    filenames = [f[f.rfind('/') + 1:] for f in files]
    df = pd.DataFrame({'filename': filenames})
    df['home_id'] = df['filename'].map(lambda x: x[:x.find('_')])
    gr = df.groupby('home_id')
    cat = ['Small', 'Large']
    # for name, group in gr:
    # name = 'AE'
    # group = gr.get_group(name)
    merge_path = concat_path.replace('concat', 'merge')
    for name, group in list(gr):
        print name
        directory = util.get_path('Dylos', merge_path, 'all') + \
            'plot/{0}'.format(name)
        if not os.path.exists(directory):
            os.makedirs(directory)
        infile = util.get_path('Dylos', merge_path, 'all') + 'plot/dygraph-combined-dev.js'
        outfile = infile.replace('plot', 'plot/{0}'.format(name))
        shutil.copyfile(infile, outfile)
        fs = group['filename'].tolist()
        dfs = []
        for f in fs:
            df = pd.read_csv(util.get_path('Dylos', concat_path, 'all') + f)
            df['location'] = f[f.find('_') + 1:f.find('.')]
            dfs.append(df)
        df_all = pd.concat(dfs, ignore_index=False)
        with open(os.getcwd() + '/input/template.html', 'r') as rd:
            lines = rd.readlines()
        lines_embed = [x.replace('dygraph-combined-dev.js', '//cdnjs.cloudflare.com/ajax/libs/dygraph/1.1.1/dygraph-combined.js') for x in lines]
        def replace(string, name, cat, csvstring=None):
            if not csvstring is None:
                string = string.replace('AE_Small_15T.csv', csvstring)
            string = string.replace('AE', name)
            string = string.replace('Small', cat)
            return string
        for c in cat:
            df_temp = df_all.copy()
            df_temp = df_temp[['Date/Time', c, 'location']]
            df_temp.drop_duplicates(cols=['Date/Time', 'location'], inplace=True)
            df_p = df_temp.pivot(index='Date/Time', columns='location', values=c)
            df_p.to_csv('{0}{1}_{2}.csv'.format(util.get_path('Dylos', merge_path, 'all'), name, c))
            df_p.set_index(pd.DatetimeIndex(pd.to_datetime(df_p.index)), inplace=True)
            df_r = df_p.resample('15T', how='mean')
            csvfile = '{0}plot/{1}/{1}_{2}_15T.csv'.format(util.get_path('Dylos', merge_path, 'all'), name, c)
            df_r.to_csv(csvfile)
            with open(csvfile, 'r') as rd:
                csvlines = rd.readlines()
            csvstring = '\n'.join(['"{0}\\n" + '.format(x[:-1]) for x in csvlines])
            # csvstring = ''.join(['{0}\\n'.format(x[:-1]) for x in csvlines])
            csvstring = csvstring[1:-4]
            newlines_embed = [replace(x, name, c, csvstring) for x in lines_embed]
            newlines_csv = [replace(x, name, c) for x in lines]
            with open(util.get_path('Dylos', merge_path, 'all') + 'plot/{0}/{0}_{1}.html'.format(name, c), 'w+') as wt:
                wt.write(''.join(newlines_embed))
            with open(util.get_path('Dylos', merge_path, 'all') + 'plot/{0}/{0}_{1}_sep.html'.format(name, c), 'w+') as wt:
                wt.write(''.join(newlines_csv))

def mergeIRO():
    files = glob.glob(util.get_path('Dylos', 'concat', 'all') + '*.csv')
    filenames = [f[f.rfind('/') + 1:] for f in files]
    df = pd.DataFrame({'filename': filenames})
    df['home_id'] = df['filename'].map(lambda x: x[:x.find('_')])
    gr = df.groupby('home_id')
    cat = ['Small', 'Large']
    # for name, group in gr:
    # name = 'AE'
    # group = gr.get_group(name)
    for name, group in list(gr):
        print name
        directory = util.get_path('Dylos', 'mergeIRO', 'all') + \
            'plot/{0}'.format(name)
        if not os.path.exists(directory):
            os.makedirs(directory)
        infile = util.get_path('Dylos', 'mergeIRO', 'all') + 'plot/dygraph-combined-dev.js'
        outfile = infile.replace('plot', 'plot/{0}'.format(name))
        shutil.copyfile(infile, outfile)
        fs = group['filename'].tolist()
        dfs = []
        for f in fs:
            df = pd.read_csv(util.get_path('Dylos', 'concat', 'all') + f)
            df['location'] = f[f.find('_') + 1:f.find('.')]
            dfs.append(df)
        df_all = pd.concat(dfs, ignore_index=False)
        with open('Dylos', util.get_path('Dylos', 'mergeIRO', 'all') + 'plot/template.html', 'r') as rd:
            lines = rd.readlines()
        def replace(string, name, cat):
            string = string.replace('AE', name)
            string = string.replace('Small', cat)
            return string
        for c in cat:
            df_temp = df_all.copy()
            df_temp = df_temp[['Date/Time', c, 'location']]
            df_p = df_temp.pivot(index='Date/Time', columns='location', values=c)
            df_p.to_csv('{0}{1}_{2}.csv'.format(util.get_path('Dylos', 'mergeIRO', 'all'), name, c))
            df_p.set_index(pd.DatetimeIndex(pd.to_datetime(df_p.index)), inplace=True)
            df_r = df_p.resample('15T', how='mean')
            df_r.to_csv('{0}plot/{1}/{1}_{2}_15T.csv'.format(util.get_path('Dylos', 'mergeIRO', 'all'), name, c))
            newlines = [replace(x, name, c) for x in lines]
            with open(util.get_path('Dylos', 'mergeIRO', 'all') + 'plot/{0}/{0}_{1}.html'.format(name, c), 'w+') as wt:
                wt.write(''.join(newlines))

def summary_by_home():
    summary_stat('/DataBySensor/Dylos/concat/round_all/', '*.[a-z][a-z][a-z]')
    filelist = glob.glob(dylos_concat_path + 'stat_summary/*.csv')
    for f in filelist:
        print f
        df = pd.read_csv(f)
        df['home_id'] = df['filename'].map(lambda x: x[:x.find('_')])
        df['location'] = df['filename'].map(lambda x: x[-5])
        df.drop(['Raw duration/[min]', 'missing data count'], axis=1,
                inplace=True)
        df.to_csv(f, index=False)
    return

def location_summary_dylos(location):
    print 'summary of location: {0}'.format(location)
    filelist = glob.glob(dylos_concat_path + \
                         '/*_{0}.csv'.format(location))
    dfs = [pd.read_csv(f) for f in filelist]
    df_all = pd.concat(dfs, ignore_index=True)
    df_all['time_in_range'] = df_all['Date/Time'].map(lambda x: right_year(x))
    df_all = df_all[df_all['time_in_range']]
    df_all.drop('time_in_range', axis=1, inplace=True)
    df_all.set_index(pd.DatetimeIndex(df_all['Date/Time']),
                     inplace=True)
    df_all.to_csv(dylos_concat_path + \
                  'summary/{0}_concat.csv'.format(location))
    df_re = df_all.resample('M', how='mean')
    df_re['month'] = df_re.index.map(lambda x: x.month)
    df_re['year'] = df_re.index.map(lambda x: x.year)
    df_re.rename(columns={'Small': 'Small_mean', 'Large':
                          'Large_mean'}, inplace=True)
    df_temp = df_all.copy()
    df_temp['month'] = df_temp.index.map(lambda x: x.month)
    df_temp['year'] = df_temp.index.map(lambda x: x.year)
    df_re_2 = df_temp.groupby(['year', 'month']).median()
    df_re_2.rename(columns={'Small': 'Small_median', 'Large':
                            'Large_median'}, inplace=True)
    df_re_1 = df_re.groupby(['year', 'month']).mean()
    df = pd.merge(df_re_1, df_re_2, left_index=True, right_index=True, how='inner')
    df.to_csv(dylos_concat_path + \
              'summary/{0}_mean_median.csv'.format(location))
    return
    
def summary_median():
    filelist = glob.glob(dylos_cleaned_path + '*.*')
    dfs = []
    for i, f in enumerate(filelist):
        filename = f[f.rfind('/') + 1:]
        print (i, filename)
        df = pd.read_csv(f)
        df.set_index(pd.DatetimeIndex(pd.to_datetime(df['Date/Time'])), inplace=True)
        df['month'] = df.index.map(lambda x: x.month)
        df['year'] = df.index.map(lambda x: x.year)
        df2 = df.groupby(['year', 'month'], as_index=False).median()
        df2['filename'] = filename
        dfs.append(df2)
    df = pd.concat(dfs, ignore_index=True)
    df.rename(columns={'Small': 'Small_median', 'Large':
                       'Large_median'}, inplace=True)
    df.to_csv(parent_dir(os.getcwd()) + '/DataBySensor/Dylos/dropdup/round_all/stat_summary/gb/group_by_month_median.csv', index=False)
    return df

def summary_location_home_by_cohort():
    # print 'generate summary of home by month'
    # filelist = glob.glob(dylos_cleaned_path + '*.*')
    # dfs = []
    # for i, f in enumerate(filelist):
    #     filename = f[f.rfind('/') + 1:]
    #     print (i, filename)
    #     df = pd.read_csv(f)
    #     df.set_index(pd.DatetimeIndex(pd.to_datetime(df['Date/Time'])), inplace=True)
    #     df1 = df.resample('M', how = 'mean')
    #     df2 = df.resample('M', how = 'min')
    #     df3 = df.resample('M', how = 'max')
    #     df1.rename(columns=lambda x: x + '_mean', inplace=True)
    #     df1['filename'] = filename
    #     df1['month'] = df1.index.map(lambda x: x.month)
    #     df1['year'] = df1.index.map(lambda x: x.year)
    #     # df1.drop('Date/Time', axis=1, inplace=True)
    #     df1['Small_min'] = df2['Small']
    #     df1['Large_min'] = df2['Large']
    #     df1['Small_max'] = df3['Small']
    #     df1['Large_max'] = df3['Large']
    #     dfs.append(df1)
    # df_all = pd.concat(dfs, ignore_index=False)
    # cols = list(df_all)
    # cols.remove('month')
    # cols.remove('year')
    # cols.insert(0, 'month')
    # cols.insert(0, 'year')
    # df_all = df_all[cols]
    # df_all.to_csv(parent_dir(os.getcwd()) + '/DataBySensor/Dylos/dropdup/round_all/stat_summary/gb/group_by_month.csv', index=False)
    # df_all = pd.read_csv(parent_dir(os.getcwd()) + '/DataBySensor/Dylos/dropdup/round_all/stat_summary/gb/group_by_month.csv')
    # df_all.dropna(subset=['Small_mean'], inplace=True)
    # df_all.drop_duplicates(cols=['year', 'month', 'Small_mean',
    #                              'Large_mean', 'Small_min',
    #                              'Large_min', 'Small_max',
    #                              'Large_max', 'filename'],
    #                        inplace=True)
    # summary_median()
    df_median = pd.read_csv(parent_dir(os.getcwd()) + '/DataBySensor/Dylos/dropdup/round_all/stat_summary/gb/group_by_month_median.csv')
    df_all = pd.merge(df_all, df_median, on=['filename', 'year',
                                             'month'], how='left')
    df_all.to_csv(parent_dir(os.getcwd()) + '/DataBySensor/Dylos/dropdup/round_all/stat_summary/gb/group_by_month_wMedian.csv', index=False)
    df_label = pd.read_csv(parent_dir(os.getcwd()) + '/DataBySensor/Dylos/dropdup/round_all/label_summary/label.csv')
    df = pd.merge(df_all, df_label, on='filename', how='left')
    df.sort(['home_id_standard', 'year', 'month'], inplace=True)
    df.to_csv(parent_dir(os.getcwd()) + '/DataBySensor/Dylos/dropdup/round_all/summary/gb/group_by_month.csv', index=False)
    return
    
# BOOKMARK PROCESS NEW FILES
def chop_wrong_time():
    # print 'chopping wrong time ...'
    df_summary = pd.read_csv(util.get_path('Dylos', 'correct_time_', 'all') +
                             'summary/Small_round_all.csv')
    df_time = pd.read_csv(os.getcwd() + '/input/cohort_time.csv')
    df_time['round'] = df_time['round'].map(lambda x: str(x))
    df_summary2 = pd.merge(df_summary, df_time, on='round', how='left')
    df_summary2 = df_summary2[['filename', 'Raw Start Time', 
                               'cohort start time', 'round']]
    df_summary2['Raw Start Time'] = pd.to_datetime(df_summary2['Raw Start Time'])
    df_summary2['cohort start time'] = pd.to_datetime(df_summary2['cohort start time'])
    df_summary2['need to chop'] = df_summary2.apply(lambda r: r['Raw Start Time'] < r['cohort start time'], axis=1)
    df_summary2.to_csv(util.get_path('Dylos', 'temp', 'all') + 'start_time.csv', index=False)
    r_files = df_summary2[~df_summary2['need to chop']]['filename'].tolist()
    existing = glob.glob(util.get_path('Dylos', 'chop_start_', 'all') + '*')
    existing_filenames = [x[x.rfind('/') + 1:] for x in existing]
    r_files = list(set(r_files).difference(set(existing_filenames)))
    for f in r_files:
        infile = util.get_path('Dylos', 'correct_time_', 'all') + f
        outfile = util.get_path('Dylos', 'chop_start_', 'all') + f
        print 'copy file ' + f
        shutil.copyfile(infile, outfile)
    df_summary2 = df_summary2[df_summary2['need to chop']]
    filenames = df_summary2['filename'].tolist()
    # print filenames
    filenames = list(set(filenames).difference(set(existing_filenames)))
    print 'chop wrong start time for {0} files'.format(len(filenames))
    df_summary2.set_index('filename', inplace=True)
    ori = time.time()
    lines = []
    lines.append('filename,original data count before chop,data count after chop')
    # filenames = [x for x in filenames if 'NCAbed' in x]
    for f in filenames:
        print f
        df = pd.read_csv(util.get_path('Dylos', 'correct_time_', 'all') + f)
        start_date = df_summary2.ix[f, 'cohort start time']
        if start_date != np.nan:
            df['date_iso'] = pd.to_datetime(df['Date/Time'])
            df2 = df[df['date_iso'] > start_date]
            line = '{0},{1},{2}'.format(f, len(df), len(df2))
            print line
            lines.append(line)
            df2.to_csv(util.get_path('Dylos', 'chop_start_', 'all') + f,
                       index=False)
        ori = timing(ori, time.time(), 'chop_time: {0}'.format(f))
    # append logs to the end
    with open (util.get_path('Dylos', 'chop_start_', 'all') + 'log/log.csv', 'a') as wt:
        wt.write('\n'.join(lines))
    return
    
def copyplot2Dropbox():
    indir = util.get_path('Dylos', 'merge_gen_spe', 'all') + 'plot/'
    outdir = '/home/yujiex/Dropbox/plot2'
    shutil.rmtree(outdir)
    shutil.copytree(indir, outdir)
    print 'end'
    return
    
def id_hash():
    df_id = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv')
    ids = df_id['HOME ID CORRECT'].unique()
    length = len(ids)
    random.seed(10)
    hashing = random.sample(range(1000, 9000), length)
    df_hash = pd.DataFrame({'home_id_standard': ids, 'hashing':
                            hashing})
    df_hash.to_csv(os.getcwd() + '/input/id_hashing.csv', index=False)
    print 'write to id_hashing.csv'
    return
    
# create dygraphs for all Outdoors
def combine_dygraph_IOR(loc, cohort=None):
    id_hash()
    files = glob.glob(util.get_path('Dylos', 'concat_gen_spe', 'all') + '*.csv')
    print len(files)
    df_id = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv')
    df_time = pd.read_csv(os.getcwd() + '/input/cohort_time.csv')
    df_time.set_index('round', inplace=True)
    df_hash = pd.read_csv(os.getcwd() + '/input/id_hashing.csv')
    df_hash.set_index('home_id_standard', inplace=True)
    nb_dict = dict(zip(df_id['HOME ID CORRECT'],
                       df_id['NEIGHBORHOOD']))
    if not cohort is None:
        ids = df_id[df_id['ROUND'] == cohort]['HOME ID CORRECT']
        cutoff_start = df_time.ix[cohort, 'cohort start time']
        cutoff_end = df_time.ix[cohort, 'cohort end time']
    else:
        ids = df_id['HOME ID CORRECT']
    print len(ids)
    outfiles = []
    for name in ids:
        print '{0}_{1}'.format(name, loc)
        outfiles += [x for x in files if '{0}_{1}'.format(name, loc)
                     in x]
    print len(outfiles)
    df_id.set_index('HOME ID CORRECT', inplace=True)
    dfs = []
    # outfiles = [x for x in outfiles if 'CAW' in x]
    for f in outfiles:
        df = pd.read_csv(f)
        filename = f[f.rfind('/') + 1:]
        print filename
        name = filename[:filename.find('_')]
        print name
        loc = filename[filename.find('_') + 1: filename.rfind('.')]
        hashing = df_hash.ix[name, 'hashing']
        nb = nb_dict[name]
        df.set_index(pd.DatetimeIndex(pd.to_datetime(df['Date/Time'])), inplace=True)
        print len(df.index)
        if not cutoff_start is None:
            df = df[df.index > pd.to_datetime(cutoff_start)]
        if not cutoff_end is None:
            df = df[df.index < pd.to_datetime(cutoff_end)]
        if len(df) == 0:
            continue
        df_r = df.resample('15T', how='mean')
        newname = '{0}_{1}_{2}'.format(hashing, loc, nb)
        df_r.rename(columns={'Small': newname}, inplace=True)
        dfs.append(df_r[[newname]])
    df = reduce(lambda x, y: pd.merge(x, y, left_index=True, right_index=True, how='outer'), dfs)
    df.to_csv(util.get_path('Dylos', 'across', cohort) + 'small_{0}.csv'.format(cohort))
    with open(util.get_path('Dylos', 'across', 'all') + 'template.html', 'r') as rd:
        lines = rd.readlines()
    length = len(lines)
    for i in range(length):
        lines[i] = lines[i].replace("AE_Small_15T.csv", "small_{0}.csv".format(cohort))
        if not cohort is None:
            lines[i] = lines[i].replace("AE_Small 15 Min Average", "Cohort {0} Small 15 Min Average".format(cohort))
        else:
            lines[i] = lines[i].replace("AE_Small 15 Min Average", "Small 15 Min Average")
    with open (util.get_path('Dylos', 'across', cohort) + 'small.html', 'w+') as wt:
        wt.write(''.join(lines))

# bookmark

def copy2round():
    files = glob.glob(util.get_path('Dylos', 'concat_gen_spe', 'all') + '*.csv')
    loc = 'O'
    df_id = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv')
    ids = df_id[df_id['ROUND'] == 9]['HOME ID CORRECT']
    outfiles = []
    for name in ids:
        outfiles += [x for x in files if '{0}_{1}'.format(name, loc)
                     in x]
    for f in outfiles:
        shutil.copyfile(f, f.replace('round_all', 'round_9'))
    
def compute_round():
    path = util.get_path('Dylos', 'chop_start', 'all') + 'summary/Small_round_all_unique.csv'
    df = pd.read_csv(path)
    df = df[['home_id_standard', 'Raw Start Time', 'Raw End Time']]
    df['Raw Start Time'] = pd.to_datetime(df['Raw Start Time'])
    df['Raw End Time'] = pd.to_datetime(df['Raw End Time'])
    df_ch = pd.read_csv(os.getcwd() + '/input/cohort_time.csv')
    df_ch['cohort start time'] = pd.to_datetime(df_ch['cohort start time'])
    df_ch['cohort end time'] = pd.to_datetime(df_ch['cohort end time'])
    def cohorts(home, start, end, df_):
        df = df_.copy()
        df = df[(df['cohort start time'] < start)]
        df = df[end < (df['cohort end time'])]
        result = df['round'].tolist()
        # print ','.join([home, str(result)])
        return str(result)
    df['cohort'] = df.apply(lambda r: cohorts(r['home_id_standard'],
                                              r['Raw Start Time'],
                                              r['Raw End Time'],
                                              df_ch), axis=1)
    df = df[df['cohort'] != '[]']
    df = df[df['home_id_standard'] != '-']
    df['cohort'] = df['cohort'].map(lambda x: x[1: -1])
    df.drop_duplicates(cols=['home_id_standard', 'cohort'],
                       inplace=True)
    df.to_csv(os.getcwd() + '/input/participant_cohort.csv',
              index=False)
    print 'end'

# routinely run to get the summary
def run_routine():
    # copy2round()
    # -- dylos -- #
    # cleaning_dylos('all')
    # concat_dylos(['general_location_standard'])
    # mergeIRO()

    # added 0725 to create dygraphs
    cohort = 15
    home=None
    # home = 'TRH'
    concat_dylos(['general_location_standard',
                  'specific_location_standard'], cohort=cohort, history=False, home=home)
    merge_loc('concat_gen_spe', cohort=cohort, history=False, home=home)
    copyplot2Dropbox()
    combine_dygraph_IOR('O', cohort=cohort)

    # Create dygraphs for Dylos outdoor for each cohort
    # for i in (range(1, 9)):
    #     combine_dygraph_IOR('O', cohort=i)

    # -- speck -- #
    # kind = 'speck'
    # print 'speck summary start'
    # import download_speck as ds
    # ds.main()
    # summary_speck_stat(util.get_path('Speck', 'raw_data',
    #                             'all_manual_download'), '*.csv')
    # summary_label('/DataBySensor/Speck/raw_data/round_all_manual_download/', '*.[a-z][a-z][a-z]', 'speck')
    # summary_all_general('/DataBySensor/Speck/raw_data/round_all_manual_download/', kind, 'all_manual_download')
    # drop_small_count('all_manual_download')
    # copy2dropbox_speck_bulk('all_manual_download')

    # summary_label('/DataBySensor/Speck/raw_data/round_all_bulkdownload/', '*.[a-z][a-z][a-z]', 'speck')
    # summary_speck_stat(util.get_path('Speck', 'raw_data', 'all_bulkdownload'), '*.csv')
    # summary_all_general('/DataBySensor/Speck/raw_data/round_all_bulkdownload/', kind, 'all_bulkdownload')
    # drop_small_count('all_bulkdownload')
    # copy2dropbox_speck_bulk('all_bulkdownload')

    # import join as j
    # j.main()
    return

def flag_wrong_time():
    summary_files = glob.glob(parent_dir(os.getcwd()) +
                              '/DataBySensor/Dylos/reform_/round_all/'
                              + 'summary/*.csv')
    for f in summary_files:
        df = pd.read_csv(f)
        df['year_start'] = df['Raw Start Time'].map(lambda x: int(x[:4]))
        df['year_end'] = df['Raw End Time'].map(lambda x: int(x[:4]))
        df['wrong timestamp_start'] = df['year_start'].map(lambda x: not right_year(x))
        df['wrong timestamp_end'] = df['year_end'].map(lambda x: not right_year(x))
        df['wrong timestamp'] = df.apply(lambda r: r['wrong timestamp_start'] or r['wrong timestamp_end'], axis=1)
        df.to_csv(f, index=False)
    return 
    
# BOOKMARK
def time_range(end_time, length):
    result = pd.date_range(end=end_time, periods=length, freq='T')
    output = result.format(formatter=lambda x: x.strftime('%m/%d/%y %H:%M'))
    return output

def calculate_time_range(first_right_idx, stamps):
    first_right_stamp = stamps[first_right_idx]
    len_head = first_right_idx + 1
    result = pd.date_range(end=first_right_stamp, periods=len_head,
                           freq='T')
    output = result.format(formatter=lambda x: x.strftime('%m/%d/%y %H:%M'))
    return output[:-1]

def right_year(year):
    return int(year) > start_year and int(year) < end_year

def manual_adjust_time(f, amount, direction):
    df = pd.read_csv(f)
    df['date_iso'] = pd.to_datetime(df['Date/Time'])
    print df.head()
    if direction == 'forward':
        df['date_iso'] = df['date_iso'].map(lambda x: x + pd.DateOffset(hours=amount))
    else:
        df['date_iso'] = df['date_iso'].map(lambda x: x + pd.DateOffset(hours=-1 * amount))
    new_stamps = df['date_iso'].map(lambda x: x.strftime('%m/%d/%y %H:%M'))
    df.drop('date_iso', axis=1, inplace=True)
    df['Date/Time'] = new_stamps
    return df

def correct_time():
    flag_wrong_time()
    df_summary = pd.read_csv(parent_dir(os.getcwd()) + \
                             '/DataBySensor/Dylos/reform_/round_all/'
                             + 'summary/Small_round_all.csv')
    filenames = df_summary['filename'].tolist()
    existing = glob.glob(util.get_path('Dylos', 'correct_time_', 'all') + '*')
    existing_filenames = [x[x.rfind('/') + 1:] for x in existing]
    filenames = list(set(filenames).difference(set(existing_filenames)))
    print 'correct time for {0} files ...'.format(len(filenames))
    data_dir = util.get_path('Dylos', 'reform_', 'all')
    df_w = df_summary[df_summary['wrong timestamp']]
    df_r = df_summary[~df_summary['wrong timestamp']] # right
    r_file_names = df_r['filename'].tolist()
    r_file_names = \
        list(set(r_file_names).difference(set(existing_filenames)))
    r_files = [data_dir + x for x in r_file_names]
    print 'copy {0} files ...'.format(len(r_files))
    for f in r_files:
        print f
        outfile = f.replace('reform_', 'correct_time_')
        print 'copy file ' + outfile[outfile.rfind('/') + 1:]
        shutil.copyfile(f, outfile)
    # manual adjust time
    infile = data_dir + 'LIP_I_D117_05-20-2016_upstairs-TIME STAMP OFF BY 90 MINUTES.txt'
    manual_adjust_time(infile, 1.5, 'forward').to_csv(infile.replace('reform_', 'correct_time_'), index=False)
    infile = data_dir + 'KD_I_D049_06-05-2016_Basement_Time off by 4 hours see note.txt'
    manual_adjust_time(infile, 1.5, 'forward').to_csv(infile.replace('reform_', 'correct_time_'), index=False)

    w_file_names = df_w['filename'].tolist()
    w_file_names = \
        list(set(w_file_names).difference(set(existing_filenames)))
    r_files = [data_dir + x for x in r_file_names]
    w_files = [data_dir + x for x in w_file_names]
    print 'copy {0} files ...'.format(len(w_files))
    df_summary.set_index('filename', inplace=True)
    df_summary['Raw End Time'] = \
        pd.to_datetime(df_summary['Raw End Time'])
    date_pattern = re.compile(r'[0-9]{1,2}-[0-9]{1,2}-[0-9]{2,4}')
    lines = []
    lines.append(','.join(['filename', 'time in filename', 'err_type',
                          'first_right_timestamp', 'new_time',
                          'source']))
    for i, x in enumerate(w_files):
        df = pd.read_csv(x)
        # special case, seems to be manual modification
        filename = x[x.rfind('/') + 1:]
        if filename == "D026_MMM_1stFl_10_03_2015.txt":
            df.drop(df.index[[0]], inplace=True)
        outfile = x.replace('reform_', 'correct_time_')
        stamps = df['Date/Time'].tolist()
        years = [int('20'+x[6:8]) for x in stamps]
        # print i, filename 
        try:
            first_right_year = next(x for x in years if right_year(x))
            first_right_idx = years.index(first_right_year)
            lines.append(','.join([filename, '', 'partially wrong',
                                   stamps[first_right_idx], '', '']))
            first_half = calculate_time_range(first_right_idx, stamps)
            second_half = stamps[first_right_idx:]
            new_stamps = first_half + second_half
            df_new = pd.DataFrame({'Date/Time': new_stamps, 'Small': df['Small'], 'Large': df['Large']})
            print '{0}: find right stamp within file'.format(filename)
            df_new.to_csv(outfile, index=False)
            continue
        except StopIteration:
            home_id = df_summary.ix[filename, 'home_id_standard']
            loc = df_summary.ix[filename, 'general_location_standard']
            time = re.search(date_pattern, filename)
            if time != None:
                time_str = time.group()
                df_mightbe = df_summary.copy()
                df_mightbe = df_summary[['home_id_standard',
                                         'general_location_standard',
                                         'Raw End Time']]
                df_mightbe = df_mightbe[df_mightbe['home_id_standard']
                                        == home_id]
                df_mightbe = \
                    df_mightbe[df_mightbe['general_location_standard']
                        != home_id]
                df_mightbe['target_time'] = time_str
                df_mightbe['target_time'] = \
                    pd.to_datetime(df_mightbe['target_time'])
                df_mightbe['timediff'] = abs(df_mightbe['Raw End' +
                    ' Time'] - df_mightbe['target_time'])
                min_timediff = df_mightbe['timediff'].min()[0]
                if min_timediff < np.timedelta64(1, 'D'):
                    df_right = df_mightbe[df_mightbe['timediff'] ==
                                          min_timediff]
                    sourcefile = df_right.index.tolist()[0]
                    end_time = df_right['Raw End Time'].tolist()[0]
                else:
                    end_time = '{1}/20{0} 12:00'.format(time_str[-2:], time_str[:time_str.rfind('-')].replace('-', '/'))
                    sourcefile = 'self'
                print '{0}: guessed end time {1} from {2}'.format(filename, end_time, sourcefile)
                # print 'no right stamp', home_id, time_str, end_time
                new_stamps = time_range(end_time, len(df))
                if type(end_time) == str:
                    end_time_str = end_time
                else:
                    end_time_str = end_time.strftime('%m/%d/%y %H:%M')
                lines.append(','.join([filename, time_str, 'all wrong',
                                       '', end_time_str, sourcefile]))
                df_new = pd.DataFrame({'Date/Time': new_stamps, 'Small': df['Small'], 'Large': df['Large']})
                df_new.to_csv(outfile, index=False)
            else:
                print 'no time string for {0}'.format(filename)
    with open(parent_dir(os.getcwd()) + \
              '/DataBySensor/Dylos/correct_time_/round_all/'
              + 'log/correct_timestamp.csv', 'a') as wt:
        wt.write('\n'.join(lines))
    summary_stat('Dylos', 'correct_time_', 'all', '*.[a-z][a-z][a-z]')
    summary_label('/DataBySensor/Dylos/correct_time_/round_all/',
                  '*.[a-z][a-z][a-z]', 'dylos')
    summary_all_general('/DataBySensor/Dylos/correct_time_/round_all/',
                        'dylos', 'all')
    remove_dup_files('correct_time_', 'all')
    return
    
from scipy import signal
def peak_find(home):
    import peakutils.peak as pp
    df = pd.read_csv(util.get_path('Dylos', 'mergeIRO', 'all') + 'plot/{0}/{0}_Small_15T.csv'.format(home))
    cols = list(df)
    measures = cols[1:]
    print measures 
    peaks = []
    for c in measures:
        print c
        data = np.array(df[c].tolist())
        indexes = pp.indexes(data, thres=7.0/max(data), min_dist=2)
        print('Peaks are: %s' % (indexes))
    #     if c == 'I':
    #         step = np.arange(1, 10)
    #     else:
    #         step = np.arange(1, 100)
    #     p = signal.find_peaks_cwt(data, step)
    #     max_value = max([x for x in data if not np.isnan(x)])
    #     if c == 'I':
    #         max_value = 1500
    #     # print max_value
    #     df[c + '_peak'] = df.index.map(lambda x: max_value if x in p else np.nan)
    # df.to_csv(util.get_path('Dylos', 'mergeIRO', 'all') + 'plot/{0}/{0}_Small_15T_p.csv'.format(home), index=False)
    # return
    
def copyfiles(home_id, outdir):
    df = pd.read_csv(util.get_path('Dylos', 'correct_time', 'all') +
                     'summary/Small_round_all_unique.csv')
    df = df[df['home_id_standard'] == home_id]
    filenames = df['filename'].tolist()
    for f in filenames:
        f = f.replace('_dylos', '')
        f = f.replace('_putty', '')
        f = f.replace('_unknown', '')
        infile = util.get_path('Dylos', 'raw_data', 'all') + f
        outfile = outdir + f
        shutil.copyfile(infile, outfile)

def copy2dropbox_speck_bulk(dirname):
    files = glob.glob(util.get_path('Speck', 'raw_data', dirname) + '*.csv')
    for x in files:
        filename = x[x.rfind('/') + 1:]
        outfile = '/home/yujiex/Dropbox/Speck/round_{1}/{0}'.format(filename, dirname)
        shutil.copyfile(x, outfile)
    summaries = glob.glob(util.get_path('Speck', 'raw_data', dirname) + 'summary/*.csv')
    for f in summaries:
        print 'copy summary: {0}'.format(f[f.rfind('/') + 1:])
        outfile = f.replace('/media/yujiex/work/ROCIS/DataBySensor/Speck/raw_data/', '/home/yujiex/Dropbox/Speck/')
        shutil.copyfile(f, outfile)
    return

def copy2dropbox():
    files = glob.glob(util.get_path('Dylos', 'chop_start_', 'all') + '*.[a-z][a-z][a-z]')
    existing = glob.glob('/home/yujiex/Dropbox/Dylos/chop_start_/round_all/*.[a-z][a-z][a-z]')
    filenames = [x[x.rfind('/') + 1:] for x in files]
    existing_filenames = [x[x.rfind('/') + 1:] for x in existing]
    new_filenames = list(set(filenames).difference(set(existing_filenames)))
    print 'copy {0} files to Dropbox'.format(len(new_filenames))
    for f in new_filenames:
        print 'copy {0}'.format(f)
        infile = util.get_path('Dylos', 'chop_start_', 'all') + f
        outfile = '/home/yujiex/Dropbox/Dylos/chop_start_/round_all/{0}'.format(f)
        shutil.copyfile(infile, outfile)
    summaries = glob.glob(util.get_path('Dylos', 'chop_start_', 'all') + 'summary/*.csv')
    for f in summaries:
        print 'copy summary: {0}'.format(f[f.rfind('/') + 1:])
        outfile = f.replace('/media/yujiex/work/ROCIS/ROCIS/DataBySensor/', '/home/yujiex/Dropbox/')
        shutil.copyfile(f, outfile)
        
# count number of files received per home
def file_counts_per_home():
    df = pd.read_csv(util.get_path('Dylos', 'chop_start', 'all') + 'summary/Small_round_all_unique.csv')
    df = df[['home_id_standard']]
    df_cnt = df.groupby('home_id_standard').count()
    df_cnt.rename(columns={'home_id_standard': 'count'}, inplace=True)
    exist = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv')[['HOME ID CORRECT', 'ROUND']]
    exist.drop_duplicates(cols=['HOME ID CORRECT'], inplace=True, take_last=False)
    exist.rename(columns={'HOME ID CORRECT': 'home_id_standard'},
                 inplace=True)
    df_all = pd.merge(exist, df_cnt, left_on='home_id_standard', right_index=True, how='left')
    df_all.fillna(0, inplace=True)
    df_all.sort(['ROUND', 'count'], ascending=False, inplace=True)
    print df_all.head()
    df_all.to_csv(util.get_path('Dylos', 'chop_start', 'all') + 'summary/dylos_file_count.csv', index=False)
    
def zero_count():
    df_summary = pd.read_csv(util.get_path('Dylos', 'chop_start', 'all') + \
                     'summary/Small_round_all_unique.csv')
    filenames = df_summary['filename'].tolist()
    lines = ['filename,column,total_count,zero_count']
    for f in filenames:
        df = pd.read_csv(util.get_path('Dylos', 'chop_start', 'all') + f)
        for x in ['Small', 'Large']:
            lines.append(','.join(map(str, [f,x,len(df[x]),
                         len(df[df[x] == 0])])))
    with open (util.get_path('Dylos', 'chop_start', 'all') + \
               'summary/zero_count.csv', 'w+') as wt:
        wt.write('\n'.join(lines))
    print 'end'
    return
    
def join_zero_count():
    df_summary = pd.read_csv(util.get_path('Dylos', 'chop_start', 'all') + \
                             'summary/Small_round_all_unique.csv')
    df_summary = df_summary[['filename', 'home_id_standard',
                             'general_location_standard',
                             'specific_location_standard',
                             'equip_id_standard', 'Raw Start Time',
                             'Raw End Time']]
    df_zero = pd.read_csv(util.get_path('Dylos', 'chop_start', 'all') + \
                          'summary/zero_count.csv')
    df_all = pd.merge(df_zero, df_summary, on='filename', how='left')
    df_all.sort(['column', 'home_id_standard'], inplace=True)
    print df_all.to_csv(util.get_path('Dylos', 'chop_start', 'all') + \
                        'summary/zero_count_winfo.csv', index=False)
    print 'end'
    
def read_files(files, cutoff_start=None, cutoff_end=None):
    dfs = []
    for f in files:
        df = pd.read_csv(f)
        filename = f[f.rfind('/') + 1:]
        name = filename[:filename.find('.')]
        print name
        df.set_index(pd.DatetimeIndex(pd.to_datetime(df['Date/Time'])), inplace=True)
        if not cutoff_start is None:
            df = df[df.index > pd.to_datetime(cutoff_start)]
        if not cutoff_end is None:
            df = df[df.index < pd.to_datetime(cutoff_end)]
        if len(df) == 0:
            continue
        df_r = df.resample('15T', how='mean')
        df_r.rename(columns={'Small': name}, inplace=True)
        dfs.append(df_r[[name]])
    df = reduce(lambda x, y: pd.merge(x, y, left_index=True, right_index=True, how='outer'), dfs)
    return df
    
def substitute_template(csvpath, folder=None):
    with open(os.getcwd() + '/input/template.html', 'r') as rd:
        lines = rd.readlines()
    length = len(lines)
    for i in range(length):
        lines[i] = lines[i].replace('src="dygraph-combined-dev.js"', 'src="//cdnjs.cloudflare.com/ajax/libs/dygraph/1.1.1/dygraph-combined.js"')
        lines[i] = lines[i].replace("AE_Small_15T.csv", "{0}.csv".format(csvpath))
        lines[i] = lines[i].replace("AE_Small 15 Min Average", "Small 15 Min Average")
    if not (folder is None):
        outfile = util.get_path('Dylos', 'to_calibrate/{0}'.format(folder)) + \
            'plot/{0}.html'.format(csvpath)
    else:
        outfile = util.get_path('Dylos', 'to_calibrate') + 'plot/{0}.html'.format(csvpath)
    with open (outfile, 'w+') as wt:
        wt.write(''.join(lines))

def dygraph_calibrate():    
    # files = glob.glob(util.get_path('Dylos', 'to_calibrate') + '*.log')
    # for f in files:
    #     cd.cleaning(f, f.replace('to_calibrate/',
    #                              'to_calibrate/clean/'))
    cutoff_start = pd.to_datetime('2016-07-26 12:00:00')
    files = glob.glob(util.get_path('Dylos', 'to_calibrate/clean/') + '*.log')
    group1 = [x for x in files if not 'LW' in x]
    group2 = [x for x in files if 'LW' in x]
    df = read_files(group1, cutoff_start=cutoff_start)
    df.to_csv(util.get_path('Dylos', 'to_calibrate') + 'plot/small_Calib.csv')
    substitute_template('small_Calib')   
    df = read_files(group2, cutoff_start=cutoff_start)
    df.to_csv(util.get_path('Dylos', 'to_calibrate') + 'plot/small_LWCalib.csv')
    substitute_template('small_LWCalib')

# general version of calibrate
def dygraph_calibrate_gen(folder, start=None, end=None):
    files = glob.glob(util.get_path('Dylos', 'to_calibrate/{0}'.format(folder)) + '*')
    files = [f for f in files if not ('/clean' in f or '/plot' in f)]
    print len(files)
    for f in files:
        cd.cleaning(f, f.replace('to_calibrate/{0}/'.format(folder),
                                 'to_calibrate/{0}/clean/'.format(folder)))
    cutoff_start = pd.to_datetime(start)
    cutoff_end = pd.to_datetime(end)
    files = glob.glob(util.get_path('Dylos', 'to_calibrate/{0}/clean/'.format(folder)) + '*')
    df = read_files(files, cutoff_start=cutoff_start, cutoff_end=cutoff_end)
    df.to_csv(util.get_path('Dylos', 'to_calibrate/{0}'.format(folder)) + 'plot/small_{0}_calib.csv'.format(folder))
    substitute_template('small_{0}_calib'.format(folder), folder=folder)

def main():
    # dygraph_calibrate_gen('1116', start='2016-11-15 00:00:00', end='2016-11-20 00:00:00')
    # dygraph_calibrate_gen('1109', start='2016-11-06 12:00:00', end='2016-11-08 00:00:00')
    # dygraph_calibrate_gen('1002', start='2016-09-30 00:00:00', end='2016-10-03 00:00:00')
    # dygraph_calibrate_gen('0918', start='2016-09-04 00:00:00', end='2016-09-07 00:00:00')
    run_routine()
    # compute_round()
    # dygraph_calibrate()
    # Create participant - harshing (anonymus ID)
    # id_hash()

    # peak_find('SPR')
    # ## ## ## ## ## ## ## ## ## ## ## #
    # Dylos cleaning_dylos and summary #
    # ## ## ## ## ## ## ## ## ## ## ## #
    # folder = util.get_path('Speck', 'raw_data', 'all_bulkdownload')
    # resample_speck(folder, '15T')
    # folder = util.get_path('Speck', 'raw_data', 'all_manual_download')
    # resample_speck(folder, '15T')
    # copyfiles('SLK', '/media/yujiex/work/ROCIS/DataBySensor/tempcopy/')
    # zero_count()
    # join_zero_count()
    # kind = 'dylos'
    # cohort = 'all'
    # file_counts_per_home()

    # additional dylos summary
    # summary_by_home()
    # location_summary_dylos('I')
    # location_summary_dylos('R')
    # location_summary_dylos('O')
    # summary_location_home_by_cohort()
    # TODO: outdoor and roamer summary

    # ## ## ## ## ## ## ## ## ## ## ## #
    # Speck cleaning and summary #
    # ## ## ## ## ## ## ## ## ## ## ## #

    # kind = 'speck'
    # print 'speck summary start'
    # summary_speck_stat(util.get_path('Speck', 'raw_data', 'all_bulkdownload'), '*.csv')
    # summary_label('/DataBySensor/Speck/raw_data/round_all_bulkdownload/', '*.[a-z][a-z][a-z]', 'speck')
    # summary_speck_stat(util.get_path('Speck', 'raw_data',
    #                             'all_manual_download'), '*.csv')
    # summary_all_general('/DataBySensor/Speck/round_all_bulkdownload/', kind)
    # # join lat long info to summary
    # join_latlong()
    # drop_small_count()
    # print 'speck summary end'
    # join_static_rounddate(kind, 'all_bulkdownload')
    return

main()

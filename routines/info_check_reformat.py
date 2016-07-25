import pandas as pd
import os
import glob
import numpy as np
import re

def parent_dir(dirname):
    return dirname[:dirname.find('/routines')]

dylos_summary_path = parent_dir(os.getcwd()) + '/DataBySensor/Dylos/dropdup/round_all/summary/'
speck_summary_path = parent_dir(os.getcwd()) + '/DataBySensor/Speck/round_all_bulkdownload/summary/'

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
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    print filelist[0]
    files = []
    logger_types = []
    is_processed = []
    putty_starts = []
    dylos_starts = []
    outfile_sep = (parent_dir(os.getcwd()) + folder.replace('RawData Unsorted', 'separate') + 'separate_summary/separate.csv')
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        files.append(filename)
        if ('.xlsx' in filename) or ('.xls' in filename):
            is_processed.append(False)
            logger_types.append('-')
            putty_starts.append('-')
            dylos_starts.append('-')
            continue
        print filename
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

        outfile = f.replace('RawData Unsorted', 'separate')
        if not '.' in outfile:
            outfile += '.txt'
        idx_dot = outfile.rfind('.')
        outfile_dylos = outfile[:idx_dot] + '_dylos' + outfile[idx_dot:]
        outfile_putty = outfile[:idx_dot] + '_putty' + outfile[idx_dot:]
        outfile_unknown = outfile[:idx_dot] + '_unknown' + outfile[idx_dot:]
        print outfile_dylos
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

    print (len(files), len(logger_types), len(is_processed), len(putty_starts),
           len(dylos_starts))
    df = pd.DataFrame({'filename': files,
                       'logger_type': logger_types,
                       'putty_start_lines': putty_starts,
                       'dylos_start_lines': dylos_starts,
                       'processed': is_processed})
    df.to_csv(outfile_sep, index=False)

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
        #print filename
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
            print filename

        if len(datalines) != 0:
            with open (outfile, 'w+') as wt:
                datalines = ['Date/Time,Small,Large\r\n'] + datalines
                wt.write(''.join(datalines))
            if len(ill_format_linenum) != 0:
                with open (outfile_err, 'w+') as wt:
                    err_lines = [lines[i] for i in ill_format_linenum]
                    '''
                    for x in err_lines:
                        print x
                    '''
                    wt.write(''.join(err_lines))
    print (len(filelist), len(logger_types), len(is_processed), len(ill_format_lines))
    print 'empty file', emptyfile_counter
    print len(filelist)
    df = pd.DataFrame({'filename': files,
                       'unit': units,
                       'multiplier': multipliers,
                       'logger_type': logger_types,
                       'lines taken out': ill_format_lines})
    df.to_csv(outfile_reform, index=False)

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
    spe_dict = {'JAZ': {'D227': 'PORCH', 'D226': 'DINING', 'D225':
                        'OFFICE'}, 
                'CJH': {'D331': 'DINING', 'D332': '-', 
                        'D333': 'GROUND FLOOR'}, 
                'DHP': {'D431': 'LIVING', 'D432': '-', 
                        'D433': 'BASEMENT'},
                'EK':  {'D219': 'LIVING', 'D220': '-', 'D221':
                        '-'}} 
    if field == 'LIVINGROOM':
        return 'LIVING'
    elif field == 'K':
        return 'KITCHEN'
    elif field == 'IRFRM':
        return 'FRONT ROOM'
    elif field == 'CONF':
        return 'CONFERENCE'
    elif home_id_standard in spe_dict:
        if equip_id in spe_dict[home_id_standard]:
            return spe_dict[home_id_standard][equip_id]
    else:
        return field

def standardize_gen_location(field, specific, indoor_list,
                             outdoor_list, home_id_standard, equip_id):
    gen_dict = {'JAZ': {'D225': 'R', 'D226': 'I', 'D227': 'O'},
                'CJH': {'D331': 'I', 'D332': 'O', 'D333': 'R'},
                'DHP': {'D431': 'I', 'D432': 'O', 'D433': 'I'},
                'EK': {'D219': 'I', 'D220': 'O', 'D221': 'R'},
                'RM': {'D116': 'I', 'D117': 'I', 'D118': 'I'},
                'AE': {'D104': 'R', 'D105': 'I', 'D106': 'O'}}

    if specific in outdoor_list:
        return 'O'
    elif specific in indoor_list:
        return 'I'
    else:
        # specific loc empty
        field = field[0]
        if field == 'F':
            return 'R'
        if field == '0':
            return 'O'
        elif home_id_standard in gen_dict:
            if equip_id in gen_dict[home_id_standard]:
                return gen_dict[home_id_standard][equip_id]
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
    filename = filename.upper()
    filename = filename.replace('ROCIS', '')
    filename = filename.replace('LOG', '')
    filename = filename.replace('DYLOSLOG00', 'DYLOSLOG_D00')
    filename = filename.replace('DDYLOS', '')
    filename = filename.replace('DYLOS', '')
    filename = filename.replace('FLOATER', 'ROAMER')
    filename = filename.replace('R MAJOR', 'RM')
    filename = filename.replace('MDIXON', 'MD')
    filename = filename.replace('WEEK', '_')
    filename = filename.replace('_L_', '_LIVING_')
    filename = filename.replace('_LIVROOM_', '_LIVING_')
    filename = filename.replace('ROVING', '_R_')
    filename = filename.replace('NB_D3-3', 'NB_D33')
    filename = filename.replace('-', '_')
    filename = filename.replace(' ', '_')
    filename = filename.replace('(', '_')
    filename = filename.replace(')', '_')
    filename = filename.replace('KIT_', '')
    filename = filename.replace('_TO_', '_')
    filename = filename.replace('EK11.6.15', '_EK')
    filename = filename.replace('DC02', 'JAZ_D02')
    filename = filename.replace('BS_D0', 'WPS_D0')
    filename = filename.replace('MCN2_', 'MCN_')
    filename = filename.replace('BCD_FLOATER 39', 'BCD_R_D439')
    filename = filename.replace('BCD_43', 'BCD_D43')
    filename = filename.replace('IDINING', 'I_DINING')
    filename = filename.replace('ROAMKITCHEN', '_R_KITCHEN')
    filename = filename.replace('D01911.20.15', 'D019_11.20.15')
    filename = filename.replace('LAN_011_', 'LAN_D011_')
    filename = filename.replace('LMC_D119_O_02-02-2016',
                                'MLC_D119_O_02-02-2016')
    filename = filename.replace('MLC-D3-119', 'MLC-D3-D119')
    filename = filename.replace('SPR_030_', 'SPR_D030_')
    filename = filename.replace('KEJ_O_D05', 'KEJ_O_D505')
    filename = filename.replace('NB_D3-3', 'NB_D33')
    filename = filename.replace('R4D0', 'D0')
    filename = filename.replace('LIP_O_Porch_106_12-19-15_BHW_ PORCH',
                                'BHW_O_Porch_106_12-19-15_BHW_ PORCH')
    filename = filename.replace('AJS_C11', 'AJS_D41')

    field_list = filename.split('_')

    if kind == 'dylos':
        prog_equip = re.compile('D[0-9]{3,4}')
    elif kind == 'speck':
        prog_equip = re.compile('S[0-9]{3,4}')
    prog_ID = re.compile('^[A-Z]{2,3}$')
    general_loc = ['inside', 'in', 'indoors', 'indoor', 'I',
                   'IDining', 'outside', 'out', 'outdoors', 'outdoor',
                   'O', '0', 'OPorch', 'roamer', 'roam', 'rover', 'R',
                   'F']
    general_loc_upper = [x.upper() for x in general_loc]
    specific_loc = ['garage', 'office', 'livingroom', 'kitchen',
                    'kirchen', 'bedroom','1stfloor', '1stfl', '1st',
                    '2ndfl', '2ndfloor', 'porch', 'living',
                    'basement', 'Dfab', 'library', 'IRFRM', 'CONF',
                    'conference', 'K', 'dining', 'front room']
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
        if x in general_loc_upper:
            d['general_location'] = x
            continue
        elif x in specific_loc_upper:
            d['specific_location'] = x
            continue
        elif re.search(prog_equip, x):
            d['equip_id'] = re.search(prog_equip, x).group()
            continue
        elif x in initial_list:
            d['home_id'] = home_id_dict[x]
            continue
        elif 'R Major' in x:
            d['home_id'] = 'RM'
            continue
        elif (re.match(prog_ID, x) and d['home_id'] == '-'):
            d['home_id_guess'] = x
            continue
    if d['home_id'] in round_dict:
        d['round'] = round_dict[d['home_id']]
    else:
        d['round'] = '-'
    #print '{0}: {1}'.format(d['home_id'], d['round'])
    if d['filename'] in mis_equip_id_dict:
        d['equip_id_standard'] = mis_equip_id_dict[d['filename']]
    else:
        d['equip_id_standard'] = standardize_equipid(d['equip_id'], d['round'], kind)
    d['home_id_standard'] = d['home_id']
    if d['home_id_standard'] == '-':
        d['home_id_standard'] = d['home_id_guess']

    id_correction_lookup = {'FS': 'BC', 'LMW': 'LIP', 'LMC': 'MLC',
                            'BS': 'WPS'}
    for key in id_correction_lookup:
        d['home_id_standard'] = \
                d['home_id_standard'].replace(key, id_correction_lookup[key])

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
    m_dict = dict(zip(df_unitlookup['filename'], df_unitlookup['multiplier']))

    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    categories = list(pd.read_csv(filelist[0]))
    categories.remove('Date/Time')
    print categories
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        print filename
        if filename in m_dict:
            m = m_dict[filename]
        else:
            m = 1
        df = pd.read_csv(f)
        for cate in categories:
            df[cate] = df[cate].map(lambda x: int(round(x * m, 0)))
        outfile = f.replace('reformat', 'convert_unit')
        df.to_csv(outfile, index=False)

def summary_stat_dylos(folder, suffix):
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    print filelist[0]
    categories = list(pd.read_csv(filelist[0]))
    categories.remove('Date/Time')
    print categories
    suf = folder[folder[:-1].rfind('/') + 1:][:-1]

    df_dict = dict(zip(categories, [[] for i in range(len(categories))]))
    print df_dict
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        print filename
        df = pd.read_csv(f, parse_dates = ['Date/Time'])
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
            print (cate, len(df_dict[cate]))
            #print df_dict

    for cate in categories:
        df_stat = pd.concat(df_dict[cate], ignore_index = True)
        df_stat.to_csv(parent_dir(os.getcwd()) + \
                  folder + 'stat_summary/{0}_{1}.csv'.format(cate, suf),
                  index=False)

# kind: 'dylos', 'speck'
def summary_label(folder, suffix, kind):
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    dfs_label = []
    df_lookup = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_02-23-2016.csv')
    initial_list = list(set(df_lookup['INITIALS'].tolist()))
    initial_list = [x.replace(' ', '') for x in initial_list]
    home_id_dict = dict(zip(df_lookup['INITIALS'],
                            df_lookup['HOME ID CORRECT']))
    round_dict = dict(zip(df_lookup['HOME ID CORRECT'], df_lookup['ROUND']))
    nb_dict = dict(zip(df_lookup['HOME ID CORRECT'],
                        df_lookup['NEIGHBORHOOD']))
    del round_dict[np.nan]
    for key in round_dict:
        round_dict[key] = int(round_dict[key])

    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        df_lb = pd.DataFrame(parse_filename(filename, initial_list,
                                            home_id_dict, round_dict,
                                            nb_dict, kind),
                             index=[0])
        dfs_label.append(df_lb)

    df_label = pd.concat(dfs_label, ignore_index = True)
    df_label.drop(['specific_location', 'specific_location_standard'],
                  axis=1, inplace=True)
    cols = list(df_label)
    cols.remove('filename')
    cols.insert(0, 'filename')
    df_label = df_label[cols]
    print df_label.head()

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

def summary_all_general(folder, kind):
    dirname = (parent_dir(os.getcwd()) + folder)
    df_label = pd.read_csv(dirname + 'label_summary/label.csv')
    files_stat = glob.glob(dirname + 'stat_summary/*.csv')
    for f in files_stat:
        filename = f[f.rfind('/') + 1:]
        datecols = ['Raw Start Time', 'Raw End Time']
        df_stat = pd.read_csv(f, parse_dates=datecols)
        df_1 = pd.merge(df_stat, df_label, on='filename', how='outer')
        if kind == 'dylos':
            df_1['filename_standard'] = df_1.apply(lambda row: \
                    '{0}_{1}_{2}_{3}'.format(row['home_id_standard'],
                                            row['general_location_standard'],
                                            row['equip_id_standard'],
                                            (row['Raw End Time']).strftime('%m-%d-%y')), axis=1)
        else:
            df_1['filename_standard'] = df_1.apply(lambda row: \
                    '{0}_{1}_{2}_{3}'.format(row['home_id_standard'],
                                            row['general_location_standard'],
                                            row['equip_id_standard'],
                                            row['neighborhood']), axis=1)

        df_1.to_csv(dirname + 'summary/{0}'.format(filename), index=False)

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
        print f
        df = pd.read_csv(f)
        df.drop_duplicates(cols='Date/Time', take_last=True, inplace=True)
        outfile = f.replace('convert_unit', 'dropdup')
        print outfile
        print
        df.to_csv(outfile, index=False)

# print Speck specifications
def summary_speck_stat(folder, suffix):
    #pd.options.display.show_dimensions = False
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    categories = list(pd.read_csv(filelist[0]))
    categories.remove('EpochTime')
    categories = [col[col.rfind('.') + 1:] for col in categories]
    print categories
    for cate in categories:
        dfs = []
        for f in filelist:
            filename = f[(f.find(folder) + len(folder)):]
            df = pd.read_csv(f)
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
        outfile = '{0}stat_summary/{1}_{2}.csv'.format(parent_dir(os.getcwd()) + folder, cate, suf)
        pd.concat(dfs, ignore_index = True).to_csv(outfile,index=False) 
def cleaning(x):
    # separate('/RawData Unsorted/Dylos/round_{0}/'.format(x), '*')

    # reformat('/separate/Dylos/round_{0}/'.format(x), '*_dylos.[a-z][a-z][a-z]',
    #          'dylos')
    # reformat('/separate/Dylos/round_{0}/'.format(x), '*_putty.[a-z][a-z][a-z]',
    #          'putty')
    # reformat('/separate/Dylos/round_{0}/'.format(x), '*_unknown.[a-z][a-z][a-z]', 'putty')
    # convert_unit('/reformat/Dylos/round_{0}/'.format(x), '*.[a-z][a-z][a-z]')
    # dropdup('/convert_unit/Dylos/round_{0}/'.format(x), '*.[a-z][a-z][a-z]')
    summary_stat_dylos('/dropdup/Dylos/round_{0}/'.format(x), '*.[a-z][a-z][a-z]')
    # summary_label('/dropdup/Dylos/round_{0}/'.format(x), '*.[a-z][a-z][a-z]', 'dylos')
    # summary_all_general('/dropdup/Dylos/round_{0}/'.format(x), 'dylos')
    return

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
    summary_label('/dropdup/Dylos/round_{0}/'.format(x), '*.[a-z][a-z][a-z]', kind)

def summary_all(x):
    summary_all_general('/dropdup/Dylos/round_{0}/'.format(x), 'dylos')

def excel2csv(folder, suffix, nskip, header, datasheets):
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        print filename
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

def reform_unitconvert_excel(folder, suffix):
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        outfile = f.replace('/csv/', '/csv_unit_convert/')
        print filename
        print outfile
        df_check = pd.read_csv(f)
        df_check.info()
        if len(list(df_check)) == 3:
            df = pd.read_csv(f)
        else:
            df = pd.read_csv(f, parse_dates = [[0, 1]])
        col_list = list(df)
        print col_list
        df.rename(columns={col_list[0]: 'Date/Time',
                           col_list[1]: 'Small',
                           col_list[2]: 'Large'}, inplace=True)
        for col in ['Small', 'Large']:
            df[col] = df[col] * 1/100
        df.to_csv(outfile, index=False)

def parse_excel():
    kind = 'dylos'
    #excel2csv('/RawData Unsorted/Dylos/round_excel/PIZ/', '*.xlsx', 7, 7, None)
    #reform_unitconvert_excel('/RawData Unsorted/Dylos/round_excel/PIZ/csv/', '*.csv')
    #excel2csv('/RawData Unsorted/Dylos/round_excel/JAZ/', '*.xlsx', 0, 0,
    #          ['Dylos - week 1', 'Dylos - week 2', 'Dylos-week3'])
    #sep_csv('/RawData Unsorted/Dylos/round_excel/JAZ/csv/', '*.csv')
    reform_unitconvert_excel('/RawData Unsorted/Dylos/round_excel/JAZ/sep/', '*.csv')
    '''
    summary_stat_dylos('/RawData Unsorted/Dylos/round_{0}/'.format('excel'), '*.csv')
    summary_label('/RawData Unsorted/Dylos/round_{0}/'.format('excel'), '*.csv', kind)
    summary_all_general('/RawData Unsorted/Dylos/round_{0}/'.format('excel'))
    '''
    return

# remove duplicate files in the summary folder
def remove_dup_files(x):
    for kind in ['Large', 'Small']:
        df = pd.read_csv(parent_dir(os.getcwd()) + '/dropdup/Dylos/round_{0}/summary/{1}_round_{0}.csv'.format(x, kind))
        df_unique = df.drop_duplicates(cols = ['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max', 'Raw Start Time', 'Raw End Time', 'home_id_standard'])
        df_unique.to_csv(parent_dir(os.getcwd()) + '/dropdup/Dylos/round_{0}/summary/{1}_round_{0}_uniquefile.csv'.format(x, kind), index=False)

def join_latlong():
    filelist = glob.glob(parent_dir(os.getcwd()) + '/RawData Unsorted/Speck/round_all_bulkdownload/summary/*.csv')
    print filelist
    df_latlong = pd.read_csv(os.getcwd() + '/input/feed_info_withname.csv')
    df_latlong = df_latlong[['filename', 'latitude', 'longitude']]
    for f in filelist:
        df = pd.read_csv(f)
        df_all = pd.merge(df, df_latlong, how='left', on='filename')
        outfile = f.replace('.csv', '_latlong.csv')
        df_all.to_csv(outfile, index=False)

def join_static_rounddate(kind):
    print 'Need to remove _range.csv files before joining !!'
    print 'join cohort start and end date info'
    if kind == 'dylos':
        filelist = glob.glob(dylos_summary_path + '*.csv')
    else:
        filelist = glob.glob(speck_summary_path + '*.csv')
    df_round = pd.read_csv(os.getcwd() + '/input/cohort_time.csv')
    for f in filelist:
        print f
        df = pd.read_csv(f)
        df2 = pd.merge(df, df_round, on='round', how='left')
        outfile = f.replace('.csv', '_range.csv')
        df2.to_csv(outfile, index=False)
    return

def main():
    # ## ## ## ## ## ## ## ## ## ## ## #
    # Dylos cleaning and summary #
    # ## ## ## ## ## ## ## ## ## ## ## #
    kind = 'dylos'
    cohort = 'all'
    cleaning(cohort)
    # summary_label_round(cohort, kind)
    # summary_all(cohort)
    # remove_dup_files(cohort)
    # join_static_rounddate(kind)

    # parse_excel()

    # ## ## ## ## ## ## ## ## ## ## ## #
    # Speck cleaning and summary #
    # ## ## ## ## ## ## ## ## ## ## ## #

    kind = 'speck'
    # print 'speck summary start'
    # # summary_speck_stat('/RawData Unsorted/Speck/round_all_bulkdownload/', '*.csv')
    # # summary_label('/RawData Unsorted/Speck/round_all_bulkdownload/', '*.csv', kind)
    # # summary_all_general('/RawData Unsorted/Speck/round_all_bulkdownload/', kind)
    # # join lat long info to summary
    # # join_latlong()
    # print 'speck summary end'
    join_static_rounddate(kind)
    return

main()

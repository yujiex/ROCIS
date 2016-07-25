import pandas as pd
import os
import glob
import numpy as np
import re

def parent_dir(dirname):
    return dirname[:dirname.find('/routines')]

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

# df is a describe df
def int_describe(df):
    for col in df:
        df[col] = df[col].map(lambda x: (int(round(x, 0))))
    return df

# if at least two comma are in the string, return True, else return False
def contain_two_comma(string):
    comma_1 = string.find(',')
    comma_2 = string.rfind(',')
    return (comma_1 != -1 and comma_1 != comma_2)

def get_first_puttyline(puttylines):
    length = len(puttylines)
    for i in range(length):
        if contain_two_comma(puttylines[i]):
            return i

# subfolder is '/...'
def insert_sub_folder(path, subfolder):
    lastslash = path.rfind('/')
    return path[:lastslash] + subfolder + path[lastslash:]

def clean_putty(folder, suffix):
    filelist = glob.glob(parent_dir(os.getcwd()) + folder + suffix)
    processed_files = []
    logger_types = []
    is_processed = []
    for f in filelist:
        filename = f[(f.find(folder) + len(folder)):]
        if ('.xlsx' in filename) or ('.xls' in filename):
            is_processed.append(False)
            continue
        print filename
        processed_files.append(filename)
        is_processed.append(True)
        with open (f, 'r') as rd:
            lines = rd.readlines()
            datalines = []
            putty = False
            dylos = False
            corrupt_dylos = False
            first_line = 0
            ill_format_linenum = []
            outfile = f.replace('RawData Unsorted', 'reformat')
            outfile_err = f.replace('RawData Unsorted', 'ErrMessage_reformat')

            length = len(lines)
            for i in range(length):
                lines[i] = lines[i].replace(', Small, Large', ',Small,Large')
                if 'PuTTY log' in lines[i]:
                    putty = True
                if 'Dylos Logger' in lines[i]:
                    dylos = True
                if (contain_two_comma(lines[i]) and ('Small' not in lines[i]):
                    datalines.append(lines[i])
                else:
                    # headers are also logged to here
                    ill_format_linenum.append(i)

            '''
            if dylos:
                nskip = 0
                for i in range(min(len(lines), 30)):
                    if 'Date/Time,Small,Large' in lines[i]:
                        nskip = i
                        break

                dylostail = lines[nskip + 1:]
                for line in dylostail:
                    if 'Date/Time' in line:
                        corrupt_dylos = True
                        corrupt_dyloslines = lines
                dyloslines = [lines[nskip]] + dylostail
                dyloslines = [x for x in dyloslines if contain_two_comma(x)]
            '''
            # resolve filename
            if not '.' in outfile:
                outfile += '.txt'
            if len(datalines) != 0:
                with open (outfile, 'w+') as wt:
                    datalines = ['Date/Time,Small,Large\r\n'] + datalines
                    wt.write(''.join(datalines))
                if len(ill_format_linenum) != 0:
                    with open (outfile_err_putty, 'w+') as wt:
                        err_lines = [lines[i] for i in ill_format_linenum]
                        for x in err_lines:
                            print x
                        wt.write(''.join(err_lines))
                else:
                    with open (outfile, 'w+') as wt:
                        wt.write(''.join(dyloslines))
            else:
                with open (outfile_err_unknown, 'w+') as wt:
                    wt.write(''.join(lines))

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

def standardize_equipid(equipid):
    prog_ID_1 = re.compile('^D0[0-9]{3}$')
    if re.match(prog_ID_1, equipid):
        return 'D' + equipid[-3:]
    else:
        return equipid[:4]

def standardize_spe_location(field):
    if field == 'LIVINGROOM':
        return 'LIVING'
    else:
        return field

def standardize_gen_location(field):
    if field == '1STFLOOR':
        return 'F1'
    if field == 'Dfab':
        return field
    field = field[0]
    if field == 'F':
        return 'R'
    else:
        return field

def get_fields(filename, df_lookup):
    d = {'equip_id': '-', 'home_id': '-', 'general_location': '-',
         'specific_location': '-', 'home_id_guess': '-',
         'equip_id_standard': '-', 'home_id_standard': '-',
         'specific_location_standard': '-',
         'general_location_standard': '-'}
    if filename == '':
        return d
    #df_lookup = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_02-05-15_10 PM.csv')
    d['originalFilename'] = filename
    initial_list = list(set(df_lookup['Initials'].tolist()))
    initial_list = [x.replace(' ', '') for x in initial_list]

    # order is important
    filename = filename.upper()
    filename = filename.replace('DDYLOS', '')
    filename = filename.replace('DYLOS', '')
    filename = filename.replace('LOG', '')
    filename = filename.replace('ROCIS', '')
    filename = filename.replace('FLOATER', 'ROAMER')

    filename = filename.replace('-', '_')
    filename = filename.replace(' ', '_')

    field_list = filename.split('_')

    #prog_equip = re.compile('.*?D[0-9]{3}')
    prog_equip = re.compile('D[0-9]{3,4}')
    prog_ID = re.compile('^[A-Z]{2,3}$')
    general_loc = ['inside', 'outside', 'roamer', 'roam', 'rover', 'I', 'O',
                   'R', 'F', '1stfloor', 'Dfab']
    general_loc_upper = [x.upper() for x in general_loc]
    specific_loc = ['garage', 'office', 'livingroom', 'kitchen', 'bedroom',
                    'porch', 'living', 'basement']
    specific_loc_upper = [x.upper() for x in specific_loc]
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
            d['home_id'] = x
            continue
        elif (re.match(prog_ID, x) and d['home_id'] == '-'):
            d['home_id_guess'] = x
            continue
    d['equip_id_standard'] = standardize_equipid(d['equip_id'])
    d['specific_location_standard'] = \
            standardize_spe_location(d['specific_location'])
    d['general_location_standard'] = \
            standardize_gen_location(d['general_location'])
    d['home_id_standard'] = d['home_id']
    if d['home_id_standard'] == '-':
        d['home_id_standard'] = d['home_id_guess']

    return d

def test_get_fields():
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
                    get_fields(x)[key])

    df.to_csv(parent_dir(os.getcwd()) + ('/reformat/Dylos/round_test/name_cvt.csv'), index=False)

# print Speck specifications
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
            # BOOKMARK: try read_csv(f, parse_dates = [colname])
            #df = pd.read_csv(f)
            df = pd.read_csv(f, parse_dates = ['Date/Time'])
            df = df[['Date/Time', cate]]
            df_disc = int_describe(df.describe())
            df_disc = df_disc.transpose()
            # NOTE: supper slow to convert to datetime, but to take max and min
            # this seems necessary
            # find ways to speed this up later
            print df.head()
            df_disc['originalFilename'] = filename
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
            df_lb = pd.DataFrame(get_fields(filename, df_lookup), index=[0])
            dfs_label.append(df_lb)

        df_stat = pd.concat(dfs, ignore_index = True)
        df_nameinfo = pd.concat(dfs_label, ignore_index = True)
        df_all = pd.merge(df_stat, df_nameinfo, on='originalFilename')
        df_all['filename_standard'] = df_all.apply(lambda row: \
                '{0}_{1}_{2}_{3}'.format(row['home_id_standard'],
                                         row['general_location_standard'],
                                         row['equip_id_standard'],
                                         (row['Raw End Time']).strftime('%m-%d-%y')), axis=1)

        df_all.to_csv(parent_dir(os.getcwd()) + \
                  folder + '/summary/{0}_{1}.csv'.format(cate, suf),
                  index=False)

# print Speck specifications
def summary_speck(folder, suffix):
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
            df_disc['file'] = filename
            df_disc['Start Time'] = df['Date/Time'].min()
            df_disc['End Time'] = df['Date/Time'].max()
            order_cols = list(df_disc)
            order_cols = order_cols[-3:] + order_cols[:-3]
            df_disc = df_disc[order_cols]
            dfs.append(df_disc)
            suf = folder[folder[:-1].rfind('/') + 1:][:-1]
        pd.concat(dfs, ignore_index = True).to_csv(parent_dir(os.getcwd()) + \
                  folder + '/summary/{0}_{1}.csv'.format(cate, suf),
                  index=False)

def main():
    #print_timerange('/received_till2016_01_24/', '*.txt')
    #print_excel_sheetname('/received_till2016_01_24/DROPBOX FOLDER/', '*.xlsx')
    #print_timerange('/received_till2016_01_24/DROPBOX FOLDER/round_2/', '*.txt')
    #print_excel_sheetname('/received_till2016_01_24/DROPBOX FOLDER/round_2/', '*.xlsx')
    '''
    folders = ['/download_DROPBOX_2016_01_24/LCMP ROUND {0}/'.format(x) for x in range(2, 5)]
    for folder in folders:
        print '\n' + folder
        print_timerange(folder, '*.txt')
        #print_excel_sheetname(folder, '*.xlsx')
    #print_csv_stat('/download_DROPBOX_2016_01_24/LCMP ROUND 2/', '*.csv')
    print_timerange('/download_DROPBOX_2016_01_24/Aded Since 1-21-16 - Mix of cohorts/', '*.txt')
    print_excel_sheetname('/download_DROPBOX_2016_01_24/Aded Since 1-21-16 - Mix of cohorts/', '*.xlsx')
    print_csv_stat('/download_DROPBOX_2016_01_24/Aded Since 1-21-16 - Mix of cohorts/', '*.csv')
    '''
    #summary_speck('/upload/RawData Unsorted_/Speck/round_mix/', '*.csv')
    #summary_speck('/RawData Unsorted/Speck/round_mix/', '*.csv')
    # clean putty file error message
    '''
    x = '4'
    clean_putty('/RawData Unsorted/Dylos/round_{0}/'.format(x), '*')
    print_timerange('/reformat/Dylos/round_{0}/'.format(x), '*')
    '''
    x = '1'
    clean_putty('/RawData Unsorted/Dylos/round_{0}/'.format(x), '*')
    summary_dylos('/reformat/Dylos/round_{0}/'.format(x), '*.[a-z][a-z][a-z]')
    #test_get_fields()
    return

main()

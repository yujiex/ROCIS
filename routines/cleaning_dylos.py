import re
import glob
import util
import pandas as pd

step_size = 100

def format_match(string):
    pattern = re.compile('^([0-9]{2}/){2}[0-9]{2} [0-9]{2}:[0-9]{2}, *[0-9]{0,12}, *[0-9]{0,6}\r\n')
    match = re.match(pattern, string)
    return match != None

def cleaning(path, outpath):
    with open (path, 'r') as rd:
        lines = rd.readlines()
    length = len(lines)
    datalines = ['Date/Time,Small,Large\n']
    multiplier = 1.0
    for i in range(length):
        if 'Particles per cubic foot\r\n' in lines[i]:
            unit = 'Particles per cubic foot'
            multiplier = 0.01
        if (format_match(lines[i])):
            datalines.append(lines[i])
    if outpath[-4] != '.':
        outpath += '.txt'
    with open (outpath, 'w+') as wt:
        wt.write(''.join(datalines))
    if multiplier != 1:
        change_unit_and_dropdup(outpath, multiplier)
    else:
        dropdup(outpath)
    return multiplier

def dropdup(f):
    df = pd.read_csv(f)
    df.drop_duplicates(cols='Date/Time', take_last=True, inplace=True)
    df['Date/Time'] = df['Date/Time'].map(util.correct_month) 
    df.to_csv(f, index=False)
    return

def change_unit_and_dropdup(f, m):
    df = pd.read_csv(f)
    categories = list(df)
    categories.remove('Date/Time')
    for cate in categories:
        df[cate] = df[cate].map(lambda x: int(round(x * m, 0)))
    df.drop_duplicates(cols='Date/Time', take_last=True, inplace=True)
    df['Date/Time'] = df['Date/Time'].map(util.correct_month) 
    df.to_csv(f, index=False)
    return

def test_cleaning():
    files = glob.glob(util.get_path('Dylos', 'raw_data', 'all') + '*.[a-z][a-z][a-z]')
    # files = files[:101]
    files = [x for x in files if 'CTG_R_D077_11-19' in x]
    mlines = ['filename,multiplier\n']
    for i, f in enumerate(files):
        if i % step_size == 0:
            print i
        m = cleaning(f, f.replace('raw_data', 'reform_'))
        mlines.append('{0},{1}\n'.format(f[f.rfind('/') + 1:], m))
    with open (util.get_path('Dylos', 'reform_', 'all') + 'summary/m.csv', 'w+') as wt:
        wt.write(''.join(mlines))
    return

def main():
    test_cleaning()
    return
    
# main()

import re

def format_match(string):
    pattern = re.compile('^([0-9]{2}/){2}[0-9]{2} [0-9]{2}:[0-9]{2}, *[0-9]{0,12}, *[0-9]{0,6}\r\n')
    match = re.match(pattern, string)
    return match != None

def cleaning(path, outpath):
    with open (path, 'r') as rd:
        lines = rd.readlines()
    length = len(lines)
    datalines = ['Date/Time,Small,Large\n']
    for i in range(length):
        if (format_match(lines[i])):
            datalines.append(lines[i])
    with open (outpath, 'w+') as wt:
        wt.write(''.join(datalines))

import pandas as pd
import os
import glob
import numpy as np
from info_check import *

def test_contain_two_comma():
    string = 'awh, asdoi'
    assert(not contain_two_comma(string))
    string = '01/18/16 17:38,945,63'
    assert(contain_two_comma(string))

def test_insert_sub_folder():
    path = '/media/yujiex/work/ROCIS/RawData Unsorted/Dylos/round_1/CD_D013_inside_10-15-2015.txt'
    assert(insert_sub_folder(path, '/putty') == \
            '/media/yujiex/work/ROCIS/RawData Unsorted/Dylos/round_1/putty/CD_D013_inside_10-15-2015.txt')
    return

def main():
    #test_contain_two_comma()
    test_insert_sub_folder()
    print 'all test passed!'
    return

main()

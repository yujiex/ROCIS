import os
import pandas as pd

def parent_dir(dirname):
    return dirname[:dirname.find('/routines')]

def get_path(sensor, step, cohort):
    return '{0}/DataBySensor/{1}/{2}/round_{3}/'.format(parent_dir(os.getcwd()), sensor, step, cohort)

def get_homeid_dict():
    df_lookup = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv')
    home_id_dict = dict(zip(df_lookup['INITIALS'], df_lookup['HOME ID CORRECT']) + zip(df_lookup['HOME ID CORRECT'], df_lookup['HOME ID CORRECT']) + zip(df_lookup['LAST'], df_lookup['HOME ID CORRECT']))
    return home_id_dict

def split_string(sep_list, string):
    for x in sep_list:
        string = string.replace(x, '_')
    tokens = string.split('_')
    return tokens

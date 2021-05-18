#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def read_file_hql(path_to_file, multi=False):
    """
    Input: The path to the hql query file
    Output: The query from the file
    
    Note: If there are many hql queries separated by ';' in the file, the function will return the first hql query
    """
    fd = open(path_to_file, 'r')
    hqlFile = fd.read()
    fd.close()

    hqlCommand = hqlFile.split(';')

    if multi:
        return(hqlCommand)
    else:
        return(hqlCommand[0])


def exec_spark_sql(path_to_file,spark_hive):
    try:

        hql = read_file_hql(path_to_file)
        print(hql)
        df = spark_hive.sql(hql).show()
        print(df.cache().count())
    except Exception as e:
        print('Failed : ' + str(e))
    
def soma(a,b):
    return a+b

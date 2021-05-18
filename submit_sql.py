import os

TIPO = 'FILE'
CAMINHO = 'c://titanic.csv'
TABLE = 'titanic'
HQL = "./query.hql"
os.system("spark-submit exec_query.py {0} {1} {2}".format(HQL, CAMINHO, TABLE))


import os
print('************************************ sql XPTO ************************************')
HQL="./query.hql"
os.system("spark-submit  "  "./exec_query.py "  + HQL)
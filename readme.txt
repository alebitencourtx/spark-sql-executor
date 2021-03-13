Executar o arquivo submit_sql.py, segue chamada abaixo:
	python submit_sql.py

O arquivo submit_sql.py executa via spark-submit o módulo "exec_query.py", passando como parâmetro o caminho para o arquivo .hql (pode ser SQL) hardcode, no caso sendo o "query.hql".

O arquivo exec_query.py inicia a sessão Spark, lê o parâmetro, adiciona o arquivo de query e o módulo de functions na sessão spark e chama a função "exec_spark_sql".
import duckdb
#  # open the local db
# local_con = duckdb.connect("dev.duckdb") 

# local_con.sql('select * from entity limit 100')
# # connect to MotherDuck
# local_con.sql("ATTACH 'md:'")
# # The from indicates the file to upload. An empty path indicates the current database 
# local_con.sql("CREATE OR REPLACE DATABASE clouddb FROM CURRENT_DATABASE()") 
import pandas as pd
local_con = duckdb.connect("dev.duckdb")

final_cardevents = local_con.sql('select * from final_cardevents')
final_cardevents.to_csv('final_cardevents.csv')

final_cardtransactions = local_con.sql('select * from final_cardtransactions')
final_cardtransactions.to_csv('final_cardtransactions.csv')

final_entity = local_con.sql('select * from final_entity')
final_entity.to_csv('final_entity.csv')
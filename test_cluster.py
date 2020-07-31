#Programme à utiliser pour un cluster de machines, très similaire au mode local, sauf la ligne 6 qu'il faut modifier
import time
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import *
sc = SparkContext(master="spark://173.212.232.106:7077") #changez la fin par votre propre adresse ip mais gardez le port 7077
spark = SparkSession(sc)      
df1=spark.read.csv("/root/tickets.csv",inferSchema=True,sep=',',header=True)  
df2=spark.read.csv("/root/postes.csv",inferSchema=True,sep=',',header=True)
df1.registerTempTable('table1') 
df2.registerTempTable('table2')
tmps1=time.time()
spark.sql('select * from table1 join table2 on CO_EXT=PO_NOM limit 50').show()
tmps2=time.time()-tmps1
print(tmps2) 

#Programme à utiliser pour un cluster de machines
import time
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import *
sc = SparkContext(master="spark://173.212.232.106:7077") #changez la fin par votre propre adresse ip mais gardez le port 7077
spark = SparkSession(sc)      
#On va maintenant créer deux DataFrames df1 et df2 à partir des fichiers CSV "tickets.csv" et "postes.csv" qui se trouvent dans le répertoire "root"
df1=spark.read.csv("/root/tickets.csv",inferSchema=True,sep=',',header=True) #l'option "sep=','" indique que les colonnes sont délimiitées par des virgules, l'option "header=True" permet à PySpark de nommer les colones à partir des informations contenues dans le csv (vous pouvez par exemple indiquer le nom des colonnes dans la première ligne)
df2=spark.read.csv("/root/postes.csv",inferSchema=True,sep=',',header=True)
df1.registerTempTable('table1') #on crée un objet qu'on appelle "table1" à partir de df1 sur lequel on va pouvoir faire des requetes SQL
df2.registerTempTable('table2') #idem
tmps1=time.time()
spark.sql('select count(*) from table1 join table 2 on CO_EXT=PO_NOM ').show() #On effectue la requête. Le ".show()" permet d'afficher la requete mais on peut aussi stocker le résultat dans une variable
tmps2=time.time()-tmps1
print(tmps2) #on affiche le temps de la requête

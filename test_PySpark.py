#Programme à utiliser pour executer PySpark en local
import time
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import *
sc = SparkContext(master="local[6]") #indique que vous voulez travailler en local avec 6 core (vous pouvez changer le nombre en fonction du nombre de cores de votre processeur)
spark = SparkSession(sc)      
#On va maintenant créer deux DataFrames df1 et df2 à partir des fichiers CSV "tickets.csv" et "postes.csv" qui se trouvent dans le répertoire "root"
df1=spark.read.csv("/root/tickets.csv",inferSchema=True,sep=',',header=True) #l'option "sep=','" indique que les colonnes sont délimiitées par des virgules, l'option "header=True" permet à PySpark de nommer les colones à partir des informations contenues dans le csv (vous pouvez par exemple indiquer le nom des colonnes dans la première ligne)
df2=spark.read.csv("/root/postes.csv",inferSchema=True,sep=',',header=True)
df1.registerTempTable('table1') #on crée un objet qu'on appelle "table1" à partir de df1 sur lequel on va pouvoir faire des requetes SQL
df2.registerTempTable('table2') #idem
tmps1=time.time()
spark.sql('select * from table1 join table2 on CO_EXT=PO_NOM limit 50 ').show(False) #On effectue la requête. Le ".show()" permet d'afficher la requete mais on peut aussi stocker le résultat dans une variable
tmps2=time.time()-tmps1
print(tmps2) #on affiche le temps de la requête
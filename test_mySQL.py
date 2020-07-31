# il faut avoir installé la lib. Mysql avec en ligne de commande : 'pip install mysql-connector-python'
import mysql.connector
import time
# Connecte MySql sur localhost avec l'utilisateur root et le mot de passe root
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
)


mycursor = mydb.cursor()

# Connecte la base MySql et ouvre la base test
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database = "test"
)
mycursor = mydb.cursor()
tmps1=time.time()
mycursor.execute("SELECT max(CO_DRING +PO_IDSCE) from grandc join grandp on CO_EXT=PO_NOM")
myresult = mycursor.fetchall()
print('Res. de requête :')
for x in myresult:
    print(x)
print(time.time()-tmps1)
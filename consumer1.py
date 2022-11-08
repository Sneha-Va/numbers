import mysql.connector
from kafka import KafkaConsumer
try:
    mydb=mysql.connector.connect(host='localhost',user='root',password='',database='numberdb')
except mysql.connector.Error as e:
    print("mysql error",e)
mycursor=mydb.cursor()
bootstrap_server=["localhost:9092"]
topic="naturalNumber"
consumer=KafkaConsumer(topic,bootstrap_servers=bootstrap_server)
for i in consumer:
    print(str(i.value.decode()))
    random_rev=int(i.value.decode())
    reverse_num=0
    while random_rev > 0:
        reverse=random_rev%10
        reverse_num=(reverse_num*10)+reverse
        random_rev=random_rev//10
    sql="INSERT INTO `reversenumber`(`reverse`) VALUES (%s)"
    data=(reverse_num,)
    mycursor.execute(sql,data)
    mydb.commit()
    print("reverse number added to db",reverse_num)
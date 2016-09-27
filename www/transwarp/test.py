import mysql.connector
import time

conn = mysql.connector.connect(user='user', password='www-data',database='blogdb', use_unicode=True)
cursor = conn.cursor()
cursor.execute('drop table if exists user')
cursor.execute('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
cursor.execute('insert into user (id, name, email, passwd, last_modified) values (%s, %s, %s, %s, %s)', ['109', 'Michael', 'michael@test.org', 'abc-1234', time.time()])
print cursor.rowcount
conn.commit()
cursor.close()

cursor = conn.cursor()
cursor.execute('select * from user where id = %s', ('100',))
values = cursor.fetchall()
print values

cursor.close()
conn.close()

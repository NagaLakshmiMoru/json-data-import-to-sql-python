import mysql.connector  # pip3 install mysql-connector-python
import json
from datetime import datetime

conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
)
cursor = conn.cursor()
database = 'GeoStats'
tableName = 'RegionStats'
fileData = []


def createDataBase():
    print('<< create SQL Database Entry >>')
    create_database_query = "CREATE DATABASE IF NOT EXISTS " + database
    cursor.execute(create_database_query)
    print('<< create SQL Database End >>')


def userDataBase():
    print('<< connect SQL Database Entry >>')
    use_database_query = "USE " + database
    cursor.execute(use_database_query)
    print('<< connect SQL Database End >>')


def createSQLStatsTable():
    print('<< create SQL Table Entry >>')
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS ''' + tableName + ''' (
            end_year INT,
            intensity INT,
            sector VARCHAR(128),
            topic VARCHAR(128),
            insight VARCHAR(256),
            url VARCHAR(512),
            region VARCHAR(128),
            start_year INT,
            impact INT,
            added DATETIME,
            published DATETIME,
            country VARCHAR(128),
            relevance INT,
            pestle VARCHAR(128),
            source VARCHAR(128),
            title VARCHAR(512),
            likelihood INT
    )
    '''
    print('create_table_query : ', create_table_query)
    cursor.execute(create_table_query)
    conn.commit()
    print('<< create SQL Table End >>')


def loadJsonData():
    print('<< load json data Entry >>')
    global fileData
    with open('jsondata.json') as file:
        fileData = json.load(file)
    print('fileData size : ', len(fileData))

    print('<< load json data End >>')


def insertDataIntoTable():
    print('<< load json data to db Entry >>')
    for record in fileData:
        print('record : ', record)
        insert_query = '''
        INSERT INTO ''' + tableName + ''' (end_year, intensity, sector, topic, insight, url, region, start_year, impact, added, published, country, relevance, pestle, source, title, likelihood)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        print(insert_query)
        cursor.execute(insert_query, (
            None if record['end_year'] == '' else record['end_year'],
            None if record['intensity'] == '' else record['intensity'],
            None if record['sector'] == '' else record['sector'],
            None if record['topic'] == '' else record['topic'],
            None if record['insight'] == '' else record['insight'],
            None if record['url'] == '' else record['url'],
            None if record['region'] == '' else record['region'],
            None if record['start_year'] == '' else record['start_year'],
            None if record['impact'] == '' else record['impact'],
            None if record['added'] == '' else datetime.strptime(record['added'], "%B, %d %Y %H:%M:%S"),
            None if record['published'] == '' else datetime.strptime(record['published'], "%B, %d %Y %H:%M:%S"),
            None if record['country'] == '' else record['country'],
            None if record['relevance'] == '' else record['relevance'],
            None if record['pestle'] == '' else record['pestle'],
            None if record['source'] == '' else record['source'],
            None if record['title'] == '' else record['title'],
            None if record['likelihood'] == '' else record['likelihood'],))
    conn.commit()
    print('<< load json data to db End >>')


if __name__ == '__main__':
    createDataBase()
    userDataBase()
    createSQLStatsTable()
    loadJsonData()
    insertDataIntoTable()
    conn.close()

import os
import pymysql
import pandas as pd
import shutil

# Create a connection object
dbServerName    = "127.0.0.1"
dbUser          = "root"
dbPassword      = ""
dbName          = "dump-petmate_db-201907131931.sql"
cusrorType      = pymysql.cursors.DictCursor
connectionObject   = pymysql.connect(host=dbServerName, user=dbUser, password=dbPassword,
                                     db=dbName,cursorclass=cusrorType)

try:
    # Create a cursor object
    cursorObject = connectionObject.cursor()      

    originalDIR = '/home/pratik/Desktop/images/'

    sql1 = "DELETE From notifications where pet_id_id IN(6,8,9);"
    sql2 = "DELETE From notifications where receiver_pet_id IN(6,8,9);"
    sql3 = "DELETE From pet_chat where receiver_pet_id IN(6,8,9);"
    sql4 = "DELETE From pet_chat where sender_pet_id IN(6,8,9);"
    sqlQuery = "SELECT photo as photos FROM pet_photos WHERE pet_id_id IN(6,8,9);"
    sqlQuery1 = "SELECT primary_photo as photos FROM `pets` where id IN(6,8,9);"
    healthSqlQuert = "SELECT `health_certificate` as photos  FROM `pets` WHERE id IN(6,8,9);"
    sql5 = "DELETE From pet_photos where pet_id_id IN(6,8,9);"
    sql6 = "DELETE From pet_preferences where pet_id_id IN(6,8,9);"
    sql7 = "DELETE From like_dislike where sender_pet_id IN(6,8,9);"
    sql8 = "DELETE From like_dislike where receiver_pet_id IN(6,8,9);"
    sql9 = "DELETE From pets where id IN(6,8,9);"

    def removeImage(dataframe):  
        for index, row in dataframe.iterrows():
            folder=row['photos'].split('/', 1)[0]
            if os.path.exists(originalDIR+folder) and folder != '':
                shutil.rmtree(originalDIR+folder)
                print(originalDIR+folder ,'deleted')
            else:
                print(folder,'folder not found')

    cursorObject.execute(sql1)
    cursorObject.execute(sql2)
    cursorObject.execute(sql3)
    cursorObject.execute(sql4)
    cursorObject.execute(sqlQuery)
    rows= cursorObject.fetchall()
    if len(rows) != 0:
        pet_photosData = pd.DataFrame(rows)
        removeImage(pet_photosData)

    cursorObject.execute(sqlQuery1)
    rows2= cursorObject.fetchall()
    if len(rows2) != 0:
        petsData = pd.DataFrame(rows2)
        removeImage(petsData)

    cursorObject.execute(healthSqlQuert)
    rows3= cursorObject.fetchall()
    if len(rows3) != 0:
        healthpetsData = pd.DataFrame(rows3)
        removeImage(healthpetsData)

    cursorObject.execute(sql5)
    cursorObject.execute(sql6)
    cursorObject.execute(sql7)
    cursorObject.execute(sql8)
    cursorObject.execute(sql9)


except Exception as e:

    print("Exeception occured:{}".format(e))

finally:
    connectionObject.close()
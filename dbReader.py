import sqlite3
import csv
import os

DATABASE = 'database.db'
fieldnames = ['time', 'temperature', 'humidity', 'pressure', 'pitch', 'roll', 'yaw', 'mag_x', 'mag_y', 'mag_z',
              'acc_x', 'acc_y', 'acc_z', 'gyro_x', 'gyro_y', 'gyro_z']
FIELDS = ['time', 'balloon_id', 'temperature', 'humidity', 'pressure', 'pitch', 'roll', 'yaw',
          'mag_x', 'mag_y', 'mag_z', 'acc_x', 'acc_y', 'acc_z', 'gyro_x', 'gyro_y', 'gyro_z']

logfile = None
logwriter = None

def openDB():
    return sqlite3.connect(DATABASE)

def dbopen():
    if os.path.isfile(DATABASE):
        print('DB Exists')
        return openDB()
    else:
        print('DB Does Not Exist')
        return

def createDB():
    db = sqlite3.connect(DATABASE)
    db.execute('CREATE TABLE ServerNormalization (time_stamp TEXT, temperature DOUBLE, humidity DOUBLE, pressure DOUBLE, pitch DOUBLE, roll DOUBLE, yaw DOUBLE, magnitude_x DOUBLE, magnitude_y DOUBLE, magnitude_z DOUBLE, acceleration_x DOUBLE, acceleration_y DOUBLE, acceleration_z DOUBLE, gyroscope_x DOUBLE, gyroscope_y DOUBLE, gyroscope_z DOUBLE)')
    db.execute('CREATE TABLE BalloonTicket (time_stamp TEXT, balloon_id INT, temperature DOUBLE, humidity DOUBLE, pressure DOUBLE, pitch DOUBLE, roll DOUBLE, yaw DOUBLE, magnitude_x DOUBLE, magnitude_y DOUBLE, magnitude_z DOUBLE, acceleration_x DOUBLE, acceleration_y DOUBLE, acceleration_z DOUBLE, gyroscope_x DOUBLE, gyroscope_y DOUBLE, gyroscope_z DOUBLE)')
    db.commit()
    return db

def dbREADBalloonTicket(db):
    for itterator in range(0,5):
        cursor = db.execute('SELECT time_stamp, balloon_id,temperature, humidity, pressure, pitch, roll, yaw, magnitude_x, magnitude_y, magnitude_z, acceleration_x, acceleration_y, acceleration_z, gyroscope_x, gyroscope_y, gyroscope_z from BalloonTicket')
        for row in cursor:
            if itterator == row[1]:
                for i in range(16):
                    if not (i == 1):
                        print(FIELDS[i] + ' : ' + str(row[i]))
    db.close()
    return

def dbWRITEBalloonTicket(db):
    cursor = db.execute('SELECT time_stamp, balloon_id,temperature, humidity, pressure, pitch, roll, yaw, magnitude_x, magnitude_y, magnitude_z, acceleration_x, acceleration_y, acceleration_z, gyroscope_x, gyroscope_y, gyroscope_z from BalloonTicket')
    rows = cursor.fetchall()
    for itterator in range(0,5):
        cursor = db.execute('SELECT time_stamp, balloon_id,temperature, humidity, pressure, pitch, roll, yaw, magnitude_x, magnitude_y, magnitude_z, acceleration_x, acceleration_y, acceleration_z, gyroscope_x, gyroscope_y, gyroscope_z from BalloonTicket')
        filename = rows[0][0][:rows[0][0].find(' ')] + ' BALLON ID ' + str(itterator) + '.csv'
        logfile = open(filename, 'a')
        logwriter = csv.DictWriter(logfile, fieldnames=fieldnames)
        logwriter.writeheader()
        for row in cursor:
             if itterator == row[1]:
                logfile = open(filename, 'a')
                logwriter = csv.DictWriter(logfile, fieldnames=fieldnames)
                logwriter.writerow({
                    'time': str(row[0]),
                    'temperature': str(row[2]), #This means temperature in Spanish.
                    'humidity': str(row[3]),
                    'pressure': str(row[4]),
                    'pitch': str(row[5]),
                    'roll': str(row[6]),
                    'yaw': str(row[7]),
                    'mag_x': str(row[8]), 'mag_y': str(row[9]), 'mag_z': str(row[10]),
                    'acc_x': str(row[11]), 'acc_y': str(row[12]), 'acc_z': str(row[13]),
                    'gyro_x': str(row[14]), 'gyro_y': str(row[15]), 'gyro_z': str(row[16])})
                logfile.close()
    db.close()
    return


## Used to Read
dbREADBalloonTicket(dbopen())

## Used to Create every single
dbWRITEBalloonTicket(dbopen())
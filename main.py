import datetime
import os
import time
import pandas as pd
import pyodbc

conn = pyodbc.connect(r'Driver={SQL Server};'
                      r'Server=SERVERNAME;'
                      r'Database=DB_NAME;'
                      r'UID=USERNAME'
                      r'PWD=PASSWORD'
                      )


def getListOfFiles(dirName):
    # create a list of file and sub directories
    # names in the given directory
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(fullPath) and not entry.startswith('.'):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)

    return allFiles


def main():
    dirName = '//FILEPATH'
    # Get the list of all files in directory tree at given path
    listOfFiles = getListOfFiles(dirName)
    # get current time
    current_time = datetime.datetime.now()

    for elem in listOfFiles:
        date_mod = datetime.datetime.strptime(time.ctime(os.path.getmtime(elem)), "%a %b %d %H:%M:%S %Y")
        df = pd.DataFrame({"File_Name": [elem], "Date_Modified": [date_mod], "Date_Imported": [current_time]})

        insert_into = f"INSERT INTO TABLENAME (File_Name, Date_Modified, Date_Imported) VALUES(?, ?, ?)"
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany(insert_into, df.values.tolist())
        conn.commit()

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()

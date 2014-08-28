#!/usr/bin/python
import json
import os
import gzip
import sqlite3
import argparse
import sys
import csv

def parseArgs():
   print "\n"
   # Arguments menu using argparse module
   parser = argparse.ArgumentParser(prog="jsonvert.py", 
                     description="""Export JSON data with embedded arrays to SQL.\n""",
                     usage="""%(prog)s --path [PATHNAME] --dbname database.db --primaryk [PKEY] --table1 [ID] --table2 [ID]"""                              )
   parser.add_argument("--path", help="""path to gzipped JSON data\n
                                         e.g /var/log/json/""")
   parser.add_argument("--dbname", help="""name of database to output data.\n
                                           Uses sqlite database so it can run 
                                           standalone from anywhere and not 
                                           interfere with live client database.\n""",
                                           nargs="?", default="output.db")
   parser.add_argument("--primaryk", help="""Primary key ID to begin from.\n
                                             Defaults to zero.\n""",
                                             nargs="?", default="0")
   parser.add_argument("--table1", help="""table1 ID to begin from,\n
                                              important to set this if inserting into live database \n
                                              or it will likely error with duplicate IDs,\n
                                              defaults to zero.\n""",
                                              nargs="?", default="0")
   parser.add_argument("--table2", help="""table2 ID to begin from,\n
                                               important to set this if inserting into live database \n
                                               or it will likely error with duplicate IDs,\n
                                               defaults to zero.\n""",
                                               nargs="?", default="0")
   args = parser.parse_args()
   # Parse arguments in lowercase and run correct method
   if args.path:
      global destDir 
      destDir = args.path.lower()
   else:
      sys.exit(parser.print_help())
   if args.dbname:
      global dbname
      dbname = args.dbname
   else:
      sys.exit(parser.print_help())   
   if args.primaryk is not None:
      global primaryk
      primaryk = int(args.primaryk)
   if table1 is not None:
      global table1
      table1 = int(args.table1)
   if args.table2 is not None:
      global table2
      table2 = int(args.table2)


def createDB():
   print "Creating Sqlite database...\n"
   # Make connection to database
   with sqlite3.connect(dbname) as connection:
      c = connection.cursor()
      # Create table1 table
      c.execute("""CREATE TABLE primary_data (  
                   ID INTEGER PRIMARY KEY,   
                   DATE TEXT,        
                   IP_ADDRESS TEXT, );""")
      # Create table2 table 
      c.execute("""CREATE TABLE secondary_data (  
                   ID INTEGER PRIMARY KEY, 
                   FKEY1 INT, 
                   DATA TEXT;""")
      # Create table3 table 
      c.execute("""CREATE TABLE tercer_data (
                   ID INTEGER PRIMARY KEY,
                   FKEY2 INT,
                   DATA TEXT);""")
      if primaryk is not None:
         # Create first rows in order to offset the autoincrement value of the primary keys
         c.execute("""INSERT INTO primary_data VALUES(
                      %i,'delete','delete','delete','delete','delete','delete','delete','delete')""" % primaryk)
      if table2 is not None:
         c.execute("""INSERT INTO secondary_data VALUES(
                      %i,%i,'delete','delete')""" % (primaryk, table1)) 
      if table3 is not None:
         c.execute("""INSERT INTO tercer_data VALUES(
                      %i,%i,'delete')""" % (table2, primaryk)) 
      else:
         pass

def populateDB():
   print "Populating Sqlite database...\n"
   # Make connection to database
   with sqlite3.connect(dbname) as connection:
      c = connection.cursor()
      # Put log files into a list sorted alpha numeric
      fileList = [] 
      for dirname, dirnames, filenames in os.walk(destDir):
         for filename in sorted(filenames):
            fileList.append(os.path.join(dirname, filename))
      for file in fileList:
         if file.endswith('.gz'):
            # Open each of our gzipped user logs one at a time
            sourceFile = gzip.open(file, 'rb')
            # Export JSON data, import into python readable JSON object
            for line in sourceFile:
               data = json.loads(line)
            # Create a list of table1 columns from the JSON data
               trailValues = [
                    data["date"],
                    data["ipAddress"],
                                       ]
               # insert table1 data into columns
               c.execute("""INSERT INTO primary_data VALUES(
                            NULL,?, ?,""", trailValues)
               # Get foreign key from table1 table
               c.execute("select MAX(ID) FROM primary_data;")
               maxId = c.fetchone()[0]
               # Create a list of secondary_data column names 
               # from the JSON data
               criteriaValues = (data["secondaryData"])
               # Separate the smaller dictionary from the JSON list 
               for criteriaItem in criteriaValues:
                  for values in criteriaItem.items():
                     # Combine FKEY, data into temp list
                     tempList = [maxId, values[0], values[1]]
                     c.execute("""INSERT INTO secondary_data VALUES
                                  (NULL, ?)""", tempList)
               # Create a list of table3 column names
               # from the JSON data
               tercerValues = (data["tercerData"])
               # Unlike the criteriaValues these are one-to-one and 
               # don't require another loop for dictionary.
               for roleItem in roleValues:
                  tempList = [maxId, roleItem]
                  c.execute("""INSERT INTO tercer_data VALUES
                               (NULL, ?, ?)""", tempList)
            sourceFile.close()
      # Remove rows created to initiate autoincrement primary keys
      c.execute("DELETE FROM primary_data where ID = %s" % primaryk)
      c.execute("DELETE FROM secondary_data where ID = %s" % table1)
      c.execute("DELETE FROM tercer_data where ID = %s" % table2)
      # Display to user that the file has been created in their 
      # current working directory
      print "\n" + "Output database saved to %s/%s" % (os.getcwd(), dbname) + "\n"

def MySQLDump():
   print "Dumping data to MySQL format...\n"
   # Make connection to database
   with sqlite3.connect(dbname) as connection:
      c = connection.cursor()
      with open(dbname + ".sql", 'w') as f:
         for line in connection.iterdump():
            if "INSERT INTO" in line:
               f.write('%s\n' % line.replace('"','`'))
      f.close()
   print "\n" + "MySQL dump saved to %s/%s.sql" % (os.getcwd(), dbname) + "\n"

if __name__ == "__main__":
  parseArgs() 

createDB()
populateDB()
MySQLDump()

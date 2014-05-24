#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb as mdb 
import sys, time
import logging
import logger	#this is the logger.py file
import read_SALES_events
import read_TABS_events
from datetime import datetime 
from variables import *

#Initialize
time.sleep(10)			#to make sure MySQL has started up fully
rows=None 

#Logging
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,)

#Connect to database
try:
	connRAW = mdb.connect(host, usrnm, psswrd, db_name);
	cursor = connRAW.cursor(mdb.cursors.DictCursor)
except:
	logger.messageException("RAW  | Except in connecting to database")
	
	

while True:
	#check for new events
	try:
		time.sleep(0.2)
		
		sql="SELECT * from __events_RAW where processed = '0' ORDER BY id ASC"
		cursor.execute(sql)
		rows=cursor.fetchall()
		connRAW.commit()

	except mdb.Error, e:
	  
		connRAW.rollback()
		logger.messageException(str("RAW  | MySQLdb error %d: %s" % (e.args[0],e.args[1])))

	except:
		connRAW.rollback()
		logger.messageException("RAW  | Exception in reading from __events_RAW")


	# if new event ==> take action
	try:
		for row in rows:
				check = False
				logger.messageInfo("RAW  | new RAW event: " + str(row))

				id=row["id"]
				timestamp=row["timestamp"]
				type_id=row["type_id"]
				user_id=row["user_id"]
				location_id=row["location_id"]
				tab_id=row["tab_id"]
				consumption_id=row["consumption_id"]
				consumption_nr=row["consumption_nr"]
				description=row["description"]
				amount=row["amount"]
				processed=row["processed"]
				

				if type_id in [1]:
					sql="UPDATE __events_RAW SET processed='1' WHERE id="+str(id)
					cursor.execute(sql)

					sql="INSERT INTO `_events_sales` (`RAW_id`, `timestamp`, `type_id`, `tab_id`, `consumption_id`, `consumption_nr`, `description`, `amount`) "+\
					"VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (id, timestamp, type_id, tab_id, consumption_id, consumption_nr, description, amount)
					cursor.execute((sql))
					insert_id=connRAW.insert_id()
				
					connRAW.commit()
					logger.messageInfo("RAW  | Event id="+str(id)+" processed ==> Copied to SALES events table")
					
					#The Secondary scripts are run
					while check == False:
						check = read_SALES_events.process(insert_id)
						if check == True:
							logger.messageInfo("RAW  | Event id="+str(id)+" processed by secondary SALES syntax - confirmation received ==> Ready for new event")
				
				
				elif type_id in [2,3,4,5]:
					sql="UPDATE __events_RAW SET processed='1' WHERE id="+str(id)
					cursor.execute(sql)

					sql="INSERT INTO `_events_tabs` (`RAW_id`, `timestamp`, `type_id`, `user_id`, `location_id`, `tab_id`, `description`) "+\
					"VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (id, timestamp, type_id, user_id, location_id, tab_id, description)
					cursor.execute((sql))
					insert_id=connRAW.insert_id()
					
					connRAW.commit()
					logger.messageInfo("RAW  | Event id="+str(id)+" processed ==> Copied to TABS events table")

					#The Secondary scripts are run
					while check == False:
						check = read_TABS_events.process(insert_id)
						if check == True:
							logger.messageInfo("RAW  | Event id="+str(id)+" processed by secondary TABS syntax - confirmation received ==> Ready for new event")
					
					
				

					

	except mdb.Error, e:
	  
		connRAW.rollback()
		logger.messageException(str("RAW  | MySQLdb error %d: %s" % (e.args[0],e.args[1])))

	except:
		connRAW.rollback()
		logger.messageException("RAW  | Exception during writing to database")

		
		

#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb as mdb 
import sys, time
import logging
import logger	#this is the logger.py file
from datetime import datetime 
from variables import *

def process(event_id):
	#Initialize
	rows=None 

	#Logging
	logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,)

	#Connect to database
	try:
		connSAL = mdb.connect(host, usrnm, psswrd, db_name);
		cursor = connSAL.cursor(mdb.cursors.DictCursor)
	except:
		logger.messageException("SALES | Except in connecting to database")
		
	try:
		sql="SELECT * from _events_sales where id = '%s'" % (event_id)
		cursor.execute(sql)
		row=cursor.fetchone()
		connSAL.commit()

	except mdb.Error, e:
	  
		connSAL.rollback()
		logger.messageException(str("SALES | MySQLdb error %d: %s" % (e.args[0],e.args[1])))

	except:
		connSAL.rollback()
		logger.messageException(str("SALES | Exception in reading from __events_sales"))

	try:
		rows=""			#in case the SELECT try-loop fails, 'rows' has been declared and is empty
		
		logger.messageInfo("SALES | new SALES event: " + str(row))

		id=row["id"]
		RAW_id=row["RAW_id"]
		timestamp=row["timestamp"]
		type_id=row["type_id"]
		tab_id=row["tab_id"]
		consumption_id=row["consumption_id"]
		consumption_nr=row["consumption_nr"]
		description=row["description"]
		amount=row["amount"]
		processed=row["processed"]
		
		#Conversion to Personal SALES entries
		
		#Retrieving Tab members, and loading them into the 'rows' variable
		try:
			sql="SELECT user_id from tabs_LT_users where tab_id = '%s' AND status='%s'" % (tab_id, 'active')
			cursor.execute(sql)
			rows=cursor.fetchall()
			connSAL.commit()
			
			
		except mdb.Error, e:
			connSAL.rollback()
			logger.messageException(str("TABS  | MySQLdb error %d: %s" % (e.args[0],e.args[1])))

		except:
			connSAL.rollback()
			logger.messageException("TABS  | Exception reading tabs_LT_users (retrieving TAB members)")
		
		# calculation of personal amount
		row_count=float(len(rows))				#number of members in TAB
		amount=float(amount)					#conversion from STRING to FLOAT
		pers_amount=amount/row_count	#This will give error of cents (image 10euro over 3 people
		
		
		#Creation of personal database entries for each DB member
		members_list=[]
		for row in rows:
			#each row shows a different 'user_id'
			user_id=row["user_id"]
			members_list.append(user_id)	#create a list of all the members in this tab (to use in the logging)
			
			sql="INSERT INTO `_events_sales_personal` (`SALE_id`, `user_id`, `timestamp`, `tab_id`, `consumption_id`, `consumption_nr`, `members_nr`, `description`, `amount`) "+\
			"VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%.5f')" % (id, user_id, timestamp, tab_id, consumption_id, consumption_nr, row_count, description, pers_amount)
			cursor.execute((sql))			

		#Updating TAB timestamp
		sql="UPDATE tabs SET timestamp_update='%s' WHERE id='%s'" % (timestamp, tab_id)
		cursor.execute(sql)

		#Updating SALES event
		sql="UPDATE _events_sales SET processed='1' WHERE id="+str(id)
		cursor.execute(sql)

		connSAL.commit()
		logger.messageInfo("SALES | Event id="+str(id)+" processed ==> Personal SALES events created for members with id="+str(members_list)+" for a personal amount of '%.5f'" % (pers_amount))
		return True
		
		

	except mdb.Error, e:
	  
		connSAL.rollback()
		logger.messageException(str("SALES | MySQLdb error %d: %s" % (e.args[0],e.args[1])))
		return False

	except:
		connSAL.rollback()
		logger.messageException("SALES | Exception during writing to database")
		return False

		
		

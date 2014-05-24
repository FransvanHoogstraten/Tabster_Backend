#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb as mdb 
import sys, time
import logging
import logger	#this is the logger.py file
import datetime 
from variables import *



def process(event_id):
	#Initialize
	rows=None 

	#Logging
	logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,)

	#Connect to database
	try:
		connTAB = mdb.connect(host, usrnm, psswrd, db_name);
		cursor = connTAB.cursor(mdb.cursors.DictCursor)
	except:
		logger.messageException("TABS  | Except in connecting to database")
	
	
	try:
		sql="SELECT * from _events_tabs where id = '%s'" % (event_id)
		cursor.execute(sql)
		row=cursor.fetchone()
		connTAB.commit()

	except mdb.Error, e:
	  
		connTAB.rollback()
		logger.messageException(str("TABS  | MySQLdb error %d: %s" % (e.args[0],e.args[1])))

	except:
		connTAB.rollback()
		logger.messageException("TABS  | Exception in reading from __events_tabs")

	try:
	
		logger.messageInfo("TABS  | new TABS event: " + str(row))

		id=row["id"]
		RAW_id=row["RAW_id"]
		timestamp=row["timestamp"]
		timestamp_delayed = timestamp + datetime.timedelta(0,1)
		type_id=row["type_id"]
		user_id=row["user_id"]
		location_id=row["location_id"]
		tab_id=row["tab_id"]
		description=row["description"]			#The "name:" variable stays in the description. It is useless to take it out. 
		processed=row["processed"]
		
		
		
		
		
		
################ Create Tab
		if type_id in [2]:
			name=""
			try:
				name=description.split("name:")[1]
				name=name.split("|")[0]
			except:
				logger.messageException("TABS  | Exception while retrieving Tab name")
				
			#Register new Tab in table.
			sql="INSERT INTO `tabs` (`timestamp_create`, `timestamp_update`, `name`, `creator_id`, `location_id`, `description`) "+\
			"VALUES ('%s', '%s', '%s', '%s', '%s', '%s')" % (timestamp, timestamp, name, user_id, location_id, description)
			cursor.execute((sql))
			new_tab_id=connTAB.insert_id()

			#Create User as a Tab Member in LinkTable
			sql="INSERT INTO tabs_LT_users (`tab_id`, `user_id`, `timestamp_join`) VALUES ('%s', '%s', '%s')" % (new_tab_id, user_id, timestamp)
			cursor.execute((sql))
			
			#Creation of tab-join event for tab starter (pure for graphical reasons
			sql="INSERT INTO `_events_tabs` (`RAW_id`, `timestamp`, `type_id`, `user_id`, `tab_id`, `processed`) VALUES ('%s', '%s', '%s', '%s', '%s', '%s')" % (RAW_id, timestamp_delayed, '4', user_id, new_tab_id, '1')
			cursor.execute((sql))

			#Update original event with tab_id and processed
			sql="UPDATE _events_tabs SET tab_id='%s', processed='1' WHERE id='%s'" % (new_tab_id, id)
			cursor.execute(sql)

			connTAB.commit()
			logger.messageInfo("TABS  | Event id="+str(id)+" processed ==> New Tab has been created: '" + name+"'")
			return True
		
################ Close Tab
		if type_id in [3]:
			
			sql="UPDATE _events_tabs SET processed='1' WHERE id="+str(id)
			cursor.execute(sql)

			#Set status of Tab to 'closed'.
			sql="UPDATE tabs SET timestamp_update='%s', status='closed' WHERE id='%s'" % (timestamp, tab_id)
			cursor.execute((sql))

			#Update Tab Members in LinkTable
			sql="UPDATE tabs_LT_users SET status='closed', timestamp_leave='%s' WHERE tab_id='%s'" % (timestamp, tab_id)
			cursor.execute((sql))
			
			connTAB.commit()
			logger.messageInfo("TABS  | Event id="+str(id)+" processed ==> Tab with id=" + str(tab_id) + " has been closed")
			return True

################ Join Tab
		if type_id in [4]:
			
			sql="UPDATE _events_tabs SET processed='1' WHERE id="+str(id)
			cursor.execute(sql)

			#timestamp_update of tab.
			sql="UPDATE tabs SET timestamp_update='%s' WHERE id='%s'" % (timestamp, tab_id)
			cursor.execute((sql))

			#Update Tab Members in LinkTable
			sql="SELECT * FROM tabs_LT_users WHERE tab_id = '%s' AND user_id='%s'" % (tab_id, user_id)
			cursor.execute(sql)
			rows=cursor.fetchall()
			#member is new to the tab
			if len(rows)==0:			
				sql="INSERT INTO tabs_LT_users (`tab_id`, `user_id`, `timestamp_join`) VALUES ('%s', '%s', '%s')" % (tab_id, user_id, timestamp)
				cursor.execute((sql))
			#member has already been registered to tab (joined/left before)
			else:
				#calculate seconds in memory
				for row in rows: #there is always only one row in rows, but we need this loop for 'fetchall' to work
					ts_join=row["timestamp_join"]
					ts_leave=row["timestamp_leave"]
					delta=ts_leave-ts_join
					delta=delta.seconds
					old_delta=row["seconds_in_memory"]
					total_delta=delta+old_delta
					logger.messageInfo('User was already member of this tab for: '+str(total_delta)+' seconds. Stored in memory')
					
				#update linktable
				sql="UPDATE tabs_LT_users SET status='%s', timestamp_join='%s', timestamp_leave='%s', seconds_in_memory='%s' WHERE tab_id = '%s' AND user_id='%s'" % ('active', timestamp, '', total_delta, tab_id, user_id)
				cursor.execute((sql))
			
			connTAB.commit()
			logger.messageInfo("TABS  | Event id="+str(id)+" processed ==> User (id="+str(user_id)+") has joined Tab (id=" + str(tab_id) + ").")
			return True

################ Exit Tab
		if type_id in [5]:
			
			sql="UPDATE _events_tabs SET processed='1' WHERE id="+str(id)
			cursor.execute(sql)

			#Update Tab Members in LinkTable
			sql="UPDATE tabs_LT_users SET status='closed', timestamp_leave='%s' WHERE tab_id='%s' AND user_id='%s'" % (timestamp, tab_id, user_id)
			cursor.execute((sql))
			
			#Update Timestamp
			sql="UPDATE tabs SET timestamp_update='%s' WHERE id='%s'" % (timestamp, tab_id)
			cursor.execute((sql))
			
			#If no more members are in the tab, create a TABS event for closing the TAB
			sql="SELECT user_id from tabs_LT_users where tab_id = '%s' AND status='%s'" % (tab_id, 'active')
			cursor.execute(sql)
			rows=cursor.fetchall()
			if len(rows)==0:
				#Creation of tab-close event (pure for graphical reasons)
				sql="INSERT INTO `_events_tabs` (`RAW_id`, `timestamp`, `type_id`, `user_id`, `tab_id`, `processed`) VALUES ('%s', '%s', '%s', '%s', '%s', '%s')" % (RAW_id, timestamp_delayed, '3', user_id, tab_id, '1')
				cursor.execute((sql)) 
				#Set status of Tab to 'closed'.
				sql="UPDATE tabs SET timestamp_update='%s', status='closed' WHERE id='%s'" % (timestamp, tab_id)
				cursor.execute((sql))
				
			
			connTAB.commit()
			logger.messageInfo("TABS  | Event id="+str(id)+" processed ==> User (id="+str(user_id)+") has left Tab (id=" + str(tab_id) + ").")
			if len(rows)==0:
				logger.messageInfo("TABS  | 0 Members left in Tab (id=" + str(tab_id) + ") ==> Tab has been closed.")
			return True
		

	except mdb.Error, e:
	  
		connTAB.rollback()
		logger.messageException(str("TABS  | MySQLdb error %d: %s" % (e.args[0],e.args[1])))
		return False

	except:
		connTAB.rollback()
		logger.messageException("TABS  | Exception during writing to database")
		return False

		
		

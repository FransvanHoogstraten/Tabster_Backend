import logging
from datetime import datetime


def messageInfo(messagetext):
	time=str(datetime.now())
	messagetext = time+" "+messagetext
	print messagetext
	logging.info(messagetext) 

def messageWarning(messagetext):
	time=str(datetime.now())
	messagetext = time+" "+messagetext
	print messagetext
	logging.warning(messagetext) 

def messageException(messagetext):
	time=str(datetime.now())
	messagetext = time+" "+messagetext
	print messagetext
	logging.exception(messagetext) 

def messageError(messagetext):
	time=str(datetime.now())
	messagetext = time+" "+messagetext
	print messagetext
	logging.error(messagetext)

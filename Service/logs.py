import logging
import os

logLevel = logging.INFO
logDir = "%s%sLogs" % (os.getcwd(), os.path.sep)
if not os.path.exists(logDir):
    os.makedirs(logDir)

def setupLogging(filename):
    logFile = "%s%s%s" % (logDir, os.path.sep, filename)
    logging.basicConfig(filename=logFile, filemode='a',
                        level=logLevel,
                        format='%(asctime)s :- %(message)s')
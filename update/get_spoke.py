#! /usr/bin/python
#
# Python script to do the regular updates of the local SPOKE data sources.
#
# This script is run out of /usr/local/etc/periodic/periodic.conf
# as user "sacsdb".	sacsdb's home directory is /home/socr/c/etc.
#
#

import getopt, paths, resource, sys, time
from datetime import timedelta
from string import replace
from time import localtime, asctime, sleep, mktime
from os import system,environ,chdir,mkdir,remove,rename
from os.path import exists,getsize,getmtime,basename
from shutil import copyfile

mailaddr = "sacsdb\@cgl.ucsf.edu"		 # mail is sent here
LOGDIR = "/databases/mol/logs/"
logfile = LOGDIR+"spoke_update.log"
DBDIR = "/databases/mol/spoke/"				# Must be absolute path with trailing /
tmp_versions = "/usr/tmp/spoke_versions.tmp"
BINDIR = "/usr/local/bin/"
# Should we use -N (timestamping)?
totalBytes = 0

# Move tmpdir to a larger partition
environ["TMPDIR"] = "/usr/tmp"

#
# Create an array with all the file names to be processed
#
# The format of the values is:
# (URL of file, Location of raw output, Location to move output, Update script, Subdirectory)
#
all_files = {
"chembl":
		(paths.ChEMBL_URL, paths.ChEMBL_Output, paths.ChEMBL_Database, "update_chembl.py", "chembl"),
"uniprot_chembl":
		(paths.UniprotChEMBL_URL_HUMAN,"", paths.UniprotChEMBLMapping_Human, "update_uniprot_chembl.py", "uniprot"),
"disease_ontology":
		(paths.DiseaseOntology_URL, "", paths.DiseaseOntologyOBO, "update_mesh.py", "DiseaseOntology"),
"entrez":
		(paths.EntrezGene_URL, "", paths.EntrezGene_CSV, "update_entrez_gene.py", "entrez"),
}

# Spread the data across several days
day_list = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
day_files = {
		'Monday': ["chembl"],
		'Tuesday': ["uniprot_chembl"],
		'Wednesday': ["disease_ontology"],
		'Thursday': ["entrez"],
		'Friday': [],
		'Saturday': [],
		'Sunday': []
}

def log(message):
		logtime = asctime(localtime())
		print "[%s] %s"%(logtime,message)
		lf = file(logfile,"a")
		print >>lf, "[%s] %s"%(logtime,message)
		lf.close()

def execute(command):
		command = "%s >> %s 2>&1"%(command,logfile)
		if debug:
				print "DEBUG: command = %s"%command
				return 0
		status = system(command)
		return int(status/256)

def set_limits():
		(soft,hard) = resource.getrlimit(resource.RLIMIT_DATA)
		resource.setrlimit(resource.RLIMIT_DATA,(hard,hard))
		(soft,hard) = resource.getrlimit(resource.RLIMIT_STACK)
		resource.setrlimit(resource.RLIMIT_STACK,(hard,hard))
		(soft,hard) = resource.getrlimit(resource.RLIMIT_FSIZE)
		resource.setrlimit(resource.RLIMIT_FSIZE,(hard,hard))

def get_file(db,subdir) :
		path = "%s%s"%(DBDIR,subdir)
		if not exists(path):
				mkdir(path)
		chdir("%s%s"%(DBDIR,subdir))
		if exists(basename(db)):
				if exists(basename(db)+".old"):
						remove(basename(db)+".old")
				rename(basename(db),basename(db)+".old")
		retVal = execute("wget -nv %s "%(db))
		return retVal

def untar(file):
		# Strip any wildcard replacement out first
		file = replace(file, "\\", "")
		if file.endswith(".tgz") or file.endswith(".tar") or file.endswith(".tar.gz"):
			return execute("tar zxf %s"%(file))

def move_file(raw, final):
	execute("mv %s %s"%(raw, final))

def process_file(infile):
	log("getting database %s"%infile)
	(url, raw, final, processor, subdir) = all_files[infile]
	if (get_file(url,subdir) != 0):
		log ("ERROR: get_spoke of %s failed"%infile)
		return
	else:
		log ("get_spoke of file %s done"%infile)
		untar(basename(url))
	if (raw != ""):
		# Move file to correct location
		move_file(raw, final)

		# Set the date
		execute("touch %s"%final)

		retVal = execute("/usr/local/projects/spoke/bin/%s"%processor)
		if (retVal != 0):
			log ("ERROR: get_spoke processor '%s' failed"%processor)

def process_day(dayArg):
		if dayArg == None:
				weekday = localtime().tm_wday
		else:
				weekday = dayArg

		fileList = []

		# If reprocess is set, we want to check for any previous missing files
		if reprocess:
				for i in range(0,weekday):
						for infile in day_files[day_list[i]]:
								(url, raw, final, processor, subdir) = all_files[infile]
								# Check the date of the final file
								try:
									mtime = getmtime(final)
								except OSError:
									# File probably doesn't exist -- reprocess
									process_file(infile)
								else:
									now = mktime(localtime())
									# If it's > 7 days old
									delta = timedelta(seconds=now) - timedelta(seconds=mtime)
									if (delta > timedelta(days=7)):
										log("Reprocessing %s"%infile)
										# Reprocess
										process_file(infile)
									else:
										log("Not reprocessing %s"%infile)

		day = day_list[weekday]
		fileList += day_files[day]

		log("WEEKLY UPDATE: Getting %s's data: %s"%(day, fileList))

		# part is the part # we start with
		# fileList is the list of files
		for infile in fileList:
			process_file(infile)

		return weekday

def usage():
				print "usage: %s [-d day] [-h]"%sys.argv[0]
				print "Arguments:"
				print "  -d day            force processing for 'day'"
				print "  -h                print this text"
				print "  -r                reprocess missing data"
				print "  -n                don't reprocess missing data"
				print


#
# Mainline
#
day = None
weekday = None
format = False
reprocess = None
debug = False

optlist, args = getopt.getopt(sys.argv[1:], 'nrhd:vx',['noreprocess','reprocess','help','day','verbose','debug'])
for o in optlist:
				(flag, value) = o
				if flag == '-d' or flag == '--day':
								day = value
								if not reprocess:
												reprocess = False
				elif flag == '-h' or flag == '--help':
								usage()
								sys.exit(0)
				elif flag == '-r' or flag == '--reprocess':
								reprocess = True
				elif flag == '-n' or flag == '--noreprocess':
								reprocess = False
				elif flag == '-x' or flag == '--debug':
								debug = True
				else:
								usage()
								sys.exit(1)

if reprocess == None:
		reprocess = True

weekday = None

if day:
		day = day.lower()
		if day.find('mon') == 0:
				weekday = 0
		elif day.find('tue') == 0:
				weekday = 1
		elif day.find('wed') == 0:
				weekday = 2
		elif day.find('thu') == 0:
				weekday = 3
		elif day.find('fri') == 0:
				weekday = 4
		elif day.find('sat') == 0:
				weekday = 5
		elif day.find('sun') == 0:
				weekday = 6


# Make sure we have appropriate limits
set_limits()

day = process_day(weekday)

# Unzip?
if day == 6 or format:
		# log("Processed %.1fMB of data"%(float(bytesProcessed)/(1024*1024)))
		log("WEEKLY UPDATE complete")

#!/usr/bin/python
import re,sys

if len(sys.argv) != 3:
	print
	print "Usage: %s inputfile outputfile"%sys.argv[0]
	print
	sys.exit(1)

inputfile = sys.argv[1]
outputfile = sys.argv[2]

fr = open(inputfile)
fw = open(outputfile, "w")

def filmAndYear(string):
	string = string.strip()

	"""
	found = re.search(r'^\".+\"',string)
	name = found.group() if found is not None else ""
	name = name[1:-1]
	"""	
	name = string[0:string.find('(')]
	name = name.replace('"', '').replace('"', '').strip()

	found = re.search(r'\(\d{4}\)',string)
	year = found.group() if found is not None else ""
	year = year[1:-1]

	found = re.search(r'(:? \{.+\})', string)
	extra = found.group() if found is not None else ""
	extra = extra.replace('{', '').replace('}', '').strip()

	found = re.search(r'(:?\([^\(^\)]+\)$)', string)
	role = found.group() if found is not None else ""

	role = role.replace('(', '').replace(')', '').strip()
	

	if role[1:-1] == year:
		role = ""

	return name,year,extra,role

directorfilm = ""
line = fr.readline()
directorcount = 0
while line!="":
	if line == "\n":
		linewritten = directorfilm.count("\n")
		directorcount += 1
		print str(directorcount) + " directors treated " + str(linewritten) + " written"
		fw.write(directorfilm)
		directorfilm = ""
		director = ""
		line = fr.readline()
		continue
	
	if directorfilm == "":
		director = line[0:line.find("\t")]
		filmstring = line[line.find("\t"):].strip()
		film,year,extra,role = filmAndYear(filmstring)
		directorfilm += '%s|%s|%s|%s|%s\n'%(director,film,year,extra,role)
		line = fr.readline()
		continue
	
	filmstring = line.strip();
	film,year,extra,role = filmAndYear(filmstring)
	directorfilm += '%s|%s|%s|%s|%s\n'%(director,film,year,extra,role)
	line = fr.readline()

fw.write(directorfilm)

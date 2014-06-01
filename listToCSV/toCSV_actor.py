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

	found = re.search(r'^\".+\"',string)
	name = found.group() if found is not None else ""
	name = name[1:-1]

	found = re.search(r'\(\d{4}\)',string)
	year = found.group() if found is not None else ""
	year = year[1:-1]

	found = re.search(r'(:?\{.+\})', string)
	extra = found.group() if found is not None else ""

	found = re.search(r'(:?\(.+\))', string)
	role = found.group() if found is not None else ""

	return name,year,extra,role

def filmYearCharacter(bigstring):
	bigstring = bigstring.strip()

	searchYear = re.search(r'\(\d{4}\)', bigstring)
	if searchYear is not None:
		year = searchYear.group()
		year = year[1:-1]
	else:
		year = ""

	searchCharacter = re.search(r'\[[a-zA-Z0-9 ]+\]', bigstring)
	if searchCharacter is not None:
		character = searchCharacter.group()
	else:
		character = ""

	film = bigstring[0:bigstring.find("(")].strip()

	return film,year,character
	


actorfilm = ""
line = fr.readline()
actorcount = 0
while line!="":
	if line == "\n":
		linewritten = actorfilm.count("\n")
		actorcount += 1
		print str(actorcount) + " actors treated " + str(linewritten) + " written"
		fw.write(actorfilm)
		actorfilm = ""
		actor = ""
		line = fr.readline()
		continue
	
	if actorfilm == "":
		actor = line[0:line.find("\t")]
		filmstring = line[line.find("\t"):].strip()
		film,year,character = filmYearCharacter(filmstring)
		actorfilm += actor + "|" + film + "|" + year + "|" + character + "\n"
		line = fr.readline()
		continue
	
	filmstring = line.strip();
	film,year,character = filmYearCharacter(filmstring)
	actorfilm += actor + "|" + film + "|" + year + "|" + character + "\n"
	line = fr.readline()

fw.write(actorfilm)

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

def filmAndYearAndMedia(string):
	string = string.strip()

	found = re.search(r'^\".+\"',string)
	name = found.group() if found is not None else ""
	name = name[1:-1]

	found = re.search(r'\(\d{4}\)',string)
	year = found.group() if found is not None else ""
	year = year[1:-1]

	name = name.replace('"', '')

	found = re.search(r'\([^0-9]\)',string)
	media = found.group() if found is not None else ""
	media = year[1:-1]

	return name,year,media

data = ""
line = fr.readline()
movie_count = 0
writebuffer = ""
writeat = 1000
while line!="":
	if writeat == 0:
		fw.write(writebuffer)
		writebuffer = ""
		writeat = 1000
		print str(movie_count) + " movies written"

		#print str(actorcount) + " actors treated " + str(linewritten) + " written"
		#fw.write(actorfilm)
	film,year,media = filmAndYearAndMedia(line)
	data = film + "|" + year + "|" + media + "\n"
	writebuffer += data
	writeat = writeat - 1
	movie_count += 1
	line = fr.readline()
	continue	

fw.write(writebuffer)

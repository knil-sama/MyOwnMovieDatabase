#!/usr/bin/python

import re
import sys


def extractNameSubNameYearCountry(string):
	
	found = re.search(r'^\".+\"|[^\(]+',string)
	name = found.group() if found is not None else ""
	name = name[1:-1]

	found = re.search(r'\(\d{4}\)',string)
	year = found.group() if found is not None else ""
	year = year[1:-1]

	found = re.search(r'\t[A-Za-z ]+\n', string)
	country = found.group() if found is not None else ""
	country = country.strip()

	found = re.search(r'\{[^\{^\}]+\}',string) 
	subname = found.group() if found is not None else ""
	subname = subname[1:-1]

	return name,subname,year,country


def main():
	
	if len(sys.argv) != 3:
		print
		print "Usage: %s inputfile outputfile"%sys.argv[0]
		print
		sys.exit(1)
	
	inputfile = sys.argv[1]
	outputfile = sys.argv[2]

	fr = open(inputfile, "r")
	fw = open(outputfile, "w")

	line = fr.readline()
	
	nextWriteIn = 50 
	writeBuffer = ""
	written = 0
	while line!="":
		if nextWriteIn == 0:
			fw.write(writeBuffer)
			nextWriteIn = 50
			writeBuffer = ""
			written += 50
			print "%d writen"%written

		name,subname,year,country = extractNameSubNameYearCountry(line)

		writeBuffer += "%s|%s|%s|%s\n"%(name,subname,year,country)
		nextWriteIn -= 1
		

		line = fr.readline()
	fw.write(writeBuffer)

if __name__ == "__main__":
	main()

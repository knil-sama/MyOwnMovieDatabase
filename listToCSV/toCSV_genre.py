#!/usr/bin/python
import re
import sys

def extract(string):
	string = string.strip()

	name = string[0:string.find('(')].strip()[1:-1]

	found = re.search(r'\(\d{4}\)',string)
	year = found.group() if found is not None else ""
	year = year[1:-1]

	genre = string[string.rfind("\t", 0, len(string))+1:].strip()

	return name,year,genre

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

		name,year,genre = extract(line)

		writeBuffer += "%s|%s|%s\n"%(name,year,genre)
		nextWriteIn -= 1
		

		line = fr.readline()
	fw.write(writeBuffer)

if __name__ == "__main__":
	main()

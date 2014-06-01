#!/usr/bin/python

import re
import sys


def extract(string):
	
	name = string[0:string.find("(")]
	if len(name) > 0 and name[0] == '"':
		name = name.replace('"', '')
	name = name.strip()

	yi = string.find(")")
	found = re.search(r'\(\d{4}\)',string[0:string.find(")")+1])

	year = found.group()[1:-1] if found is not None else ""

	if string.find("{") > -1 and string.find("}") > -1:
		episode = string[string.find("{"):string.find("}")+1][1:-1]
		yi = string.find("}")+1
	else:
		yi = string.find("\t")
		episode = ""


	string = string[yi:]

	distributor = string[0:string.find("[")].strip()

	countrycode = string[string.find("[")+1:string.find("]")]

	yrm = string[string.find("]")+1:]
	
	y = yrm.find(")")
	year_dist = yrm[0:y] 
	yrm = yrm[y+1:]
	year_dist = year_dist.replace("(", "").strip()

	y = yrm.find(")")
	region = yrm[:y].strip() 
	yrm = yrm[y+1:]
	region = region.replace("(", "").strip()

	y = yrm.find(")")
	media = yrm[0:y].strip()
	media = media.replace("(", "").strip()


	#print name,year,episode,distributor,countrycode,year_dist,region,media
	return name,year,episode,distributor,countrycode,year_dist,region,media
	


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
			#print writeBuffer
			nextWriteIn = 50
			writeBuffer = ""
			written += 50
			print "%d writen"%written

		name,year,episode,distributor,countrycode,year_dist,region,media =  extract(line)

		writeBuffer += "%s|%s|%s|%s|%s|%s|%s|%s\n"%(name,year,episode,distributor,countrycode,year_dist,region,media)
		nextWriteIn -= 1
		

		line = fr.readline()
	#fw.write(writeBuffer)

if __name__ == "__main__":
	main()

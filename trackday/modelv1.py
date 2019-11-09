import json
from pseudodb import *

def parse_model(raw_model):
	year, make, model = None, None, None
	foundYear, foundMake = False, False
	duplicateYear = None
	idxYear, idxMake = None, None
	modelWords = raw_model.split(" ")
	
	for idx, word in enumerate(modelWords):
	        if len(word) == 4 and word.isnumeric():
	                if foundYear:
				if year == int(word):
					duplicateYear = idx
	                        continue
	                
	                year = int(word)
	                foundYear = True
	                idxYear = idx
	
	        elif word.lower() in carBrands:
	                make = word.lower()     
	                # convert from alias to actual car make
	                if make in aliasDict:
	                        make = aliasDict[make]
	                foundMake = True        
	                idxMake = idx   
	
	# if we cannot parse year, skip car
	if not foundYear:
	        return  #continue (was previous in loop)
	if duplicateYear is not None:
	        del modelWords[duplicateYear]
		if idxMake > duplicateYear:
			idxMake -= 1
	
	if len(modelWords) == 2 and foundMake:
		return year, make, None  
	
	if not foundMake:		
		# try checking two words before and after year index 
	        idx_len = len(modelWords)-1
	        if yearIdx == 0:
	                if idxYear + 3 > idx_len:
	                        pass


if __name__ == "__main__":
	with open("parse_models.txt", "r+") as file:
		for raw in file:
			parse_model(raw_model)

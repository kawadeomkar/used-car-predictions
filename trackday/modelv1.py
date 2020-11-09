import json
from pseudodb import *

def parse_model(model):
	foundYear = False
	foundMake = False
	makeDouble = False
	idxMake = None
	modelWords = model.strip().split(" ")
	year = None
	make = None
	ret = {}
	
	def checkAlias(makeAlias):
	        return aliasDict[makeAlias] if makeAlias in aliasDict else makeAlias
	
	# assuming year is the first word (99%+ case)
	if len(modelWords[0]) == 4 and modelWords[0].isnumeric():
		foundYear = True
		year = int(modelWords[0])
		ret['year'] = year
		modelWords.remove(modelWords[0])
	# skip otherwise
	else:
		print(modelWords)
		return None
	# if remaining word is a make
	if len(modelWords) == 1 and modelWords[0].lower() in car_brands:
		ret['make'] = checkAlias(modelWords[0].lower())
		return ret		

	for idx, word in enumerate(modelWords):
		if word.lower() in car_brands:
			make = word.lower()
			if idx+1 < len(modelWords):
				if make + modelWords[idx+1].lower() in car_brands:
					idxMake = idx
					makeDouble = True
			ret['make'] = checkAlias(make)
			foundMake = True
			idxMake = idx
			break
		elif idx+1 < len(modelWords) and word.lower() + modelWords[idx+1].lower() in car_brands:
			ret['make'] = checkAlias(word.lower() + modelWords[idx+1].lower())
			foundMake = True
			makeDouble = True
			idxMake = idx
			break
	if makeDouble:
	        del modelWords[idx]
	        del modelWords[idx]
	elif foundMake:
	        del modelWords[idx]
	else:
		print(modelWords)
		return None
	
	if modelWords:
		ret['model'] = " ".join(modelWords)
		
	return	ret 




# under construction
def parse_test1(raw_model):
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

		elif word.lower() in car_brands:
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
	pass_counter = 0
	none_counter = 0
	with open("parse_models.txt", "r+") as file:
		for raw in file:
			output = parse_model(raw.strip())
			if output is None:
				none_counter += 1
				print(raw)
			else:
				pass_counter += 1

	print("PASSED: ", pass_counter)
	print("FAILED: ", none_counter)
	print("GRADE: ", float(pass_counter/(pass_counter+none_counter)))

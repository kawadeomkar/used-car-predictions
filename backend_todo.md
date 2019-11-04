

### BACKEND & DATA

SCRAPER TODO
1.	City Scraper Deployment to lambda is failing because of dependencies with lxml, need to figure out a way to correctly import package.
	Possible solutions: Compiling dependencies in EC2 virtual environment then FTP for deployment, Using docker of aws ec2 (AMI 2018.03) to yum install dependencies
2.	Standardize data format when producing message to kafka. 
4.	Create an algorithm to extract year + model + trim
3.	Refactor vehicleScraper code (extra unused variables at bottom)
TODO:
1.	Automate craigslistFilter to run on a daily basis, kafka for data streaming, store in S3, using Airflow for scheduling (satvik)
2.	Stream website traffic and logging using kafka, store in S3 (omkar)
3.	Create machine learning model (satvik & omkar)
4.	(something with account creation processing with spark from DB to redshift TBD)


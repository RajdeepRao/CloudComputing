import os 
import sys
import numpy as np

spark_home = os.environ.get('SPARK_HOME', None)					# Code to start spark using python
if not spark_home:								
    raise ValueError('SPARK_HOME environment variable is not set')		
sys.path.insert(0, os.path.join(spark_home, 'python'))				
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.9-src.zip'))
execfile(os.path.join(spark_home, 'python/pyspark/shell.py'))

def processA(line): # This function processes a line to sparate the x and y component and returns the product of x and it's transpose
	x = line.split(",")[1:]
	x = [1]+x # Appending the 1 for the intercept value
	x = np.asmatrix(x).astype(float).transpose() # Since we need a column mantrix here
	xT = np.transpose(x) # Calculating it's transpose
	return x*xT # The product that we need

def processb(line): # This function processes a line to sparate the x and y component and returns the product of x and it's corresponding y
	l = line.split(",")
	yi = l[0]
	xi = l[1:]
	xi = [1]+xi
	xi = np.asmatrix(xi).astype(float).transpose() # Need a column matrix again
	yi = np.asmatrix(yi).astype(float) 
	return xi*yi  

path = sys.argv[1]
print "file://"+path
fileContents = sc.textFile(path)

dataA = fileContents.map(lambda line: ("keyA",processA(line))) \
		.reduceByKey(lambda v1,v2: v1+v2) #First Map Reduce task to calculate the first term:  Summation(X*X transpose)
A=dataA.collect()

datab = fileContents.map(lambda line: ("keyB",processb(line))) \
		.reduceByKey(lambda v1,v2: v1+v2) #Second Map Reduce task to calculate the second term: Summation(X*Y)
b=datab.collect()

keyb, valb = zip(*b) # Extracting the tuple from the list for b
keyA, valA = zip(*A) # Extracting the tuple from the list for A
beta = np.linalg.inv(valA[0])*valb[0] # Calculating the values of Beta and storing it. Using the first value of the tuple hence tup[0]

for coeff in beta:
	print coeff #Printing coeffs
sc.stop()


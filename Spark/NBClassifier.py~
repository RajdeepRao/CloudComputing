import os 
import sys
import numpy as np
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from random import shuffle

spark_home = os.environ.get('SPARK_HOME', None)				# Code to start spark using python
if not spark_home:								
    raise ValueError('SPARK_HOME environment variable is not set')		
sys.path.insert(0, os.path.join(spark_home, 'python'))				
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.9-src.zip'))
execfile(os.path.join(spark_home, 'python/pyspark/shell.py'))

SOC_NAMES = {} 								# Data Structure that contains all the categories of the SOC_NAME param
JOB_TITLES = {} 							# Data Structure that contains all the categories of the JOB_TITLE param
STATES = {} 								# Data Structure that contains all the categories of the STATE param
POSITIVES = ["CERTIFIED", "CERTIFIED-WITHDRAWN"]			# Data Structure that contains all the Poitive classes
NEGATIVES = ["DENIED"]							# Data Structure that contains all the Negative classes
LABELS = []								# Data Structure that contains all the labels
data = []								# Data Structure that contains all the generated Row Vectors

def prepVectorCreation(line):						# A function to populate the above data structures that help in prob computation
	x = line.split(",")
	if x[2] not in SOC_NAMES:						
		SOC_NAMES[x[2]] = len(SOC_NAMES)
	if x[8] not in STATES:
		STATES[x[8]] = len(STATES)
	
def createVector(line):							# A function that does the actual creation of the vectors 
	x = line.split(",")
	status = 0
	if x[0] in POSITIVES:
		status=1						# Target values being captured to populate the row vector
	else:
		status=0
	soc = [0.0]*len(SOC_NAMES)
	soc[SOC_NAMES[x[2]]] = 1.0
	val = float(x[5])
	val = 0 if val<100000 else (1 if val<180000 else 2)		# Setting slabs on "income" so that we can categorize continuous data 
	sal = [0]*3
	sal[val] = 1.0
	states = [0.0]*len(STATES)
	states[STATES[x[8]]] = 1.0
	features = sal + soc + states
 	vector = LabeledPoint(status, features)				# Generating a row Vector using the calculated features and Target Value
	data.append(vector)						# Append to the data set to get a processed dataset with row vectors

path = sys.argv[1]
print "file://"+path
inputFile = sc.textFile(path)
inputFile = inputFile.filter(lambda l: not l.startswith("CASE_STATUS")) # Removing the header to get a correct count of categories needed
fileContents = inputFile.collect()					# Converting from RDD to List because we need an Iterable to process this data
shuffle(fileContents)							# Shuffle up the data to get a diverse selection
temp_file_contents = (fileContents[:40000])				# Due to PC restrictions and unavailability of the cluster, a subset is chosen

for i in temp_file_contents:						# Iterate over the data to populate the data structures discussed above	
	prepVectorCreation(i)
for i in temp_file_contents:						# Iterate over the data to create vectors using the populated datastructures	
	createVector(i)

data = sc.parallelize(data)						# Create an RDD to Pass into the Library after splitting into test & train
training, test = data.randomSplit([0.7,0.3])

model = NaiveBayes.train(training,1.0)					# Create a model for the training dataset

prediction = test.map(lambda p: (model.predict(p.features), p.label))	# Generate predictions for each test vector and compare with target values
accuracy_library = float(prediction.filter(lambda pl: pl[0]==pl[1]).count())/float(test.count())

print accuracy_library


############################################################### TRAINING MY MODEL ######################################################################

vect_size = len(SOC_NAMES) + len(STATES) + 3
tempop = training.map(lambda l: ("YES",1) if(l.label==1.0) else ("NO",1))	
tempop = tempop.reduceByKey(lambda v1,v2:v1+v2)				# Count total instances of POSITIVES (Yes) and NEGATIVES (No) in the dataset
op = tempop.collect()
total = training.count()						# Store total number of training instances

prob_total, vals = zip(*op)
total_yes_count = vals[0]
total_no_count = vals[1]
prob_yes = float(total_yes_count)/float(total)				# P(Yes) and P(No) are being computed here
prob_no = float(total_no_count)/float(total)

feature_yes = []							# Datastructure to store P(feature | Yes)
feature_no = []								# Datastructure to store P(feature | No)
m = 3 									# For the m-estimate approach of Naive Bayes
alpha = 1								# For Multinomial NB with 'alpha' to make it smooth

for i in range(0,vect_size):						# Iterating over each feature to get No of occurances classified 'Yes' & 'No'
	dep_prob= training.map(lambda l:("YES",1) if(l.label==1 and l.features[i]==1) else(("NO",1) if(l.label==0 and l.features[i]==1) else ("NEITHER",1)))
	dep_prob = dep_prob.reduceByKey(lambda v1,v2:v1+v2)
	dep_prob = dep_prob.collect()
	dep_prob = sorted(dep_prob, key=lambda tup:tup[0], reverse=True)# Gotta sort it to get a count of 'Yes', 'No' and 'Neither' in this order
	flag_yes = 0
	flag_no = 0
	for i in dep_prob:
		if i[0]=="YES":
			feature_yes.append(i[1])			# Store No Ocuurances this feature is classified 'Yes'
			flag_yes = 1
		elif i[0]=="NO":
			feature_no.append(i[1])				# Store No Ocuurances this feature is classified 'No'
			flag_no = 1
		if i[0]=="NEITHER":					# If either of them was 0, take care of those here based on flag value
			if flag_yes == 0:
				feature_yes.append(0)
			if flag_no == 0:
				feature_no.append(0)


for i in range(0,vect_size):
	#feature_yes[i] = (feature_yes[i] + m*0.5)/(total_yes_count+m)	# This is the calculation for the m-estimate approach
	#feature_no[i] = (feature_no[i] + m*0.5)/(total_no_count+m)
	feature_yes[i] = float(feature_yes[i] + alpha)/float(total_yes_count+(2*alpha)) # Calculation for the NB using alpha approach
	feature_no[i] = float(feature_no[i] + alpha)/float(total_no_count+(2*alpha))


################################################################ TESTING MY MODEL ######################################################################

predicted_labels = []						# Datastructure to store values predicted by my Model
actual_labels = []						# Datastructure to store actual labels
test = test.collect()

for test_vector in test:
	yes = prob_yes;
	no = prob_no;
	actual_labels.append(test_vector.label)
	for i in range(0,len(test_vector.features)):			
		if test_vector.features[i]==1.0:
			yes*=feature_yes[i]			# Calculate product of probabilities for all features of Test Vector if classified 'Yes'
	for i in range(0,len(test_vector.features)):
		if test_vector.features[i]==1.0:		# Calculate product of probabilities for all features of Test Vector if classified 'No'
			no*=feature_no[i]

	if yes>=no:						# Check which probability is higher and classify it 
		predicted_labels.append(1.0)
	else:
		predicted_labels.append(0.0)

accuracy_stock = 0.0
for i in range(0,len(predicted_labels)):
	if predicted_labels[i]==actual_labels[i]:
		accuracy_stock = accuracy_stock+1
accuracy_stock = float(accuracy_stock)/float(len(predicted_labels))	# Calculate the accuracy of my Model

print "--------------------------------------AND FINALLY------------------------------------------"
print "STOCK ACCURACY = ", accuracy_stock
print "LIBRARY ACCURACY = ", accuracy_library
print "--------------------------------------SIGNING OFF------------------------------------------"

sc.stop()

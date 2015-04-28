#!/usr/bin/env /Applications/Orange.app/Contents/MacOS/python 

import Orange
print("version: ");
print("version: ",Orange.version.version);


#Load training test data
train = Orange.data.Table("genestrain");
print train.domain.class_var;

#Self validate
#bayes = Orange.classification.bayes.NaiveLearner();
#res = Orange.evaluation.testing.cross_validation([bayes], train, folds=5);
#print "Accuracy: %.2f" % Orange.evaluation.scoring.CA(res)[0];
#print "AUC:      %.2f" % Orange.evaluation.scoring.AUC(res)[0];

#Load test data.
blind = Orange.data.Table("genesblind");  #learner

#Run Learner on training data to construct Classifier (missing values are handled here).
classifier = Orange.classification.bayes.NaiveLearner(train);
target = 1;
print "Probabilities for %s:" % train.domain.class_var.values[target]

for d in blind:
    ps = classifier(d, Orange.classification.Classifier.GetProbabilities);
    print "";
    print "Next entry";
    print "";
    if (ps[1] > 0.8):
	print " %s" % train.domain.class_var.values[1];
    #print "%5.3f" % ps[1];
    if (ps[2] > 0.8):  
	print " %s" % train.domain.class_var.values[2];
    #print "%5.3f" % ps[2];
    if (ps[3] > 0.8):
	 print " %s" % train.domain.class_var.values[3];
    #print "%5.3f" % ps[3];
    if (ps[4] > 0.8):
	 print " %s" % train.domain.class_var.values[4];
    #print "%5.3f" % ps[4];
    if (ps[0] > 0.8):
	 print " %s" % train.domain.class_var.values[0];
    #print "%5.3f" % ps[0];




#learner = Orange.classification.bayes.NaiveLearner();
#classifier = bayes(train)
#classifier(blind[0])
#blind[0].get_class();

print("finished");
#print train.domain.class_var


#(Optional) Cross-validate training data to ensure high accuracy.
#For each test datum
#-- For each ethnicity class ...
#----  Apply Classifier and return Probabilities per ethnicity class per test datum.
#-- Report highest Probability ethnicity class per test datum.
#Convert ethnicities into numeric values.


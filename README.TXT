LAUNCH
yarn jar countBytes.jar /training/hadoop/hw3/input /training/hadoop/hw3/output

READ RESULT
hadoop fs -libjars countBytes.jar -text /training/hadoop/hw3/output/part-r-00000
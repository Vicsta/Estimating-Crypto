---Starting the web server
./sbt run

We are currently displaying 4 currencies that have been estimated using linear regression. We have taken the data from our spark jobs and graphed it using d3, to allow for additional exploration.


---ETL
In the etl folder we have the various steps the data has gone through.

1. clean - Our clean script buckets the data into 1 minute windows calculating a weighted price for this time bucket. We then calculate forward/backward moving averages to calculate a forwardDelta feature. All of the data is written out to hdfs for use in later steps.

2. profile - Our scripts for profiling the data. We have run these scripts to have a better idea of the schema and range of values.

3. merge - We merge all the currencies into 1 dataframe, grouping the currencies by the bucket. We forward fill any missing data, if 1 currency had trades in that time bucket, but others did not. We drop any trades that are outside the scope of the timeframe all currencies have existing trades in. This data is then saved for later use in training.

4. train - The training data is applied to each currency and machine learning model we are testing. This is the cartesian product of all models with all currencies(N x M). For each of these pairs we fit the training data onto the model, then save that model for use later.

5. results - For each of the model/currency pairs, we load the model from disk then test the model out by applying a prediction to our test data. We then massage the results into a file with bucket, actual price, and predicted price. We had done analysis on these files calculating Mean Error, Mean Square Error, and plotting the actual prices versus the predicted prices. The output from these step is used in our web application. Though the  web  application could just as easily load the Model and produce calculations in realtime.

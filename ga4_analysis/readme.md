
# Hotdog Analysis

The two files hotodog_extract.sql and hotdog_analysis.ipynb are both for the analysis to try and diagnose metrics most correlated to engagement. The SQL file is for bigquery to extract the feature data from the GA4 schema for the DTA site. The jupyter notebooks file is for the data preprocessing and for the visualisation as well as the machine learning model to find the metrics for engagement.

## Feature Selection

The features were essentially selected in the SQL code. Any adjustments in the features should be done on the SQL code.
Features selected can be split into 2 categories:

1. User Metrics
2. Event Metrics

### User Metrics

The aggragation takes the most common category (mode) for each feature per user. For example, if a user visited with their mobile phone 60% of the time, they will count as a mobile user.
User metrics include the following:
* device_category 	
* browser 	
* country 	
* traffic_medium 	
* traffic_source

### Event Metrics

The aggragation counts the number of occurances of these events per user.
Event metrics include the following:
* count_link_click 	
* count_scroll 	
* count_first_visit 	
* count_click 	
* count_pageview 	
* count_external_click 	
* count_file_download 	
* count_user_engagement 	

## The label - the thing we are trying to predict

I chose to take the sum of all the session_engaged metrics (which may be 1 or 0 depending on whether they were engaged or not respectively). This number is then divided by the count of all the session_engaged metrics. This is because there may be multiple session_engaged metrics per session and therefore also per user.

To try this in terms of a classification problem, I also tried using a threshold of 60% engaged to be 1 and less than that to be 0. I did not use 50% to make the dataset more balanced. I tried this so that I could see some other algorithms and how well they predicted an engaged user.

# The models that we tried

To be honest I tried a bunch because I wanted to see if I could get the shapley calues working. 

Classification models tried include:
* Decision tree
* Random Forest
* Logistic Regression - this one is easiest to find the coefficients for so I ended up using these for sprint review

Regression models tried include:
* SGDRegressor
* BayesianRidge
* LassoLars
* LinearRegression
* ARDRegression
* PassiveAggressiveRegressor
* TheilSenRegressor
* LinearRegression

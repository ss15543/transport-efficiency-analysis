This repository is for transport efficiency analysis condusted on the motor vehicle collision data for New York City.  
 
# Data Ingestion
 Name: Motor Vehicle Collisions-Crashes
 
 Source: NYC Open Data
 
 URL: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95 
 
 Size: 422.4 MB
 
 Description: The Motor Vehicle Collisions-Crashes dataset provides details about a police reported motor vehicle collisions occurred in NYC. This dataset was collected originally by the Police Department (NYPD) using their Finest Online Records Management System (FORMS) to conduct traffic safety analyses. This dataset is public, updated daily, and has records from 2012 - present. There are 1.98M rows and 29 columns in this dataset.
 
  Key Features: Crash date, location, geolocation (latitude, longitude), number of persons killed or injured, contributing factor, collision id, vehicle type etc.
  
 Data Cleaning:
The entire data cleaning and profiling was done using two mapreduce programs ‘Delimiter and Subset’. 

 1. First step was to change the delimiter to ‘%%%’ from ‘,’ to deal with Location values such as "(40.8058254, -73.9092546)”. This way remaining logic could be based on splitting fields based on ‘%%%’ rather than comma-separated values.

 2. Second step was to create a calculated field Number of Vehicles Involved which calculates the total number of vehicles involved in a crash/collision. The logic here was that if there is a value in columns Vehicle_Type_code_1, Vehicle_Type_code_2, Vehicle_Type_code_3, Vehicle_Type_code_4, Vehicle_Type_code_5, add +1 for each and calculate the total.
    
 3. Third step was to deal with null values. These steps were taking for respective features:
  - Borough : Value UNKNOWN is created to replace null and empty values.
  - ZipCode/Latitude/Longitude: Null fields are replaced by ‘0000’.
    
 4. Final step was to select the features that will be used in this analysis and remove any duplicate records. T

After Data cleaning, the final file has 17 fields and the size of the file is 778 KB. Shell Commands used are added in the log file.

# Data Analysis and Final Recommendation
Please refer to analysis.sql file for details on the queries run for this analysis. Of all the boroughs, Brooklyn had the highest number of pedestrian/cyclist/motorist injuries and fatalities. Here are some of my recommendations to minimize this issue:

 1. Increase awareness and enforcement of traffic laws and safety measures in Brooklyn to reduce the number of collisions, injuries, and fatalities.
 2. Develop targeted educational campaigns for drivers to address the top causes of collisions, such as driver inattention/distraction, unsafe speed, and alcohol involvement.
 3. Improve road infrastructure and traffic control measures in Brooklyn to reduce the risk of collisions, such as increasing the number of traffic lights and stop  signs, improving road signage, and reducing speed limits in high-risk areas.
 4. Increase police presence and enforcement efforts in Brooklyn to deter reckless driving behaviors, such as following too closely and traffic control disregard.
 5. Conduct ongoing data analysis and monitoring to identify emerging trends and hotspots for collisions and implement targeted interventions to address them.


use hive.ss15543_nyu_edu;

# Which borough has the most number of Collisions? 
select borough, count(*) AS total
from collisions
group by borough
order by total desc;

# Calculate the total number of collisions in a particular ZipCode and identify the top 10 zip codes. 
select zipcode, count(*) AS total
from collisions
group by zipcode
order by total desc;

# Are the top 10 zip codes in ‘Brooklyn’ ? 
select zipcode, count(*) AS total
from collisions
where borough = 'BROOKLYN'
group by zipcode
order by total desc;

# Calculate the total number of collisions in all the years available in the dataset. Identity top 5 years. 
select substr(CRASHDATE, LENGTH(CRASHDATE) -1, 2) AS year, 
		count(*) AS total
from collisions
group by substr(CRASHDATE, LENGTH(CRASHDATE) -1, 2)
order by total desc;

# Calculate the total number of collisions in all the years available in the dataset. Identify the last 5 years. 
select substr(CRASHDATE, LENGTH(CRASHDATE) -1, 2) AS year, 
		count(*) AS total
from collisions
group by substr(CRASHDATE, LENGTH(CRASHDATE) -1, 2)
order by total ;

# top 10 reasons for vehicle collisions
select CONTRIBUTINGFACTOR, count(*) AS total
from collisions
group by CONTRIBUTINGFACTOR
order by total desc
limit 11;

#Calculate number of persons injured in collisions in each borough.
select borough, SUM(numberofpersonsinjured) AS total
from collisions
group by borough
order by total desc; 

#Calculate number of persons killed in collisions in each borough.
select borough, SUM(numberofpersonskilled) AS total
from collisions
group by borough
order by total desc; 

#Calculate the number of collisions based on the number of vehicles involved.
select numberofvehiclesinvolved, count(*) AS total
from collisions
group by numberofvehiclesinvolved
order by total desc;

#Calculate the proportion of numberofpersonskilled to numberofpersonsinjured in every borough. 
select borough,
		(CAST(SUM(numberofpersonskilled) AS DOUBLE)/NULLIF(CAST(SUM(numberofpersonsinjured)AS DOUBLE),0)) AS proportion
from collisions 
group by borough
order by proportion desc;




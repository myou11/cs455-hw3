# Utilizing Hadoop MapReduce to Analyze Airline Data

##### CS455 - Distributed Systems - ASG2

##### Maxwell You

## Program Overview
I have written some MapReduce jobs to answer the following questions with this [dataset](https://www.transtats.bts.gov/Fields.asp?Table_ID=236):

1. What is the best time-of-the-day/day-of-week/time-of-year to fly to minimize delays?
2. What is the worst time-of-the-day / day-of-week/time-of-year to fly to minimize delays?
3. What are the major hubs (busiest airports) in continental U.S.? Please list the top 10. Has
there been a change over the 21-year period covered by this dataset?
4. Which carriers have the most delays? You should report on the total number of delayed
flights and also the total number of minutes that were lost to delays. Which carrier has the
highest average delay?
5. Do older planes cause more delays? Contrast their on-time performance with newer planes.
Planes that are more than 20 years will be considered old.
6. Which cities experience the most weather-related delays? Please list the top 10.
7. Is there a shift in airline traffic during different months of the year?

My jobs are designed with the goal of reducing network traffic, therefore, I have answered the seven questions
with four jobs. There are further improvements I could incorporate into this project to reduce
the number of jobs, such as the use of Distributed Cache for tying in related data from multiple files.

For question 7, I thought it would be a cool idea to see if the airline traffic to a particular state increased during different months of the year. I wrote a MapReduce job to find the traffic to the top 100 airports for each month. I then parsed this data with python and added it to the airports.csv file given. This new file with the state and counts of traffic is mapped using (Plot.ly)[https://plot.ly/feed/].

A timelapse of the traffic for these 12 months can be seen (here)[http://www.cs.colostate.edu/~myou/cs455/mapGif].

## File Descriptions (by grouping):
### **GetInput**
  This is the core of my MapReduce jobs because it collects and prepares the input for the subsequent jobs.
  - **GetInputMapper**: Retrieves specific columns from the main dataset based on the needs of each question.
  For example, for question one and two, it gets the delay from the DepDelay column, and appends a "_1" to the end, to use as a value. The "_1" is so that we can keep track of how many delays has happened so we can average the total delay at the end. 
  For a key, the question number and M, D, or H (representing month, day, or year) is prepended to the actual time value we are
  mapping. For example, if the Mapper is processing a row with month 2 and delay 32, the key-value pair would be:
  <q1q2:M_2, 32_1>.
  The Mapper does a similar process to this for every question. Each question is partitioned into its own Reducer.
  - **GetInputCombiner**: Performs nearly identical to the Reducer, but with a few tweaks since it is the Combiner and not
  writing real outputs like the Reducer does.
  - **GetInputReducer**: Parses the key-value pairs and performs different operations depending on the question number.
  - **GetInputJob**: Configuration for the all the jobs. Chains together the subsequent jobs as well.
  - **GetAirportStateMapper**: Used to parse the airports.csv supplementary data.
  - **GetPlaneDataMapper**: Used to parse the plane-data.csv supplementary data.
### **Delay**
  This job takes care of outputs from the GetInput job that deal with questions 1, 2, 4, and 5.
  - **DelayMapper**: Passes on the keys to the DelayReducer. Only does special processing for question 1 and 2.
  - **DelayReducer**: Processes the data for questions 1, 2, 4, and 5 to find answers.
### **TopTenAirports**
  - **TopTenAirportsMapper**: Passes on the keys to the TopTenAirportsReducer. No special processing is done.
  - **TopTenAirportsReducer**: Processes the data for questions 3 and 6 to find answers.
### **BusiestAirports**
  - **BusiestAirportsMapper**: Passes on the keys to the BusiestAirportsReducer. No special processing is done.
  - **BusiestAirportsPartitioner**: Partitions each key (which will be a month) to 12 different Reducers.
  - **BusiestAirportsReducer**: Processes the data for question 7 and finds the busiest airports per month.

### **Disclaimers**
I am not claiming the answers produced by my program are correct as this was a project open to much interpretation of what the questions were asking for.


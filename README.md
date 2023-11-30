# ETL_REPO  
  
# Appuccino
Appuccino project team repo
## Team members:
Erica L., Rachel A., Muhammad N., Thomasin L., Georgia D.
## Links:
- Trello: https://trello.com/b/GaKj4jZt/appuccino
- Inception jamboard: https://jamboard.google.com/d/16efxfSm80piCYS-GOJfjKnxY0MNqt1Yjo25Tp93nJOI/viewer
- Team's project approach: https://miro.com/app/board/uXjVM1X7YYk=/
- Schema approach: https://miro.com/app/board/uXjVM0Mk-jU=/
## Run ETL pipeline:
Option 1:
- `git pull` on the `main` branch to activate the ETL pipeline

Option 2:

Main application located in `sprint3_lambda`\ `src` folder
- open a Git Bash integrated terminal in the `sprint3_lambda` folder
- then run this command `./deploy-ci.sh`

## Testing:
Unit Testing:
- on folders: `test_extract_csv.py`, `test_remove_sensitive_data.py`

Integration Testing:
- on folders: `test_unit.py`
## Tools used:
- `psycopg2-binary`
- `python-dotenv`
- `jmespath`
- `uuid`
## Progress track
### Week 7
#### 18/07/2023, Tue
- assigned name to our team
- built an elevator pitch
- agreed importance
- agreed scope (MoSCoW prioritisation)
- defined our success with a SMART goal
- outlined our approach with a wireframe
- created and cloned our team git hub repo appuccino-dream-team
#### 20/07/2023, Thu
- setup ways of working as a team
- created brunch in GitHub
- raised a pull request
- updated README.md
- updated Brainstorm
### Week 8
#### 24/07/2023, Mon
- extracted data from csv file
- added headers through code
- designed schema to model data
#### 25-26/07/2023, Tue and Wed
- we worked on the script for the SQL tables of the databases
- added docker compose to github.
- discussed the data normalisation left to do on the cleansed data so far
- wrote unit tests for removing sensitive data
- wrote code to remove sensitive data from the data files
- merged all the complete branches to the main branch
#### 27/07/2023
- we imported all our data cleaning functions into our main cafe-management file
- called all the insert functions in our main cafe-management file to successfully insert records into our tables
- organised our github repo into 3 main folders: msc files, sample data and src folder
#### 28/07/2023
- successful sprint 1 presentation
### Week 9
#### 02/08/2023
- organised our sprint 2 tickets within our Trello board
- used the Fibonachi scale to estimate the difficulty of each task
- assigned tasks to individuals in our group
- created our team s3 bucket 'appuccino bucket'
- successfuly created a Lambda that reads a csv file
- set up a trigger within our lambda that responds to an event (csv file added to our s3 bucket)
#### 03/08/2023
- generated random guids to use as id's for each of our tables
#### 04/08/2023
- converted our database from MySQL to PostgreSQL
- created our cloudformation yml file
- organised our github repo acording to each sprint
- entended our lambda function to include all our of data cleaning code
### Week 10
#### 08/08/2023
- loaded all of our data from S3 bucket to Redshift
- added logging statements to our script
#### 09/08/2023
- setup Grafana infrastracture
- created an EC2 instance
- created metrics for EC2/S3/Lambda
#### 10/08/2023
- connected Grafana with Redshift
- created first sale graph
- complete integration testing
#### 11/08/2023
- updated readme file for the current week
- created more sale graphs
#### 14/08/2023 - 17/08/2023
- moved to more data analysis:
    - produced graphs to analyse:
        - Total No. of products sold by Location
        - Top 5 products sold/referencing to the branch sold in most
        - Revenue by Top 10 Products
        - Total Revenue per branch
        - Total No. of products sold by size
## Elevator pitch:
- For Super Cafe
- who management of Super Cafe
- the cafe management app
- is a storage and analysis app that uses Python and/or SQL
- that identifies trends
- unlike existing csv files/excel
- our product will help the company to look at the trends through analysis and make profit
## Agree Importance
```
Scope:
3 - Instructors have given us flexibility on how to tackle the criteria, and we'll be learning new concepts each week which we'll be able to add to our project
```
```
Budget:
2 - fixed number of software we'll we using and fixed time
```
```
Time:
1 - Fixed time limit of 6 weeks
```
```
Quality
4 - quality will change over time, we will also receive weekly feedback during our weekly demos, resulting in more opportunities for improvement
```
## MoSCoW Prioritisation
MUST HAVE:
- create products, cost, datetime tables
- database
- datawarehouse
- create pipeline
- user login
- create a graph to output the analysis result
- read csv files
- data analytics software
- unit testing
- git hub repo
- application monitoring software
- persistence when saving a file into the cloud
SHOULD HAVE:
- emailing to management team when an error occurs
- daily, weekly, monthly, annual data reports
- provide analysis result in reports
- different level of user can access different kinds of analysis result / report
COULD/WON'T HAVE:
- send confirmation emails to different cafe branches when the data from CSV files has been added to the database
- data from csv files to be updated to the database automatically as soon as the transaction has been made
- visualize our logo
- use pretty tables
## Smart goal
Successfully create an ETL pipeline to handle large volumes of transaction data (csv files from 10 branches) for the super cafe using an agile approach. We will measure progress by making sure we meet all of the weekly sprint criteria to ensure we are on track. The deadline for the programme development is the 18th of August on Friday so we have the last week to wrap up and prepare for the presentation.
https://miro.com/https://miro.com/
Sign up | Miro | Online Whiteboard for Visual Collaboration
Scalable, secure, cross-device and enterprise-ready team collaboration whiteboard for distributed teams. Join 30M+ users from around the world (6 kB)
https://miro.com/app/board/uXjVM1X7YYk=/

https://miro.com/https://miro.com/
Sign up | Miro | Online Whiteboard for Visual Collaboration
Scalable, secure, cross-device and enterprise-ready team collaboration whiteboard for distributed teams. Join 30M+ users from around the world (6 kB)
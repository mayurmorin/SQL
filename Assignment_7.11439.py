
# coding: utf-8

# Read the following data set: https://archive.ics.uci.edu/ml/machine-learning-databases/adult

# In[1]:


# DownLoading the adult.data,adult.names, and adult.test data writing into respective files 
# and storing into current working folder using os and requests python libraries
import os
import requests
ADULT_DATASET = (
    "http://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data",
    "http://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.names",
    "http://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.test",
)

# Writing a function download_data to download the adult folder related all the data into current working folder
def download_data(path='.', urls=ADULT_DATASET):
    #Checks if path does not exists, then create respective directory
    if not os.path.exists(path):
        os.mkdir(path)
        
    #tuple has been passed and running a for loop for reading data from url using requests and storing into physical files.
    for url in urls:
        response = requests.get(url)
        name = os.path.basename(url)
        with open(os.path.join(path, name), 'w') as f:
            f.write(response.text)
# End of download_data function
            
#Calling download_data function    
download_data()

# Listing files in the directory
os.listdir()


# Rename the columns as per the description from this file: https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.names

# In[2]:


import pandas as pd
names = ['age','workclass','fnlwgt','education','education-num',
        'marital-status','occupation','relationship','race',
        'sex','capital-gain','capital-loss','hours-per-week',
        'native-country','income',]
#sep=r'\s*,\s* considers white space along with comma separator
#na_values="?" replaces ? with NaN values for further cleaning and churning of data 
data = pd.read_csv('adult.data', names=names,sep=r'\s*,\s*',engine='python',na_values="?")
data.info()
#data =data.dropna()
#data.info()


# Create a sql db from adult dataset and name it sqladb

# In[3]:


import sqlite3 as db

#Create and/or connects to adult.db sqlite3 database file
sqladb = db.connect("adult.db")

#Inserting dataframe data into adult_tbl
data.to_sql('adult_tbl', sqladb, if_exists='replace', index=False)

#Creating database cursor for further querying the data
c=sqladb.cursor()


# Task 1.1 Select 10 records from the adult sqladb

# In[4]:


query = """ SELECT * FROM adult_tbl limit 10 """    #other way c.execute("SELECT * FROM adult_tbl limit 10")
pd.read_sql_query(query,sqladb)                     #other way #print(c.fetchall())


# Task 1.2 Show me the average hours per week of all men who are working in private sector

# In[5]:


c.execute("""SELECT sex,workclass, AVG("hours-per-week") AverageHoursPerWeek FROM adult_tbl 
             where sex='Male' and workclass='Private'  group by sex,workclass  """)
print(c.fetchone())


# In[6]:


query = """ SELECT sex,workclass, avg("hours-per-week") AverageHoursPerWeek FROM adult_tbl 
            where sex='Male' and workclass='Private'  group by sex,workclass   """    
pd.read_sql_query(query,sqladb)                     


# Task 1.3 Show me the frequency table for education, occupation and relationship, separately

# In[8]:


df = pd.read_sql_query("""SELECT  education, count(education) frequency from adult_tbl 
                       group by education order by frequency desc """,sqladb)
display(df)


# In[9]:


df1 = pd.read_sql_query("""SELECT  occupation, count(occupation) frequency from adult_tbl
                        group by occupation order by frequency desc """,sqladb)
display(df1)


# In[10]:


df2 = pd.read_sql_query("""SELECT  relationship, count(relationship) frequency from adult_tbl
                        group by relationship order by frequency desc """,sqladb)
display(df2)


# Task 1.4 Are there any people who are married, working in private sector and having a masters degree

# In[11]:


#education: Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool.
#workclass: Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked.
#marital-status: Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse.
df2 = pd.read_sql_query("""SELECT count(*) as People FROM adult_tbl where 1=1
                            and "marital-status" like 'Married-%'
                            and workclass='Private' 
                            and education='Masters' """,sqladb)
display(df2)


# Task 1.5 What is the average, minimum and maximum age group for people working in different sectors

# In[13]:


query = """ SELECT workclass, avg(age) mean,min(age) min,max(age) max from adult_tbl group by workclass """
pd.read_sql_query(query,sqladb)


# Task 1.6. Calculate age distribution by country

# In[14]:


query = """ SELECT "native-country" as Country, Round(Round((count(age)*100),5)/Round((select count(age) 
            from adult_tbl),5),5) as "Age Distribution"  FROM adult_tbl 
            group by "native-country" order by count(age) desc  """
pd.read_sql_query(query,sqladb)


# Task 1.7. Compute a new column as 'Net-Capital-Gain' from the two columns 'capital-gain' and 'capital-loss'

# In[20]:


#Net Capital Gain= Sum(Capital Gain)-Sum(Capital Loss)
query = """ SELECT sum("capital-gain") AS "Capital-Gain",sum("capital-loss") AS "Capital-Loss", (sum("capital-gain")-sum("capital-loss")) 
                    "Net-Capital-Gain" FROM adult_tbl  """
pd.read_sql_query(query,sqladb)


# In[21]:


#Each Row Specific Net Capital Gain Calculation
#Net Capital Gain= (Capital Gain)-(Capital Loss)  
query = """ SELECT "capital-gain","capital-loss", ("capital-gain"-"capital-loss") 
                    "Net-Capital-Gain" FROM adult_tbl where "capital-gain">0 or "capital-loss">0  """
pd.read_sql_query(query,sqladb)


# Task 2: Read the following data set: https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data
# Task 2.1. Create an sqlalchemy engine using a sample from the data set

# In[22]:


from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
db_uri = "sqlite:///adult.db"
engine = create_engine(db_uri)

# Create a MetaData instance
metadata = MetaData()
print(metadata.tables)

# reflect db schema to MetaData
metadata.reflect(bind=engine)
print(metadata.tables)

# Get Table
ex_table = metadata.tables['adult_tbl']
print(ex_table)

# create session
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()


# Task 2.2 Write two basic update queries

# In[23]:


from sqlalchemy.sql import select
from sqlalchemy import update
connection = engine.connect()

#Update Query 1 where age of 39 should be changed to 40
u = update(ex_table, ex_table.c.age=='39')
connection.execute(u, age="40")

#Update Query 2 where relationship wife should be changed to wifi
u = update(ex_table, ex_table.c.relationship=='Wife')
connection.execute(u, relationship='Wifi')

s = select([ex_table])
result=connection.execute(s)
display(result.fetchmany(10))
connection.close()


# Task 2.3. Write two delete queries

# In[28]:


from sqlalchemy import delete
connection = engine.connect()
#Delete Query to delete records with age is 40
d = delete(ex_table, ex_table.c.age==40)
connection.execute(d)

#Delete Query to delete records with relationship is Wifi
d = delete(ex_table, ex_table.c.relationship=='Wifi')
connection.execute(d)

s = select([ex_table])
result = connection.execute(s)
display(result.fetchmany(10))
connection.close()


# Task 2.4 Write two filter queries

# In[29]:


from sqlalchemy.sql import select
connection = engine.connect()
#Select Query 1 for selecting records where age is greater than 40
s = select([ex_table], ex_table.c.age>=45)
result = connection.execute(s)
display(result.fetchmany(10))
connection.close()


# In[33]:


#Select Query 2 for selecting records where relationship is Husband
connection = engine.connect()
s = select([ex_table], ex_table.c.relationship=='Husband')
result = connection.execute(s)
display(result.fetchmany(10))
connection.close()


# Task 2.5 Write two function queries

# In[34]:


import sqlite3

#function 1 connects or create database file
def connect(sqlite_file):
    """ Make connection to an SQLite database file """
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    return conn, c

#function 2 close connection with SQLite database
def close(conn):
    """ Commit changes and close connection to the database """
    conn.close()

#function 3 counts total rows
def total_rows(cursor, table_name, print_out=False):
    """ Returns the total number of rows in the database """
    cursor.execute('SELECT COUNT(*) FROM {}'.format(table_name))
    count = cursor.fetchall()
    if print_out:
        print('\nTotal rows: {}'.format(count[0][0]))
    return count[0][0]

#function 4 gives table columns information
def table_col_info(cursor, table_name, print_out=False):
    """ Returns a list of tuples with column informations:
    (id, name, type, notnull, default_value, primary_key)
    """
    cursor.execute('PRAGMA TABLE_INFO({})'.format(table_name))
    info = cursor.fetchall()
    if print_out:
        print("\nColumn Info:\nID, Name, Type, NotNull, DefaultVal, PrimaryKey")
        for col in info:
            print(col)
    return info

if __name__ == '__main__':
    sqlite_file = 'adult.db'
    table_name = 'adult_tbl'
    conn, c = connect(sqlite_file)                     #function 1  
    total_rows(c, table_name, print_out=True)          #function 2
    table_col_info(c, table_name, print_out=True)      #function 3
    close(conn)                                        #function 4


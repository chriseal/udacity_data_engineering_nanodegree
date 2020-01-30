#!/usr/bin/env python
# coding: utf-8

# # Lesson 2 Exercise 1: Creating Normalized Tables
# 
# <img src="images/postgresSQLlogo.png" width="250" height="250">

# ## In this exercise we are going to walk through the basics of modeling data in normalized form. We will create tables in PostgreSQL, insert rows of data, and do simple JOIN SQL queries to show how these mutliple tables can work together. 
# 
# #### Where you see ##### you will need to fill in code.
# 
# Note: __Do not__ click the blue Preview button in the lower task bar

# #### Import the library 
# Note: An error might popup after this command has exectuted. If it does, read it carefully before ignoring. 

# In[1]:


import psycopg2


# __Create a connection to the database, get a cursor, and set autocommit to true)__

# In[2]:


try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)
try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get cursor to the Database")
    print(e)
conn.set_session(autocommit=True)


# #### Let's imagine we have a table called Music Store. 
# 
# `Table Name: music_store
# column 0: Transaction Id
# column 1: Customer Name
# column 2: Cashier Name
# column 3: Year 
# column 4: Albums Purchased`
# 
# 
# ## Now to translate this information into a CREATE Table Statement and insert the data
# 
# <img src="images/table12.png" width="650" height="650">
# 
# 
# 

# In[3]:


# TO-DO: Add the CREATE Table Statement and INSERT statements to add the data in the table

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS music_store (transaction_id int, customer_name varchar, cashier_name varchar, year int, albums_purchased text[])")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchased)                  VALUES (%s, %s, %s, %s, %s)",                  (1, "Amanda", "Sam", 2000, ["Rubber Soul", "Let it Be"]))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchased)                  VALUES (%s, %s, %s, %s, %s)",                  (2, "Toby", "Sam", 2000, ['My Generation']))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchased)                  VALUES (%s, %s, %s, %s, %s)",                  (3, "Max", "Bob", 2018, ['Meet the Beatles','Help!']))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
    
try: 
    cur.execute("SELECT * FROM music_store;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()


# #### Moving to 1st Normal Form (1NF)
# 
# ### TO-DO: This data has not been normalized. To get this data into 1st normal form, you need to remove any collections or list of data and break up the list of songs into individual rows. 
# 

# In[4]:


## TO-DO: Complete the CREATE table statements and INSERT statements

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS music_store2 (transaction_id int, customer_name varchar, cashier_name varchar, year int, albums_purchased varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchased)                  VALUES (%s, %s, %s, %s, %s)",                  (1, "Amanda", "Sam", 2000, "Rubber Soul"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchased)                  VALUES (%s, %s, %s, %s, %s)",                  (1, "Amanda", "Sam", 2000, "Let it Be"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchased)                  VALUES (%s, %s, %s, %s, %s)",                  (2, "Toby", "Sam", 2000, 'My Generation'))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchased)                  VALUES (%s, %s, %s, %s, %s)",                  (3, 'Max', 'Bob', 2018, 'Meet the Beatles'))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchased)                  VALUES (%s, %s, %s, %s, %s)",                  (3, 'Max', 'Bob', 2018, 'Help!'))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("SELECT * FROM music_store2;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()


# #### Moving to 2nd Normal Form (2NF)
# You have now moved the data into 1NF, which is the first step in moving to 2nd Normal Form. The table is not yet in 2nd Normal Form. While each of the records in the table is unique, our Primary key (transaction id) is not unique. 
# 
# ### TO-DO: Break up the table into two tables, transactions and albums sold. 
# 

# In[15]:


try: 
    cur.execute("CREATE TABLE IF NOT EXISTS transactions (transaction_id int,         customer_name varchar, cashier_name varchar, year int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS albums_sold (        album_id int, transaction_id int, album_name varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
    cur.execute("INSERT INTO transactions (                    transaction_id, customer_name, cashier_name, year)                  VALUES (%s, %s, %s, %s)",                  (1, "Amanda", "Sam", 2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO transactions (                    transaction_id, customer_name, cashier_name, year)                 VALUES (%s, %s, %s, %s)",                 (2, "Toby", "Sam", 2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO transactions (                    transaction_id, customer_name, cashier_name, year)                  VALUES (%s, %s, %s, %s)",                  (3, "Max", "Bob", 2018))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name)                  VALUES (%s, %s, %s)",                  (1, 1, "Let it Be"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name)                  VALUES (%s, %s, %s)",                  (2, 2, "My Generation"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name)                  VALUES (%s, %s, %s)",                  (3, 1, "Rubber Soul"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name)                  VALUES (%s, %s, %s)",                  (4, 3, "Meet the Beatles"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
                
try: 
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name)                  VALUES (%s, %s, %s)",                  (5, 3, "Help!"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

print("Table: transactions\n")
try: 
    cur.execute("SELECT * FROM transactions;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: albums_sold\n")
try: 
    cur.execute("SELECT * FROM albums_sold;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)
row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()


# ### TO-DO: Do a `JOIN` on these tables to get all the information in the original first Table. 

# In[8]:


## TO-DO: Complete the join on the transactions and album_sold tables

# transactions (transaction_id int, customer_name varchar, cashier_name varchar, year int)
# albums_sold (album_id int, transaction_id int, album_name varchar)

try: 
    cur.execute("SELECT * FROM transactions JOIN         albums_sold ON         transactions.transaction_id = albums_sold.transaction_id ;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()


# #### Moving to 3rd Normal Form (3NF)
# Check our table for any transitive dependencies. 
# _HINT:_ Check the table for any transitive dependencies. _Transactions_ can remove _Cashier Name_ to its own table, called _Employees_, which will leave us with 3 tables. 
# 
# 
# ### TO-DO: Create the third table named *employees* to move to 3rd NF. 
# 

# In[14]:


# cur.execute("DROP table IF EXISTS transactions2")
# cur.execute("DROP table IF EXISTS employees")
# cur.execute("DROP table IF EXISTS albums_sold")


# In[16]:


try: 
    cur.execute("CREATE TABLE IF NOT EXISTS transactions2 (transaction_id int,         customer_name varchar, cashier_id int, year int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS employees (        employee_id int, employee_name varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
        
        
try: 
    cur.execute("INSERT INTO transactions2 (                    transaction_id, customer_name, cashier_id, year)                  VALUES (%s, %s, %s, %s)",                  (1, "Amanda", 1, 2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO transactions2 (                    transaction_id, customer_name, cashier_id, year)                 VALUES (%s, %s, %s, %s)",                 (2, "Toby", 1, 2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO transactions2 (                    transaction_id, customer_name, cashier_id, year)                  VALUES (%s, %s, %s, %s)",                  (3, "Max", 2, 2018))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)        
        
        
try: 
    cur.execute("INSERT INTO employees (employee_id, employee_name)                  VALUES (%s, %s)",                  (1, "Sam"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO employees (employee_id, employee_name)                  VALUES (%s, %s)",                  (2, "Bob"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)    

print("Table: transactions2\n")
try: 
    cur.execute("SELECT * FROM transactions2;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: albums_sold\n")
try: 
    cur.execute("SELECT * FROM albums_sold;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: employees\n")
try: 
    cur.execute("SELECT * FROM employees;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()


# ### TO-DO: Complete the last two `JOIN` on these 3 tables so we can get all the information we had in our first Table. 

# In[19]:


try: 
    cur.execute("SELECT * FROM (transactions2 JOIN employees ON                                transactions2.cashier_id = employees.employee_id) JOIN                                albums_sold ON transactions2.transaction_id = albums_sold.transaction_id;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()


# ### Your output for the above cell should be:
# 
# (1, 'Amanda', 1, 2000, 1, 1, 'Rubber Soul', 1, 'Sam')<br>
# (1, 'Amanda', 1, 2000, 2, 1, 'Let it Be', 1, 'Sam')<br>
# (2, 'Toby', 1, 2000, 3, 2, 'My Generation', 1, 'Sam')<br>
# (3, 'Max', 2, 2018, 4, 3, 'Meet the Beatles', 2, 'Bob')<br>
# (3, 'Max', 2, 2018, 5, 3, 'Help!', 2, 'Bob')<br>
# 

# ### Awesome work!! You have Normalized the dataset! 

# ### And finally close your cursor and connection. 

# In[20]:


try: 
    cur.execute("DROP table music_store")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table music_store2")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table albums_sold")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table employees")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table transactions")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table transactions2")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)


# ### And finally close your cursor and connection. 

# In[21]:


cur.close()
conn.close()


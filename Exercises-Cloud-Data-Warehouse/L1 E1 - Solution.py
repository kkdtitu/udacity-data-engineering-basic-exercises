#!/usr/bin/env python
# coding: utf-8

# # Demo 01 -  Sakila Star Schema & ETL  
# 
# All the database tables in this demo are based on public database samples and transformations
# - `Sakila` is a sample database created by `MySql` [Link](https://dev.mysql.com/doc/sakila/en/sakila-structure.html)
# - The postgresql version of it is called `Pagila` [Link](https://github.com/devrimgunduz/pagila)
# - The facts and dimension tables design is based on O'Reilly's public dimensional modelling tutorial schema [Link](http://archive.oreilly.com/oreillyschool/courses/dba3/index.html)

# # STEP0: Using ipython-sql
# 
# - load ipython-sql: `%load_ext sql`
# 
# - To execute SQL queries you write one of the following atop of your cell: 
#     - `%sql`
#         - For a one-liner SQL query
#         - You can access a python var using `$`    
#     - `%%sql`
#         - For a multi-line SQL query
#         - You can **NOT** access a python var using `$`
# 
# 
# - Running a connection string like:
# `postgresql://postgres:postgres@db:5432/pagila` connects to the database
# 

# # STEP1 : Connect to the local database where Pagila is loaded

# ##  1.1 Create the pagila db and fill it with data
# - Adding `"!"` at the beginning of a jupyter cell runs a command in a shell, i.e. we are not running python code but we are running the `createdb` and `psql` postgresql commmand-line utilities

# In[ ]:


get_ipython().system('PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila')
get_ipython().system('PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-schema.sql')
get_ipython().system('PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-data.sql')


# ## 1.2 Connect to the newly created db

# In[ ]:


get_ipython().run_line_magic('load_ext', 'sql')


# In[ ]:


DB_ENDPOINT = "127.0.0.1"
DB = 'pagila'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}"                         .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)


# In[ ]:


get_ipython().run_line_magic('sql', '$conn_string')


# # STEP 1.1: Create and populate the star schema

# # STEP2 : Explore the  3NF Schema

# <img src="./pagila-3nf.png" width="50%"/>

# ## 2.1 How much? What data sizes are we looking at?

# In[ ]:


nStores = get_ipython().run_line_magic('sql', 'select count(*) from store;')
nFilms = get_ipython().run_line_magic('sql', 'select count(*) from film;')
nCustomers = get_ipython().run_line_magic('sql', 'select count(*) from customer;')
nRentals = get_ipython().run_line_magic('sql', 'select count(*) from rental;')
nPayment = get_ipython().run_line_magic('sql', 'select count(*) from payment;')
nStaff = get_ipython().run_line_magic('sql', 'select count(*) from staff;')
nCity = get_ipython().run_line_magic('sql', 'select count(*) from city;')
nCountry = get_ipython().run_line_magic('sql', 'select count(*) from country;')

print("nFilms\t\t=", nFilms[0][0])
print("nCustomers\t=", nCustomers[0][0])
print("nRentals\t=", nRentals[0][0])
print("nPayment\t=", nPayment[0][0])
print("nStaff\t\t=", nStaff[0][0])
print("nStores\t\t=", nStores[0][0])
print("nCities\t\t=", nCity[0][0])
print("nCountry\t\t=", nCountry[0][0])


# ## 2.2 When? What time period are we talking about?

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'select min(payment_date) as start, max(payment_date) as end from payment;')


# ## 2.3 Where? Where do events in this database occur?

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'select district,  sum(city_id) as n\nfrom address\ngroup by district\norder by n desc\nlimit 10;')


# # STEP3: Perform some simple data analysis

# ## 3.1 Insight 1:   Top Grossing Movies 
# - Payments amounts are in table `payment`
# - Movies are in table `film`
# - They are not directly linked, `payment` refers to a `rental`, `rental` refers to an `inventory` item and `inventory` item refers to a `film`
# - `payment` &rarr; `rental` &rarr; `inventory` &rarr; `film`

# ### 3.1.1 Films

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'select film_id, title, release_year, rental_rate, rating  from film limit 5;')


# ### 3.1.2 Payments

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'select * from payment limit 5;')


# ### 3.1.3 Inventory

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'select * from inventory limit 5;')


# ### 3.1.4 Get the movie of every payment

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'SELECT f.title, p.amount, p.payment_date, p.customer_id                                            \nFROM payment p\nJOIN rental r    ON ( p.rental_id = r.rental_id )\nJOIN inventory i ON ( r.inventory_id = i.inventory_id )\nJOIN film f ON ( i.film_id = f.film_id)\nlimit 5;')


# ### 3.1.5 sum movie rental revenue

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'SELECT f.title, sum(p.amount) as revenue                                            \nFROM payment p\nJOIN rental r    ON ( p.rental_id = r.rental_id )\nJOIN inventory i ON ( r.inventory_id = i.inventory_id )\nJOIN film f ON ( i.film_id = f.film_id)\nGROUP BY title\nORDER BY revenue desc\nlimit 10;')


# ## 3.2 Insight 2:   Top grossing cities 
# - Payments amounts are in table `payment`
# - Cities are in table `cities`
# - `payment` &rarr; `customer` &rarr; `address` &rarr; `city`

# ### 3.2.1 Get the city of each payment

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'SELECT p.customer_id, p.rental_id, p.amount, ci.city                            \nFROM payment p\nJOIN customer c  ON ( p.customer_id = c.customer_id )\nJOIN address a ON ( c.address_id = a.address_id )\nJOIN city ci ON ( a.city_id = ci.city_id )\norder by p.payment_date\nlimit 10;')


# ### 3.2.2 Top grossing cities

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'SELECT ci.city ,  sum(p.amount) as revenue\nFROM payment p\nJOIN customer c  ON ( p.customer_id = c.customer_id )\nJOIN address a ON ( c.address_id = a.address_id )\nJOIN city ci ON ( a.city_id = ci.city_id )\ngroup by ci.city\norder by revenue desc\nlimit 10;')


# ## 3.3 Insight 3 : Revenue of a movie by customer city and by month 

# ### 3.3.1 Total revenue by month

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'SELECT sum(p.amount) as revenue, EXTRACT(month FROM p.payment_date) as month\nfrom payment p\ngroup by month\norder by revenue desc\nlimit 10;')


# ### 3.3.2 Each movie by customer city and by month (data cube)

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'SELECT f.title, p.amount, p.customer_id, ci.city, p.payment_date,EXTRACT(month FROM p.payment_date) as month\nFROM payment p\nJOIN rental r    ON ( p.rental_id = r.rental_id )\nJOIN inventory i ON ( r.inventory_id = i.inventory_id )\nJOIN film f ON ( i.film_id = f.film_id)\nJOIN customer c  ON ( p.customer_id = c.customer_id )\nJOIN address a ON ( c.address_id = a.address_id )\nJOIN city ci ON ( a.city_id = ci.city_id )\norder by p.payment_date\nlimit 10;')


# ### 3.3.3 Sum of revenue of each movie by customer city and by month

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'SELECT f.title, ci.city,EXTRACT(month FROM p.payment_date) as month, sum(p.amount) as revenue\nFROM payment p\nJOIN rental r    ON ( p.rental_id = r.rental_id )\nJOIN inventory i ON ( r.inventory_id = i.inventory_id )\nJOIN film f ON ( i.film_id = f.film_id)\nJOIN customer c  ON ( p.customer_id = c.customer_id )\nJOIN address a ON ( c.address_id = a.address_id )\nJOIN city ci ON ( a.city_id = ci.city_id )\ngroup by (f.title, ci.city, month)\norder by month, revenue desc\nlimit 10;')


# # STEP4 : Creating Facts & Dimensions

# <img src="pagila-star.png" width="50%"/>

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'CREATE TABLE dimDate\n(\n  date_key integer NOT NULL PRIMARY KEY,\n  date date NOT NULL,\n  year smallint NOT NULL,\n  quarter smallint NOT NULL,\n  month smallint NOT NULL,\n  day smallint NOT NULL,\n  week smallint NOT NULL,\n  is_weekend boolean\n);\n\nCREATE TABLE dimCustomer\n(\n  customer_key SERIAL PRIMARY KEY,\n  customer_id  smallint NOT NULL,\n  first_name   varchar(45) NOT NULL,\n  last_name    varchar(45) NOT NULL,\n  email        varchar(50),\n  address      varchar(50) NOT NULL,\n  address2     varchar(50),\n  district     varchar(20) NOT NULL,\n  city         varchar(50) NOT NULL,\n  country      varchar(50) NOT NULL,\n  postal_code  varchar(10),\n  phone        varchar(20) NOT NULL,\n  active       smallint NOT NULL,\n  create_date  timestamp NOT NULL,\n  start_date   date NOT NULL,\n  end_date     date NOT NULL\n);\n\nCREATE TABLE dimMovie\n(\n  movie_key          SERIAL PRIMARY KEY,\n  film_id            smallint NOT NULL,\n  title              varchar(255) NOT NULL,\n  description        text,\n  release_year       year,\n  language           varchar(20) NOT NULL,\n  original_language  varchar(20),\n  rental_duration    smallint NOT NULL,\n  length             smallint NOT NULL,\n  rating             varchar(5) NOT NULL,\n  special_features   varchar(60) NOT NULL\n);\nCREATE TABLE dimStore\n(\n  store_key           SERIAL PRIMARY KEY,\n  store_id            smallint NOT NULL,\n  address             varchar(50) NOT NULL,\n  address2            varchar(50),\n  district            varchar(20) NOT NULL,\n  city                varchar(50) NOT NULL,\n  country             varchar(50) NOT NULL,\n  postal_code         varchar(10),\n  manager_first_name  varchar(45) NOT NULL,\n  manager_last_name   varchar(45) NOT NULL,\n  start_date          date NOT NULL,\n  end_date            date NOT NULL\n);\nCREATE TABLE factSales\n(\n  sales_key        SERIAL PRIMARY KEY,\n  date_key         INT NOT NULL REFERENCES dimDate(date_key),\n  customer_key     INT NOT NULL REFERENCES dimCustomer(customer_key),\n  movie_key        INT NOT NULL REFERENCES dimMovie(movie_key),\n  store_key        INT NOT NULL REFERENCES dimStore(store_key),\n  sales_amount     decimal(5,2) NOT NULL\n);')


# # STEP 5: ETL the data from 3NF tables to Facts & Dimension Tables

# In[ ]:


get_ipython().run_cell_magic('sql', '', "INSERT INTO dimDate (date_key, date, year, quarter, month, day, week, is_weekend)\nSELECT DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer) AS date_key,\n       date(payment_date)                                           AS date,\n       EXTRACT(year FROM payment_date)                              AS year,\n       EXTRACT(quarter FROM payment_date)                           AS quarter,\n       EXTRACT(month FROM payment_date)                             AS month,\n       EXTRACT(day FROM payment_date)                               AS day,\n       EXTRACT(week FROM payment_date)                              AS week,\n       CASE WHEN EXTRACT(ISODOW FROM payment_date) IN (6, 7) THEN true ELSE false END AS is_weekend\nFROM payment;\n\n\nINSERT INTO dimCustomer (customer_key, customer_id, first_name, last_name, email, address, address2, district, city, country, postal_code, phone, active, create_date, start_date, end_date)\nSELECT c.customer_id AS customer_key,\n       c.customer_id,\n       c.first_name,\n       c.last_name,\n       c.email,\n       a.address,\n       a.address2,\n       a.district,\n       ci.city,\n       co.country,\n       a.postal_code,\n       a.phone,\n       c.active,\n       c.create_date,\n       now()         AS start_date,\n       now()         AS end_date\nFROM customer c\nJOIN address a  ON (c.address_id = a.address_id)\nJOIN city ci    ON (a.city_id = ci.city_id)\nJOIN country co ON (ci.country_id = co.country_id);\n\nINSERT INTO dimMovie (movie_key, film_id, title, description, release_year, language, original_language, rental_duration, length, rating, special_features)\nSELECT f.film_id      AS movie_key,\n       f.film_id,\n       f.title,\n       f.description,\n       f.release_year,\n       l.name         AS language,\n       orig_lang.name AS original_language,\n       f.rental_duration,\n       f.length,\n       f.rating,\n       f.special_features\nFROM film f\nJOIN language l              ON (f.language_id=l.language_id)\nLEFT JOIN language orig_lang ON (f.original_language_id = orig_lang.language_id);\n\nINSERT INTO dimStore (store_key, store_id, address, address2, district, city, country, postal_code, manager_first_name, manager_last_name, start_date, end_date)\nSELECT s.store_id    AS store_key,\n       s.store_id,\n       a.address,\n       a.address2,\n       a.district,\n       c.city,\n       co.country,\n       a.postal_code,\n       st.first_name AS manager_first_name,\n       st.last_name  AS manager_last_name,\n       now()         AS start_date,\n       now()         AS end_date\nFROM store s\nJOIN staff st   ON (s.manager_staff_id = st.staff_id)\nJOIN address a  ON (s.address_id = a.address_id)\nJOIN city c     ON (a.city_id = c.city_id)\nJOIN country co ON (c.country_id = co.country_id);\n\nINSERT INTO factSales (date_key, customer_key, movie_key, store_key, sales_amount)\nSELECT TO_CHAR(p.payment_date :: DATE, 'yyyyMMDD')::integer AS date_key ,\n       p.customer_id                                        AS customer_key,\n       i.film_id                                            AS movie_key,\n       i.store_id                                           AS store_key,\n       p.amount                                             AS sales_amount\nFROM payment p\nJOIN rental r    ON ( p.rental_id = r.rental_id )\nJOIN inventory i ON ( r.inventory_id = i.inventory_id );")


# # STEP 6: Repeat the computation from the facts & dimension table

# ## 6.1 Facts Table has all the needed dimensions, no need for deep joins

# In[42]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT movie_key, date_key, customer_key, sales_amount\nFROM factSales \nlimit 5;')


# ## 6.2 Join fact table with dimensions to replace keys with attributes

# In[ ]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT dimMovie.title, dimDate.month, dimCustomer.city, sales_amount\nFROM factSales \nJOIN dimMovie on (dimMovie.movie_key = factSales.movie_key)\nJOIN dimDate on (dimDate.date_key = factSales.date_key)\nJOIN dimCustomer on (dimCustomer.customer_key = factSales.customer_key)\nlimit 5;')


# In[ ]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT dimMovie.title, dimDate.month, dimCustomer.city, sum(sales_amount) as revenue\nFROM factSales \nJOIN dimMovie    on (dimMovie.movie_key      = factSales.movie_key)\nJOIN dimDate     on (dimDate.date_key         = factSales.date_key)\nJOIN dimCustomer on (dimCustomer.customer_key = factSales.customer_key)\ngroup by (dimMovie.title, dimDate.month, dimCustomer.city)\norder by dimMovie.title, dimDate.month, dimCustomer.city, revenue desc;')


# In[ ]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT f.title, EXTRACT(month FROM p.payment_date) as month, ci.city, sum(p.amount) as revenue\nFROM payment p\nJOIN rental r    ON ( p.rental_id = r.rental_id )\nJOIN inventory i ON ( r.inventory_id = i.inventory_id )\nJOIN film f ON ( i.film_id = f.film_id)\nJOIN customer c  ON ( p.customer_id = c.customer_id )\nJOIN address a ON ( c.address_id = a.address_id )\nJOIN city ci ON ( a.city_id = ci.city_id )\ngroup by (f.title, month, ci.city)\norder by f.title, month, ci.city, revenue desc;')


# # Conclusion

# - We were able to show that a start schema is easier to understand
# - Evidence that is more performantm

# In[46]:


get_ipython().system('PGPASSWORD=student pg_dump -h 127.0.0.1 -U student pagila > Data/pagila-star.sql')


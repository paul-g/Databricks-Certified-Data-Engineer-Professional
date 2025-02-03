# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/books.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NOTE: This cell contains the SQL query for your reference, and won't work if run directly.
# MAGIC -- The query is used below in the type2_upsert() function as part of the foreachBatch call.
# MAGIC
# MAGIC -- write a sql query to create the streaming books table in silver layer, with SCD type 2 support

# COMMAND ----------

def type2_upsert(microBatchDF, batch):
  # write a functino to apply type2 upsert

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS books_silver
# MAGIC (book_id STRING, title STRING, author STRING, price DOUBLE, current BOOLEAN, effective_date TIMESTAMP, end_date TIMESTAMP)

# COMMAND ----------

def process_books():
    schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"
 
    # write a pyspark query to load orders from bronze to silver, using type2 SCD
    
process_books()

# COMMAND ----------

books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)

# COMMAND ----------

bookstore.load_books_updates()
bookstore.process_bronze()
process_books()

# COMMAND ----------

books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/current_books.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- write a SQL query to create a view of the current books, current_books (book_id, title, author, price) from books_silver
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_books
# MAGIC ORDER BY book_id
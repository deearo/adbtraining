// Databricks notebook source
// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS PracticeDB;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS PracticeDB.Customers
// MAGIC USING CSV
// MAGIC OPTIONS
// MAGIC (
// MAGIC path "mnt/data/Customers/*.csv",
// MAGIC sep ",",
// MAGIC header "true",
// MAGIC inferSchema "true"
// MAGIC failfast "true"
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT Count(1) FROM PracticeDB.Customers

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS PracticeDB.Products
// MAGIC USING JSON
// MAGIC OPTIONS
// MAGIC (
// MAGIC path "mnt/data/products/*.json",
// MAGIC multiline "true",
// MAGIC failfast "true"
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT Count(1) FROM PracticeDB.Products

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS PracticeDB.Orders
// MAGIC USING CSV
// MAGIC OPTIONS
// MAGIC (
// MAGIC path "mnt/data/orders/*.csv",
// MAGIC sep ",",
// MAGIC header "true",
// MAGIC inferSchema "true",
// MAGIC failfast "true"
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT Count(1) FROM PracticeDB.Orders

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC USE PracticeDB;
// MAGIC SHOW TABLES

// COMMAND ----------

val getOrderAmount = (units: Int, unitPrice: Int, itemdiscount: Int) => {
  val total = (units * unitPrice)
  val discount = ((total * itemdiscount) / 100).asInstanceOf[Int]
  
  (total - discount).asInstanceOf[Int]
}

val getCustomerType = (credit: Int) => {
  if(credit < 10000) "Silver"
  else if(credit >= 10000 && credit < 25000) "Gold"
  else "Platinum"
}

spark.udf.register("getCustomerType", getCustomerType)
spark.udf.register("getOrderAmount", getOrderAmount)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT o.orderid AS OrderId, o.orderdate AS OrderDate, c.customername AS CustomerName, p.title AS ProductTitle,
// MAGIC     c.address AS CustomerLocation, getCustomerType(c.credit) AS CustomerType,
// MAGIC     getOrderAmount(o.units, p.unitprice, p.itemdiscount) AS OrderAmount,
// MAGIC     p.unitprice AS UnitPrice, p.itemdiscount AS ItemDiscount,
// MAGIC     o.billingaddress AS BillingAddress, o.remarks AS OrderRemarks
// MAGIC   FROM PracticeDB.Orders o
// MAGIC   INNER JOIN  PracticeDB.Customers c ON c.customerid = o.customer
// MAGIC   INNER JOIN PracticeDB.Products p ON p.productid = o.product
// MAGIC   WHERE o.billingaddress IN ( 'Bangalore', 'Trivandrum', 'Hyderabad', 'Mumbai', 'Chennai', 'New Delhi')
// MAGIC   ORDER BY OrderAmount

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS PracticeDB.ProcessedOrders
// MAGIC USING PARQUET
// MAGIC OPTIONS
// MAGIC (
// MAGIC path "mnt/data/processed-orders",
// MAGIC failfast "true"
// MAGIC )
// MAGIC AS 
// MAGIC SELECT CAST(o.orderid AS STRING) AS OrderId, o.orderdate AS OrderDate, c.customername AS CustomerName, p.title AS ProductTitle,
// MAGIC     c.address AS CustomerLocation, getCustomerType(c.credit) AS CustomerType,
// MAGIC     getOrderAmount(o.units, p.unitprice, p.itemdiscount) AS OrderAmount,
// MAGIC     p.unitprice AS UnitPrice, p.itemdiscount AS ItemDiscount,
// MAGIC     o.billingaddress AS BillingAddress, o.remarks AS OrderRemarks
// MAGIC   FROM PracticeDB.Orders o
// MAGIC   INNER JOIN  PracticeDB.Customers c ON c.customerid = o.customer
// MAGIC   INNER JOIN PracticeDB.Products p ON p.productid = o.product
// MAGIC   WHERE o.billingaddress IN ( 'Bangalore', 'Trivandrum', 'Hyderabad', 'Mumbai', 'Chennai', 'New Delhi')
// MAGIC   ORDER BY OrderAmount

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS PracticeDB.PreProcessedOrders
// MAGIC USING PARQUET
// MAGIC OPTIONS
// MAGIC (
// MAGIC path "mnt/data/processed-orders",
// MAGIC failfast "true"
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM PracticeDB.PreProcessedOrders
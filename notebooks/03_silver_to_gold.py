# ============================================================
# Notebook: 03_silver_to_gold
# Purpose:  Build aggregated Gold tables from Silver (Delta)
# Source:   Silver (Delta) → Sink: Gold (Delta)
# ============================================================

from pyspark.sql.functions import col, sum, count, avg, round, date_format

storage_account = "adlsfirstdepr"

client_id     = dbutils.secrets.get(scope="kv-scope", key="entra-client-id")
tenant_id     = dbutils.secrets.get(scope="kv-scope", key="entra-tenant-id")
client_secret = dbutils.secrets.get(scope="kv-scope", key="entra-client-secret")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

silver = f"abfss://silver@{storage_account}.dfs.core.windows.net/SalesLT"
gold   = f"abfss://gold@{storage_account}.dfs.core.windows.net/SalesLT"

# Helper function for writing to Gold
def write_gold(df, path):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path)

print("✅ Configs set!")

# ============================================================
# Read Silver Tables
# ============================================================
df_customer     = spark.read.format("delta").load(f"{silver}/Customer/")
df_address      = spark.read.format("delta").load(f"{silver}/Address/")
df_cust_addr    = spark.read.format("delta").load(f"{silver}/CustomerAddress/")
df_product      = spark.read.format("delta").load(f"{silver}/Product/")
df_category     = spark.read.format("delta").load(f"{silver}/ProductCategory/")
df_model        = spark.read.format("delta").load(f"{silver}/ProductModel/")
df_header       = spark.read.format("delta").load(f"{silver}/SalesOrderHeader/")
df_detail       = spark.read.format("delta").load(f"{silver}/SalesOrderDetail/")

print("✅ All Silver tables loaded!")

# ============================================================
# Gold 1: Sales By Customer
# ============================================================
df_gold = df_header.join(df_customer, "CustomerID") \
    .groupBy("CustomerID", "FirstName", "LastName", "EmailAddress", "CompanyName") \
    .agg(
        count("SalesOrderID").alias("Total_Orders"),
        round(sum("TotalDue"), 2).alias("Total_Revenue"),
        round(avg("TotalDue"), 2).alias("Avg_Order_Value"),
        round(sum("SubTotal"), 2).alias("Total_Sub_Total"),
        round(sum("TaxAmt"), 2).alias("Total_Tax"),
        round(sum("Freight"), 2).alias("Total_Freight")
    ) \
    .withColumnRenamed("CustomerID", "Customer_ID") \
    .withColumnRenamed("FirstName", "First_Name") \
    .withColumnRenamed("LastName", "Last_Name") \
    .withColumnRenamed("EmailAddress", "Email_Address") \
    .withColumnRenamed("CompanyName", "Company_Name") \
    .orderBy(col("Total_Revenue").desc())

write_gold(df_gold, f"{gold}/Sales_By_Customer/")
print(f"✅ Sales_By_Customer: {df_gold.count()} rows → Gold")

# ============================================================
# Gold 2: Sales By Product
# ============================================================
df_gold = df_detail \
    .join(df_product, "ProductID") \
    .join(df_category, "ProductCategoryID") \
    .groupBy("ProductID", "ProductName", "ProductNumber", "CategoryName", "Color") \
    .agg(
        count("SalesOrderID").alias("Total_Orders"),
        sum("OrderQty").alias("Total_Qty_Sold"),
        round(sum("LineTotal"), 2).alias("Total_Revenue"),
        round(avg("UnitPrice"), 2).alias("Avg_Unit_Price"),
        round(avg("UnitPriceDiscount"), 4).alias("Avg_Discount")
    ) \
    .withColumnRenamed("ProductID", "Product_ID") \
    .withColumnRenamed("ProductName", "Product_Name") \
    .withColumnRenamed("ProductNumber", "Product_Number") \
    .withColumnRenamed("CategoryName", "Category_Name") \
    .orderBy(col("Total_Revenue").desc())

write_gold(df_gold, f"{gold}/Sales_By_Product/")
print(f"✅ Sales_By_Product: {df_gold.count()} rows → Gold")

# ============================================================
# Gold 3: Sales By Category
# ============================================================
df_gold = df_detail \
    .join(df_product, "ProductID") \
    .join(df_category, "ProductCategoryID") \
    .groupBy("ProductCategoryID", "CategoryName") \
    .agg(
        count("SalesOrderID").alias("Total_Orders"),
        sum("OrderQty").alias("Total_Qty_Sold"),
        round(sum("LineTotal"), 2).alias("Total_Revenue"),
        round(avg("UnitPrice"), 2).alias("Avg_Unit_Price")
    ) \
    .withColumnRenamed("ProductCategoryID", "Product_Category_ID") \
    .withColumnRenamed("CategoryName", "Category_Name") \
    .orderBy(col("Total_Revenue").desc())

write_gold(df_gold, f"{gold}/Sales_By_Category/")
print(f"✅ Sales_By_Category: {df_gold.count()} rows → Gold")

# ============================================================
# Gold 4: Monthly Sales Trend
# ============================================================
df_gold = df_header \
    .withColumn("Year_Month", date_format("OrderDate", "yyyy-MM")) \
    .withColumn("Year", date_format("OrderDate", "yyyy")) \
    .withColumn("Month", date_format("OrderDate", "MM")) \
    .groupBy("Year_Month", "Year", "Month") \
    .agg(
        count("SalesOrderID").alias("Total_Orders"),
        round(sum("TotalDue"), 2).alias("Total_Revenue"),
        round(avg("TotalDue"), 2).alias("Avg_Order_Value"),
        round(sum("SubTotal"), 2).alias("Total_Sub_Total")
    ) \
    .orderBy("Year_Month")

write_gold(df_gold, f"{gold}/Monthly_Sales_Trend/")
print(f"✅ Monthly_Sales_Trend: {df_gold.count()} rows → Gold")

# ============================================================
# Gold 5: Sales By Geography
# ============================================================
df_gold = df_header \
    .join(df_cust_addr, "CustomerID") \
    .join(df_address, "AddressID") \
    .groupBy("City", "StateProvince", "CountryRegion") \
    .agg(
        count("SalesOrderID").alias("Total_Orders"),
        round(sum("TotalDue"), 2).alias("Total_Revenue"),
        round(avg("TotalDue"), 2).alias("Avg_Order_Value")
    ) \
    .withColumnRenamed("StateProvince", "State_Province") \
    .withColumnRenamed("CountryRegion", "Country_Region") \
    .orderBy(col("Total_Revenue").desc())

write_gold(df_gold, f"{gold}/Sales_By_Geography/")
print(f"✅ Sales_By_Geography: {df_gold.count()} rows → Gold")

# ============================================================
# Gold 6: Product Performance
# ============================================================
df_gold = df_product \
    .join(df_category, "ProductCategoryID") \
    .join(df_model, "ProductModelID") \
    .select(
        col("ProductID").alias("Product_ID"),
        col("ProductName").alias("Product_Name"),
        col("ProductNumber").alias("Product_Number"),
        col("CategoryName").alias("Category_Name"),
        col("ModelName").alias("Model_Name"),
        col("Color"),
        col("StandardCost").alias("Standard_Cost"),
        col("ListPrice").alias("List_Price"),
        col("IsDiscontinued").alias("Is_Discontinued"),
        col("SellStartDate").alias("Sell_Start_Date"),
        col("SellEndDate").alias("Sell_End_Date")
    )

write_gold(df_gold, f"{gold}/Product_Performance/")
print(f"✅ Product_Performance: {df_gold.count()} rows → Gold")

# ============================================================
# Final Summary
# ============================================================
print("\n" + "="*50)
print("✅✅ ALL GOLD TABLES CREATED SUCCESSFULLY!")
print("="*50)
print("""
Gold Tables:
  1. Sales_By_Customer
  2. Sales_By_Product
  3. Sales_By_Category
  4. Monthly_Sales_Trend
  5. Sales_By_Geography
  6. Product_Performance
""")

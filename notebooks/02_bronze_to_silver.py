# ============================================================
# Notebook: 02_bronze_to_silver
# Purpose:  Clean, cast, deduplicate all 10 SalesLT tables
# Source:   Bronze (Parquet) → Sink: Silver (Delta)
# ============================================================

from pyspark.sql.functions import col, trim, upper, lower, to_date, when

storage_account = "adlsfirstdepr"

client_id     = dbutils.secrets.get(scope="kv-scope", key="entra-client-id")
tenant_id     = dbutils.secrets.get(scope="kv-scope", key="entra-tenant-id")
client_secret = dbutils.secrets.get(scope="kv-scope", key="entra-client-secret")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

bronze = f"abfss://bronze@{storage_account}.dfs.core.windows.net/SalesLT"
silver = f"abfss://silver@{storage_account}.dfs.core.windows.net/SalesLT"

# Helper function for writing to Silver
def write_silver(df, path):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path)

print("✅ Configs set!")

# ============================================================
# 1. Address
# ============================================================
df = spark.read.parquet(f"{bronze}/Address/")

df_silver = df.select(
    col("AddressID").cast("int"),
    trim(col("AddressLine1")).alias("AddressLine1"),
    trim(col("AddressLine2")).alias("AddressLine2"),
    trim(col("City")).alias("City"),
    trim(col("StateProvince")).alias("StateProvince"),
    trim(col("CountryRegion")).alias("CountryRegion"),
    trim(col("PostalCode")).alias("PostalCode"),
    to_date(col("ModifiedDate")).alias("ModifiedDate")
) \
.dropDuplicates(["AddressID"]) \
.filter(col("AddressID").isNotNull())

write_silver(df_silver, f"{silver}/Address/")
print(f"✅ Address: {df_silver.count()} rows → Silver")

# ============================================================
# 2. Customer
# ============================================================
df = spark.read.parquet(f"{bronze}/Customer/")

df_silver = df.select(
    col("CustomerID").cast("int"),
    col("NameStyle").cast("boolean"),
    trim(col("Title")).alias("Title"),
    trim(col("FirstName")).alias("FirstName"),
    trim(col("MiddleName")).alias("MiddleName"),
    trim(col("LastName")).alias("LastName"),
    trim(col("Suffix")).alias("Suffix"),
    trim(col("CompanyName")).alias("CompanyName"),
    trim(col("SalesPerson")).alias("SalesPerson"),
    lower(trim(col("EmailAddress"))).alias("EmailAddress"),
    trim(col("Phone")).alias("Phone"),
    to_date(col("ModifiedDate")).alias("ModifiedDate")
) \
.dropDuplicates(["CustomerID"]) \
.filter(col("CustomerID").isNotNull())

write_silver(df_silver, f"{silver}/Customer/")
print(f"✅ Customer: {df_silver.count()} rows → Silver")

# ============================================================
# 3. CustomerAddress
# ============================================================
df = spark.read.parquet(f"{bronze}/CustomerAddress/")

df_silver = df.select(
    col("CustomerID").cast("int"),
    col("AddressID").cast("int"),
    trim(col("AddressType")).alias("AddressType"),
    to_date(col("ModifiedDate")).alias("ModifiedDate")
) \
.dropDuplicates(["CustomerID", "AddressID"]) \
.filter(col("CustomerID").isNotNull() & col("AddressID").isNotNull())

write_silver(df_silver, f"{silver}/CustomerAddress/")
print(f"✅ CustomerAddress: {df_silver.count()} rows → Silver")

# ============================================================
# 4. Product
# ============================================================
df = spark.read.parquet(f"{bronze}/Product/")

df_silver = df.select(
    col("ProductID").cast("int"),
    trim(col("Name")).alias("ProductName"),
    trim(col("ProductNumber")).alias("ProductNumber"),
    trim(col("Color")).alias("Color"),
    col("StandardCost").cast("double"),
    col("ListPrice").cast("double"),
    trim(col("Size")).alias("Size"),
    col("Weight").cast("double"),
    col("ProductCategoryID").cast("int"),
    col("ProductModelID").cast("int"),
    to_date(col("SellStartDate")).alias("SellStartDate"),
    to_date(col("SellEndDate")).alias("SellEndDate"),
    to_date(col("DiscontinuedDate")).alias("DiscontinuedDate"),
    trim(col("ThumbnailPhotoFileName")).alias("ThumbnailPhotoFileName"),
    to_date(col("ModifiedDate")).alias("ModifiedDate")
) \
.dropDuplicates(["ProductID"]) \
.filter(col("ProductID").isNotNull()) \
.withColumn("IsDiscontinued", when(col("DiscontinuedDate").isNotNull(), True).otherwise(False))

write_silver(df_silver, f"{silver}/Product/")
print(f"✅ Product: {df_silver.count()} rows → Silver")

# ============================================================
# 5. ProductCategory
# ============================================================
df = spark.read.parquet(f"{bronze}/ProductCategory/")

df_silver = df.select(
    col("ProductCategoryID").cast("int"),
    col("ParentProductCategoryID").cast("int"),
    trim(col("Name")).alias("CategoryName"),
    to_date(col("ModifiedDate")).alias("ModifiedDate")
) \
.dropDuplicates(["ProductCategoryID"]) \
.filter(col("ProductCategoryID").isNotNull())

write_silver(df_silver, f"{silver}/ProductCategory/")
print(f"✅ ProductCategory: {df_silver.count()} rows → Silver")

# ============================================================
# 6. ProductDescription
# ============================================================
df = spark.read.parquet(f"{bronze}/ProductDescription/")

df_silver = df.select(
    col("ProductDescriptionID").cast("int"),
    trim(col("Description")).alias("Description"),
    to_date(col("ModifiedDate")).alias("ModifiedDate")
) \
.dropDuplicates(["ProductDescriptionID"]) \
.filter(col("ProductDescriptionID").isNotNull())

write_silver(df_silver, f"{silver}/ProductDescription/")
print(f"✅ ProductDescription: {df_silver.count()} rows → Silver")

# ============================================================
# 7. ProductModel
# ============================================================
df = spark.read.parquet(f"{bronze}/ProductModel/")

df_silver = df.select(
    col("ProductModelID").cast("int"),
    trim(col("Name")).alias("ModelName"),
    trim(col("CatalogDescription")).alias("CatalogDescription"),
    to_date(col("ModifiedDate")).alias("ModifiedDate")
) \
.dropDuplicates(["ProductModelID"]) \
.filter(col("ProductModelID").isNotNull())

write_silver(df_silver, f"{silver}/ProductModel/")
print(f"✅ ProductModel: {df_silver.count()} rows → Silver")

# ============================================================
# 8. ProductModelProductDescription
# ============================================================
df = spark.read.parquet(f"{bronze}/ProductModelProductDescription/")

df_silver = df.select(
    col("ProductModelID").cast("int"),
    col("ProductDescriptionID").cast("int"),
    trim(col("Culture")).alias("Culture"),
    to_date(col("ModifiedDate")).alias("ModifiedDate")
) \
.dropDuplicates(["ProductModelID", "ProductDescriptionID", "Culture"]) \
.filter(col("ProductModelID").isNotNull())

write_silver(df_silver, f"{silver}/ProductModelProductDescription/")
print(f"✅ ProductModelProductDescription: {df_silver.count()} rows → Silver")

# ============================================================
# 9. SalesOrderDetail
# ============================================================
df = spark.read.parquet(f"{bronze}/SalesOrderDetail/")

df_silver = df.select(
    col("SalesOrderID").cast("int"),
    col("SalesOrderDetailID").cast("int"),
    col("OrderQty").cast("int"),
    col("ProductID").cast("int"),
    col("UnitPrice").cast("double"),
    col("UnitPriceDiscount").cast("double"),
    col("LineTotal").cast("double"),
    to_date(col("ModifiedDate")).alias("ModifiedDate")
) \
.dropDuplicates(["SalesOrderDetailID"]) \
.filter(col("SalesOrderDetailID").isNotNull()) \
.withColumn("DiscountedPrice", col("UnitPrice") * (1 - col("UnitPriceDiscount")))

write_silver(df_silver, f"{silver}/SalesOrderDetail/")
print(f"✅ SalesOrderDetail: {df_silver.count()} rows → Silver")

# ============================================================
# 10. SalesOrderHeader
# ============================================================
df = spark.read.parquet(f"{bronze}/SalesOrderHeader/")

df_silver = df.select(
    col("SalesOrderID").cast("int"),
    col("RevisionNumber").cast("int"),
    to_date(col("OrderDate")).alias("OrderDate"),
    to_date(col("DueDate")).alias("DueDate"),
    to_date(col("ShipDate")).alias("ShipDate"),
    col("Status").cast("int"),
    col("OnlineOrderFlag").cast("boolean"),
    trim(col("SalesOrderNumber")).alias("SalesOrderNumber"),
    trim(col("PurchaseOrderNumber")).alias("PurchaseOrderNumber"),
    trim(col("AccountNumber")).alias("AccountNumber"),
    col("CustomerID").cast("int"),
    col("ShipToAddressID").cast("int"),
    col("BillToAddressID").cast("int"),
    trim(col("ShipMethod")).alias("ShipMethod"),
    col("SubTotal").cast("double"),
    col("TaxAmt").cast("double"),
    col("Freight").cast("double"),
    col("TotalDue").cast("double"),
    trim(col("Comment")).alias("Comment"),
    to_date(col("ModifiedDate")).alias("ModifiedDate")
) \
.dropDuplicates(["SalesOrderID"]) \
.filter(col("SalesOrderID").isNotNull()) \
.withColumn("OrderStatus",
    when(col("Status") == 1, "In Process")
    .when(col("Status") == 2, "Approved")
    .when(col("Status") == 3, "Backordered")
    .when(col("Status") == 4, "Rejected")
    .when(col("Status") == 5, "Shipped")
    .when(col("Status") == 6, "Cancelled")
    .otherwise("Unknown")
)

write_silver(df_silver, f"{silver}/SalesOrderHeader/")
print(f"✅ SalesOrderHeader: {df_silver.count()} rows → Silver")

# ============================================================
# Final Summary
# ============================================================
print("\n" + "="*50)
print("✅✅ ALL 10 TABLES WRITTEN TO SILVER SUCCESSFULLY!")
print("="*50)

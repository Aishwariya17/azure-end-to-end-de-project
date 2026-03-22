# configs using Entra ID (Service Principal)
storage_account = "adlsfirstdepr"

client_id     = dbutils.secrets.get(scope="kv-scope", key="entra-client-id")
tenant_id     = dbutils.secrets.get(scope="kv-scope", key="entra-tenant-id")
client_secret = dbutils.secrets.get(scope="kv-scope", key="entra-client-secret")

# Set Spark configs directly
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

print("✅ Spark configs set successfully!")

# Test connection — list bronze container
dbutils.fs.ls(f"abfss://bronze@adlsfirstdepr.dfs.core.windows.net/")

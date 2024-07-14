# Access Azure Data Lake using access keys
# Set the spark config fs.azure.account.key.
# List files form demo container.
# Read data form circuits.csv file.

spark.conf.set("fs.azure.account.key.f1dl9072024.dfs.core.windows.net",
               "secretkeyHERE")

# "DemoAccountName - keyvault Secret = 'demo'"
dbutils.fs.ls("abfss://demo@f1dl9072024.dfs.core.windows.net")

display(spark.read.csv(
    "abfss://demo@f1dl9072024.dfs.core.windows.net/upload-circuits-csv"))

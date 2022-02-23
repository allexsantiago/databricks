# Databricks notebook source
#dbutils.secrets.help()

# COMMAND ----------

#dbutils.secrets.listScopes()

# COMMAND ----------

#dbutils.secrets.list("akv-alex-lab")

# COMMAND ----------

#dbutils.secrets.get("akv-alex-lab", key="dataEngineer-Client-id")

# COMMAND ----------

#Neste modo de autenticação no akv basicamente ele apenas impede que acidentalmente voce suba suas senhas para o github ou algo do tipo, atraves do script abaixo ainda é possivel ver a key.
#Existe algumas configs mais com recursos para resolver este problema
#for x in dbutils.secrets.get("akv-alex-lab", key="dataEngineer-Client-id"):
 #   print(x)
    

# COMMAND ----------

storage_account_name = "datalakelabalex2"
client_id            = dbutils.secrets.get("akv-alex-lab", key="dataEngineer-Client-id")
tenant_id            = dbutils.secrets.get("akv-alex-lab", key="dataEngineer-Tenant-id")
client_secret        = dbutils.secrets.get("akv-alex-lab", key="dataEngineer-secret-id")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/datalakelabalex2/raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/datalakelabalex2/processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/datalakelabalex2/presentation")

# COMMAND ----------

#dbutils.fs.mounts()

# COMMAND ----------

#dbutils.fs.unmount("/mnt/datalakelabalex2/raw")

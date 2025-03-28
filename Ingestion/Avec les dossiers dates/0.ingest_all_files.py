# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuit_file (CSV)", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
                         #le file qu'on veut run -   la colonne source prévu dans "1.ingest" : le fichier contenant le dataset

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ########### Technique pour éxecuter les notebook en parallèle
# MAGIC ########### Limite grandement la quantité de processus (threads) balancés

# COMMAND ----------

import concurrent.futures

# Fonction pour exécuter un notebook
def run_notebook(notebook_name, parameters):
    result = dbutils.notebook.run(notebook_name, 0, parameters)
    return result

# Liste des notebooks à exécuter et de leurs paramètres
notebooks = [
    ("1.ingest_circuit_file (CSV)", {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"}),
    ("2.ingestion_races_file", {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"}),
    ("3.ingestion_constructors_file (JSON)", {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"}),
    ("4.ingest_drivers_file (JSON)",{"p_data_source": "Ergast API", "p_file_date": "2021-04-18"}),
    ("5.ingest_results_file (JSON)", {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"}),
    ("6.ingest_pit-stops_file (JSON)", {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"}),
    ("7.ingest_lap-times_folder", {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"}),
    ("8.ingest_qualifying_folder", {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})
]

# Exécution parallèle des notebooks
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = [executor.submit(run_notebook, notebook, params) for notebook, params in notebooks]

# Attendre la fin de toutes les exécutions
for future in concurrent.futures.as_completed(results):
    try:
        result = future.result()
        print("Résultat:", result)
    except Exception as e:
        print("Une erreur s'est produite:", e)

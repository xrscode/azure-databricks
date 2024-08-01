# Databricks notebook source
notebook_dicts = [  
                  "1.d_transform_race_results.py",
                  "2.d_transform_driver_standings.py",
                  "3.d_transform_constructor_standings.py"
                  ]

dict_list = [
    # {"p_file_date": "2021-04-18"},
    # {"p_file_date": "2021-03-28"},
    {"p_file_date": "2021-03-21"}
]

for notebook in notebook_dicts:
    for dict in dict_list:
        try:
            dbutils.notebook.run(notebook, 0, dict)
        except Exception as e:
            print('Error message: {e}')

try:
    dbutils.notebook.run("4.calculated_increment_race_results", 0)
except Exception as e:
    print('Error message: {e}')
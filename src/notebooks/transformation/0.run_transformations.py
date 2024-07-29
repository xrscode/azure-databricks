# Databricks notebook source
notebook_dicts = [  
                  {"file": "1.race_results", "time": 0},
                  {"file": "2.driver_standings", "time": 0},
                  {"file": "3.constructor_standings", "time": 0},
                  {"file": "4.calculated_race_results", "time": 0}
                  ]


for note in notebook_dicts:
    try:
        print(f"Attempting notebook: {note['file']}")
        dbutils.notebook.run(note['file'], note['time'])
    except Exception as e:
        print(f"Error! Message: {e}")

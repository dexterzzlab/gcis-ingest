# gcis_ingest



Private repo for GCIS ingest scripts

Overview:
This is a set of three scripts designed to modularly :


Three part dump/ingest scripts.

python dump_all.py - this grabs all json files from GCIS live site and separates every entry into its own .json file

python dt_crawler.py - calls python general_ingest.py type file. Does this for all json files created by dump

general_ingest.py type file - ingests a json file into elastic search

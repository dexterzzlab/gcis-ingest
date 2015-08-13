#!/bin/bash

python /home/ubuntu/dump_ingest/gcis_dump.py ;
python /home/ubuntu/dump_ingest/gcis_es_crawler.py ;
source /home/ubuntu/facetview-prov-es/env/bin/activate 
python /home/ubuntu/dump_ingest/gcis_to_prov_es_crawler.py ;
source /home/ubuntu/facetview-prov-es/env/bin/activate 
python /home/ubuntu/dump_ingest/prov_es_crawler.py

#!/bin/bash

python /home/gcisops/gcis-crawler/gcis_dump.py ;
python /home/gcisops/gcis-crawler/gcis_es_crawler.py ;
#source /home/gcisops/gcis-crawler/env/bin/activate 
#python /home/gcisops/gcis-crawler/gcis_to_prov_es_crawler.py ;
#source /home/ubuntu/facetview-prov-es/env/bin/activate 
#python /home/ubuntu/dump_ingest/prov_es_crawler.py

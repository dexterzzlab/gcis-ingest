#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

source /home/gcisops/env/bin/activate


DUMP_PATH=$(ls -dt -- /home/gcisops/ops/gcis-crawler/GCIS_DUMP/*/ | head -n1); 

python $BASE_PATH/prov_scripts/organization_prov.py $DUMP_PATH
python $BASE_PATH/prov_scripts/person_prov.py $DUMP_PATH
python $BASE_PATH/prov_scripts/article_prov.py $DUMP_PATH
python $BASE_PATH/prov_scripts/book_prov.py $DUMP_PATH
python $BASE_PATH/prov_scripts/dataset_prov.py $DUMP_PATH
python $BASE_PATH/prov_scripts/figure_prov.py $DUMP_PATH
python $BASE_PATH/prov_scripts/instrument_prov.py $DUMP_PATH
python $BASE_PATH/prov_scripts/journal_prov.py $DUMP_PATH
python $BASE_PATH/prov_scripts/model_prov.py $DUMP_PATH
python $BASE_PATH/prov_scripts/platform_prov.py $DUMP_PATH
python $BASE_PATH/prov_scripts/report_prov.py $DUMP_PATH
python $BASE_PATH/prov_scripts/scenario_prov.py $DUMP_PATH


#echo "${DUMP_PATH}"

#python $BASE_PATH/gcis_es_crawler.py gcis_ingest.py $VAR

#source $BASE_PATH/env/bin/activate ; 
#python $BASE_PATH/ops/gcis-crawler/gcis_dump.py
#python /home/gcisops/ops/gcis-crawler/gcis_es_crawler.py gcis_ingest $*

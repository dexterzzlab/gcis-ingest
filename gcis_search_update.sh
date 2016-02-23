#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

#echo $BASE_PATH

source /home/gcisops/env/bin/activate

python $BASE_PATH/gcis_dump.py ;

DUMP_PATH=$(ls -dt -- /home/gcisops/ops/gcis-crawler/GCIS_DUMP/*/ | head -n1);·

#echo "${DUMP_PATH}"

#VAR=$(ls -dt -- parent/*/ | head -n1); 


#echo "${VAR::-1}"

python $BASE_PATH/gcis_es_crawler.py $BASE_PATH/gcis_es_ingest.py $DUMP_PATH

#source $BASE_PATH/env/bin/activate ; 
#python $BASE_PATH/ops/gcis-crawler/gcis_dump.py
#python /home/gcisops/ops/gcis-crawler/gcis_es_crawler.py gcis_ingest $*

#!/bin/bash

# Purpose of this script:
# 1. Prepare files for aranogdb-import command

if [ $# -eq 0 ]
  then
    echo "Provide path to data folder"
    exit 1
fi

DATA_DIR="$1"
TIMESTAMP=`basename $DATA_DIR`
OUTPUT_DIR=$PWD/output/arangodb/$TIMESTAMP
IMPORT_DIR=$OUTPUT_DIR/import

# ArangoDB connection options
DBNAME="Transactions"
DBUSERNAME="root"
DBPASSWORD=""

source $(dirname "$0")/arangoUtil.sh

if [ ! -d $OUTPUT_DIR ]; then
  echo "Generating files for import"

  mkdir -p $IMPORT_DIR

  TRANSACTIONS_HEADER="_key|source|target|date|time|amount|currency"
  ATMS_HEADER="_key|latitude|longitude"
  CLIENTS_HEADER="_key|first_name|last_name|age|email|occupation|political_views|nationality|university|academic_degree|address|postal_code|country|city"
  COMPANIES_HEADER="_key|type|name|country"

  echo $TRANSACTIONS_HEADER >> $IMPORT_DIR/transdata.csv
  for transactions in `ls $DATA_DIR/nodes.transactions.*`; do
    cat $transactions >> $IMPORT_DIR/transdata.csv
  done

  sed -i.bak "1 s/.*/$TRANSACTIONS_HEADER/" $IMPORT_DIR/transdata.csv
  echo "transdata.csv generated"

  cp $DATA_DIR/nodes.atms.csv $IMPORT_DIR/atms.csv
  sed -i.bak "1 s/.*/$ATMS_HEADER/" $IMPORT_DIR/atms.csv
  echo "atms.csv generated"

  cp $DATA_DIR/nodes.clients.csv $IMPORT_DIR/clients.csv
  sed -i.bak "1 s/.*/$CLIENTS_HEADER/" $IMPORT_DIR/clients.csv
  echo "clients.csv generated"

  cp $DATA_DIR/nodes.companies.csv $IMPORT_DIR/companies.csv
  sed -i.bak "1 s/.*/$COMPANIES_HEADER/" $IMPORT_DIR/companies.csv
  echo "companies.csv generated"

  rm $IMPORT_DIR/*.bak
else
  echo "ArangoDB files are already generated"
fi

if [ "$(arangoDbExists ${DBNAME})" == "true" ]; then
   echo "Recreating database ${DBNAME}"
   arangoDropDb ${DBNAME}
fi

for fname in `ls $IMPORT_DIR`; do
   coll_name=${fname%%.*}
   echo "Importing collection ${coll_name}"
   arangoimport --server.username "${DBUSERNAME}" --server.password "${DBPASSWORD}" \
      --server.database "${DBNAME}" --create-database true \
      --collection "${coll_name}" --create-collection true \
      --separator '|' --type csv \
      --file "${IMPORT_DIR}/${fname}" --threads 4 --batch-size 1048576
done

arangoUseDb ${DBNAME}
arangoCreateEdgeCollection transactions
numTransactions=$(arangoCollSize transdata)

NUM_THREADS=4
BATCH_SIZE=100000

read -r -d '' GEN_EDGES_QUERY << EOM
FOR t IN transdata
   LIMIT %d, %d
   LET srcColl =
        DOCUMENT("clients", t.source) != null ? "clients/" :
        DOCUMENT("companies", t.source) != null ? "companies/" :
        "atms/"
   LET targetColl =
        DOCUMENT("clients", t.target) != null ? "clients/" :
        DOCUMENT("companies", t.target) != null ? "companies/" :
        "atms/"
   INSERT {
      _key : t._key,
      _from : CONCAT(srcColl, t.source),
      _to : CONCAT(targetColl, t.target),
      date : t.date,
      time : t.time,
      amount : t.amount,
      currency : t.currency
   } INTO transactions
EOM

for i in `seq 0 $((NUM_THREADS*BATCH_SIZE)) ${numTransactions}`; do
   echo "Edges generated: $i"
   for j in `seq ${i} ${BATCH_SIZE} $((i+BATCH_SIZE*NUM_THREADS-1))`; do
      query=$(printf "${GEN_EDGES_QUERY}" $j ${BATCH_SIZE})
      arangoQueryNoRes "${query}" &
   done
   wait
done

echo "Edges generated: ${numTransactions}"

echo "Deleting temporary transaction data"
arangoDropCollection transdata

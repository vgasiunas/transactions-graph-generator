#!/bin/bash

DBUSERNAME="${DBUSERNAME:-root}"
DBPASSWORD="${DBPASSWORD:-}"

function arangoExecOnDB() {
  local dbname=$1
  local codestring=$2
  arangosh --server.username "${DBUSERNAME}" --server.password "${DBPASSWORD}" \
      --server.database "${dbname}" \
      --javascript.execute-string "${codestring}" \
  || (echo "Error executing ${codestring}" && exit 1)
}

function arangoUseDb() {
  CURR_DBNAME="$1"
}

function arangoExec() {
  if [ "${CURR_DBNAME}" == "" ]; then
     echo Specify the database name using arangoUseDb
     return 1
  fi
  arangoExecOnDB "${DBNAME}" "$1" 
}

function arangoSysExec() {
  arangoExecOnDB "_system" "$1"
}

function arangoDbExists() {
  local dbName="$1"
  arangoSysExec "print(db._databases().includes(\"${dbName}\"))"
}

function arangoCreateDb() {
  local dbName="$1"
  arangoSysExec "db._createDatabase(\"${dbName}\")"
}

function arangoDropDb() {
  local dbName="$1"
  arangoSysExec "db._dropDatabase(\"${dbName}\")"
}

function arangoCreateEdgeCollection() {
  local collName="$1"
  arangoExec "db._createEdgeCollection(\"${collName}\")"
}

function arangoDropCollection() {
  local collName="$1"
  arangoExec "db._drop(\"${collName}\")"
}

function arangoListCollections() {
  arangoExec "print(db._collections().map(coll => coll.name()).filter(name => name[0] != '_'))"
}

function arangoCollSize() {
  local collName="$1"
  arangoExec "print(db.${collName}.count())"
}

function arangoEnsureIndex() {
  local collName="$1"
  local fieldName="$2"
  arangoExec "db.${collName}.ensureIndex({ type: \"persistent\", fields: [ \"${fieldName}\" ] })"
}

function arangoQuery() {
  local query=$(echo "$1" | sed 's/`/\\`/g')
  arangoExec "print(db._query(\`${query}\`).toArray())"
}

function arangoQueryNoRes() {
  local query=$(echo "$1" | sed 's/`/\\`/g')
  arangoExec "db._query(\`${query}\`).toArray()"
}

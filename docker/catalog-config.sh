#!/bin/bash -xe

CATALOG_CLASS_PREFIX="org.apache.spark.sql.crossdata.catalog.persistent"

function jdbcCatalog() {
    export crossdata_core_catalog_jdbc_driver=$1
    export crossdata_core_catalog_jdbc_url=$2
    export crossdata_core_catalog_jdbc_name=$3
    export crossdata_core_catalog_jdbc_user=$4
    export crossdata_core_catalog_jdbc_pass=$5
}

function zookeeperCatalog() {
    export crossdata_core_catalog_zookeeper_connectionString=${1:-localhost:2181}
    export crossdata_core_catalog_zookeeper_connectionTimeout=${2:-15000}
    export crossdata_core_catalog_zookeeper_sessionTimeout=${3:-60000}
    export crossdata_core_catalog_zookeeper_retryAttempts=${4:-5}
    export crossdata_core_catalog_zookeeper_retryInterval=${5:-10000}
}


export crossdata_core_catalog_class="${CATALOG_CLASS_PREFIX}.$1Catalog"
if [ "$1" == "MySQL" ]; then
   jdbcCatalog "org.mariadb.jdbc.Driver" ${XD_CATALOG_HOST} ${XD_CATALOG_DB_NAME} ${XD_CATALOG_DB_USER} ${XD_CATALOG_DB_PASS}
fi
if [ "$1" == "PostgreSQL" ]; then
   jdbcCatalog "org.postgresql.Driver" ${XD_CATALOG_HOST} ${XD_CATALOG_DB_NAME} ${XD_CATALOG_DB_USER} ${XD_CATALOG_DB_PASS}
fi
if [ "$1" == "Zookeeper" ]; then
   zookeeperCatalog ${XD_CATALOG_ZOOKEEPER_CONNECTION_STRING} ${XD_CATALOG_ZOOKEEPER_CONNECTION_TIMEOUT} ${XD_CATALOG_ZOOKEEPER_SESSION_TIMEOUT} ${XD_CATALOG_ZOOKEEPER_RETRY_ATTEMPS} ${XD_CATALOG_ZOOKEEPER_RETRY_INTERVAL}
fi
if [ "x$2x" != "xx" ]; then
   export crossdata_core_catalog_prefix=${2:-crossdataCluster}
fi

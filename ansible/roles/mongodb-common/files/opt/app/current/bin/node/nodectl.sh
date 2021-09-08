# sourced by /opt/app/current/bin/ctl.sh
# error code
ERR_HOSTS_INFO_FILE=201

# path info
MONGODB_DATA_PATH=/data/mongodb-data
MONGODB_LOG_PATH=/data/mongodb-logs
MONGODB_CONF_PATH=/data/mongodb-conf
DB_QC_CLUSTER_PASS_FILE=/data/appctl/data/qc_cluster_pass
DB_QC_LOCAL_PASS_FILE=/data/appctl/data/qc_local_pass
HOSTS_INFO_FILE=/data/appctl/data/hosts.info
CONF_INFO_FILE=/data/appctl/data/conf.info

getItemFromFile() {
    local res=$(cat $2 | sed '/^'$1'=/!d;s/.*=//')
    echo "$res"
}

start() {
    _start
    log "service started"
}

init() {
    log "init replicaset"
}

createMongoConf() {
    local replication_replSetName
    local storage_engine
    local net_port
    local setParameter_cursorTimeoutMillis
    local operationProfiling_mode
    local operationProfiling_slowOpThresholdMs
    local replication_enableMajorityReadConcern
    local sharding_clusterRole
    local sharding_configDB
    if [ $MY_ROLE = "mongos_node" ]; then
        net_port=$(getItemFromFile net_port $CONF_INFO_FILE)
        sharding_configDB=$(getItemFromFile sharding_configDB $CONF_INFO_FILE)
        setParameter_cursorTimeoutMillis=$(getItemFromFile setParameter_cursorTimeoutMillis $CONF_INFO_FILE)
        cat > $MONGODB_CONF_PATH/mongo.conf <<MONGO_CONF
systemLog:
    destination: file
    path: $MONGODB_LOG_PATH/mongo.log
    logAppend: true
    logRotate: reopen
net:
    port: $net_port
    bindIp: 0.0.0.0
setParameter:
    cursorTimeoutMillis: $setParameter_cursorTimeoutMillis
sharding:
    configDB: $sharding_configDB
MONGO_CONF
    else
        net_port=$(getItemFromFile net_port $CONF_INFO_FILE)
        setParameter_cursorTimeoutMillis=$(getItemFromFile setParameter_cursorTimeoutMillis $CONF_INFO_FILE)
        replication_replSetName=$(getItemFromFile replication_replSetName $CONF_INFO_FILE)
        storage_engine=$(getItemFromFile storage_engine $CONF_INFO_FILE)
        operationProfiling_mode=$(getItemFromFile operationProfiling_mode $CONF_INFO_FILE)
        operationProfiling_slowOpThresholdMs=$(getItemFromFile operationProfiling_slowOpThresholdMs $CONF_INFO_FILE)
        replication_enableMajorityReadConcern=$(getItemFromFile replication_enableMajorityReadConcern $CONF_INFO_FILE)
        if [ "$MY_ROLE" = "cs_node" ]; then
            sharding_clusterRole="configsvr"
        else
            sharding_clusterRole="shardsvr"
        fi
        cat > $MONGODB_CONF_PATH/mongo.conf <<MONGO_CONF
systemLog:
    destination: file
    path: $MONGODB_LOG_PATH/mongo.log
    logAppend: true
    logRotate: reopen
net:
    port: $net_port
    bindIp: 0.0.0.0
security:
    keyFile: $MONGODB_CONF_PATH/repl.key
    authorization: enabled
storage:
    dbPath: $MONGODB_DATA_PATH
    journal:
        enabled: true
    engine: $storage_engine
operationProfiling:
    mode: $operationProfiling_mode
    slowOpThresholdMs: $operationProfiling_slowOpThresholdMs
replication:
    oplogSizeMB: 2048
    replSetName: $replication_replSetName
sharding:
    clusterRole: $sharding_clusterRole
setParameter:
    cursorTimeoutMillis: $setParameter_cursorTimeoutMillis
MONGO_CONF

        cat > $MONGODB_CONF_PATH/mongo-admin.conf <<MONGO_CONF
systemLog:
    destination: syslog
net:
    port: $CONF_MAINTAIN_NET_PORT
    bindIp: 0.0.0.0
storage:
    dbPath: $MONGODB_DATA_PATH
    engine: $storage_engine
processManagement:
    fork: true
MONGO_CONF
    fi
}

clusterPreInit() {
    # folder
    mkdir -p $MONGODB_DATA_PATH $MONGODB_LOG_PATH $MONGODB_CONF_PATH
    chown -R mongod:svc $MONGODB_DATA_PATH $MONGODB_LOG_PATH $MONGODB_CONF_PATH
    # repl.key
    echo "$GLOBAL_UUID" | base64 > "$MONGODB_CONF_PATH/repl.key"
    chown mongod:svc $MONGODB_CONF_PATH/repl.key
    chmod 0400 $MONGODB_CONF_PATH/repl.key
    #qc_cluster_pass
    local encrypted=$(echo -n ${CLUSTER_ID}${GLOBAL_UUID} | sha256sum | base64)
    echo ${encrypted:0:16} > $DB_QC_CLUSTER_PASS_FILE
    #qc_local_pass
    encrypted=$(echo -n ${GLOBAL_UUID}${CLUSTER_ID} | sha256sum | base64)
    echo ${encrypted:16:16} > $DB_QC_LOCAL_PASS_FILE
    #create config files
    cat $HOSTS_INFO_FILE.new > $HOSTS_INFO_FILE
    cat $CONF_INFO_FILE.new > $CONF_INFO_FILE
    touch $MONGODB_CONF_PATH/mongo.conf
    chown mongod:svc $MONGODB_CONF_PATH/mongo.conf
    createMongoConf
}

checkConfdChange() {
    if [ ! -d /data/appctl/logs ]; then
        log "cluster pre-init"
        clusterPreInit
    fi
}
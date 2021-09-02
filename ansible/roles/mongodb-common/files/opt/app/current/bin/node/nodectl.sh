# sourced by /opt/app/current/bin/ctl.sh
MONGODB_DATA_PATH=/data/mongodb-data
MONGODB_LOG_PATH=/data/mongodb-logs
MONGODB_CONF_PATH=/data/mongodb-conf
start() {
    #_start
    log "service started"
}

init() {
    log "init replicaset"
}

checkConfdChange() {
    if [ ! -d /data/appctl/logs ]; then
        log "cluster pre-init"
        return
    fi
}
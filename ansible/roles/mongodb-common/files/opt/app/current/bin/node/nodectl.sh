# sourced by /opt/app/current/bin/ctl.sh

start() {
    #_start
    log "service started"
}

init() {
    log "init cluster"
}

checkConfdChange() {
    if [ ! -d /data/appctl/logs ]; then
        log "cluster pre-init"
        return
    fi
}
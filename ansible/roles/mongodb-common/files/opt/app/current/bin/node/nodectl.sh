# sourced by /opt/app/current/bin/ctl.sh
# error code
ERR_BALANCER_STOP=201
ERR_CHGVXNET_PRECHECK=202
ERR_SCALEIN_SHARD_FORBIDDEN=203
ERR_SERVICE_STOPPED=204
ERR_PORT_NOT_LISTENED=205
ERR_NOTVALID_SHARD_RESTORE=206
ERR_INVALID_PARAMS_MONGOCMD=207
ERR_REPL_NOT_HEALTH=208

# path info
MONGODB_DATA_PATH=/data/mongodb-data
MONGODB_LOG_PATH=/data/mongodb-logs
MONGODB_CONF_PATH=/data/mongodb-conf
DB_QC_LOCAL_PASS_FILE=/data/appctl/data/qc_local_pass
HOSTS_INFO_FILE=/data/appctl/data/hosts.info
CONF_INFO_FILE=/data/appctl/data/conf.info
NODE_FIRST_CREATE_FLAG_FILE=/data/appctl/data/node.first.create.flag
CS_MONITOR_ITEM_FILE=/opt/app/current/bin/node/cs.monitor
MONGOS_MONITOR_ITEM_FILE=/opt/app/current/bin/node/mongos.monitor
SHARD_MONITOR_ITEM_FILE=/opt/app/current/bin/node/shard.monitor
HEALTH_CHECK_FLAG_FILE=/data/appctl/data/health.check.flag
BACKUP_FLAG_FILE=/data/appctl/data/backup.flag
CONF_ZABBIX_INFO_FILE=/data/appctl/data/conf.zabbix
ZABBIX_CONF_PATH=/etc/zabbix
ZABBIX_LOG_PATH=/data/zabbix-log

# runMongoCmd
# desc run mongo shell
# $1: script string
# $2-x: option
# -u username, -p passwd
# -P port, -H ip
runMongoCmd() {
  local cmd="/opt/mongodb/current/bin/mongo --quiet"
  local jsstr="$1"
  
  shift
  if [ $(($# % 2)) -ne 0 ]; then log "Invalid runMongoCmd params"; return $ERR_INVALID_PARAMS_MONGOCMD; fi
  while [ $# -gt 0 ]; do
    case $1 in
      "-u") cmd="$cmd --authenticationDatabase admin --username $2";;
      "-p") cmd="$cmd --password $2";;
      "-P") cmd="$cmd --port $2";;
      "-H") cmd="$cmd --host $2";;
    esac
    shift 2
  done

  timeout --preserve-status 5 $cmd --eval "$jsstr"
}

APPCTL_CMD_PATH=/usr/bin/appctl
isSingleThread() {
  local tmpcnt=$(pgrep -fa "$APPCTL_CMD_PATH $1" | wc -l)
  test $tmpcnt -eq 2
}

# getSid
# desc: get sid from NODE_LIST item
# $1: a NODE_LIST item (5/192.168.1.2)
# output: sid
getSid() {
  echo $(echo $1 | cut -d'/' -f1)
}

getIp() {
  echo $(echo $1 | cut -d'/' -f2)
}

getNodeId() {
  echo $(echo $1 | cut -d'/' -f3)
}

getGid() {
  echo $(echo $1 | cut -d'/' -f4)
}

getItemFromFile() {
  local res=$(cat $2 | sed '/^'$1'=/!d;s/^'$1'=//')
  echo "$res"
}

isNodeFirstCreate() {
  test -f $NODE_FIRST_CREATE_FLAG_FILE
}

clearNodeFirstCreateFlag() {
  if [ -f $NODE_FIRST_CREATE_FLAG_FILE ]; then rm -f $NODE_FIRST_CREATE_FLAG_FILE; fi
}

enableHealthCheck() {
  touch $HEALTH_CHECK_FLAG_FILE
}

disableHealthCheck() {
  rm -f $HEALTH_CHECK_FLAG_FILE
}

needHealthCheck() {
  test -f $HEALTH_CHECK_FLAG_FILE
}

# msIsReplStatusOk
# check if replia set's status is ok
# 1 primary, other's secondary
msIsReplStatusOk() {
  local allcnt=$1
  shift
  local tmpstr=$(runMongoCmd "JSON.stringify(rs.status())" $@ | jq .members[].stateStr)
  local pcnt=$(echo "$tmpstr" | grep PRIMARY | wc -l)
  local scnt=$(echo "$tmpstr" | grep SECONDARY | wc -l)
  test $pcnt -eq 1
  test $((pcnt+scnt)) -eq $allcnt
}

msIsReplOther() {
  local res=$(runMongoCmd "JSON.stringify(rs.status())" $@ | jq .ok)
  if [ -z "$res" ] || [ $res -eq 0 ]; then return 0; fi
  return 1
}

msEnableBalancer() {
  if runMongoCmd "sh.setBalancerState(true)" $@; then
    log "enable balancer: succeeded"
  else
    log "enable balancer: failed"
  fi
}

msGetHostDbVersion() {
  local jsstr=$(cat <<EOF
db.version()
EOF
  )
  runMongoCmd "$jsstr" $@
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
  local read_concern
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
security:
  keyFile: $MONGODB_CONF_PATH/repl.key
setParameter:
  cursorTimeoutMillis: $setParameter_cursorTimeoutMillis
sharding:
  configDB: $sharding_configDB
MONGO_CONF

    cat > $MONGODB_CONF_PATH/mongo-admin.conf <<MONGO_CONF
systemLog:
  destination: syslog
net:
  port: $NET_MAINTAIN_PORT
  bindIp: 0.0.0.0
security:
  keyFile: $MONGODB_CONF_PATH/repl.key
setParameter:
  cursorTimeoutMillis: $setParameter_cursorTimeoutMillis
sharding:
  configDB: $sharding_configDB
processManagement:
  fork: true
MONGO_CONF
  else
    net_port=$(getItemFromFile net_port $CONF_INFO_FILE)
    setParameter_cursorTimeoutMillis=$(getItemFromFile setParameter_cursorTimeoutMillis $CONF_INFO_FILE)
    replication_replSetName=$(getItemFromFile replication_replSetName $CONF_INFO_FILE)
    storage_engine=$(getItemFromFile storage_engine $CONF_INFO_FILE)
    operationProfiling_mode=$(getItemFromFile operationProfiling_mode $CONF_INFO_FILE)
    operationProfiling_slowOpThresholdMs=$(getItemFromFile operationProfiling_slowOpThresholdMs $CONF_INFO_FILE)
    replication_enableMajorityReadConcern=$(getItemFromFile replication_enableMajorityReadConcern $CONF_INFO_FILE)
    replication_oplogSizeMB=$(getItemFromFile replication_oplogSizeMB $CONF_INFO_FILE)
    if [ "$MY_ROLE" = "cs_node" ]; then
      sharding_clusterRole="configsvr"
      read_concern=""
    else
      sharding_clusterRole="shardsvr"
      read_concern="enableMajorityReadConcern: $replication_enableMajorityReadConcern"
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
  oplogSizeMB: $replication_oplogSizeMB
  replSetName: $replication_replSetName
  $read_concern
sharding:
  clusterRole: $sharding_clusterRole
setParameter:
  cursorTimeoutMillis: $setParameter_cursorTimeoutMillis
MONGO_CONF

    cat > $MONGODB_CONF_PATH/mongo-admin.conf <<MONGO_CONF
systemLog:
  destination: syslog
net:
  port: $NET_MAINTAIN_PORT
  bindIp: 0.0.0.0
storage:
  dbPath: $MONGODB_DATA_PATH
  journal:
    enabled: true
  engine: $storage_engine
processManagement:
  fork: true
MONGO_CONF
  fi
}

createZabbixConf() {
  local zServer=$(getItemFromFile Server $CONF_ZABBIX_INFO_FILE)
  local zListenPort=$(getItemFromFile ListenPort $CONF_ZABBIX_INFO_FILE)
  cat > $ZABBIX_CONF_PATH/zabbix_agent2.conf <<ZABBIX_CONF
PidFile=/var/run/zabbix/zabbix_agent2.pid
LogFile=/data/zabbix-log/zabbix_agent2.log
LogFileSize=50
Server=$zServer
#ServerActive=127.0.0.1
ListenPort=$zListenPort
Include=/etc/zabbix/zabbix_agent2.d/*.conf
UnsafeUserParameters=1
ZABBIX_CONF
}

updateZabbixConf() {
  if ! diff $CONF_ZABBIX_INFO_FILE $CONF_ZABBIX_INFO_FILE.new; then
    cat $CONF_ZABBIX_INFO_FILE.new > $CONF_ZABBIX_INFO_FILE
    createZabbixConf
  fi
}

updateMongoConf() {
  if ! diff $CONF_INFO_FILE $CONF_INFO_FILE.new; then
    cat $CONF_INFO_FILE.new > $CONF_INFO_FILE
    createMongoConf
  fi
}

updateHostsInfo() {
  if ! diff $HOSTS_INFO_FILE $HOSTS_INFO_FILE.new; then
    cat $HOSTS_INFO_FILE.new > $HOSTS_INFO_FILE
  fi
}

doWhenMongosPreStart() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return 0; fi
  if isNodeFirstCreate || [ $ADDING_HOSTS_FLAG = true ] || [ $VERTICAL_SCALING_FLAG = true ]; then return 0; fi
  
  local slist=(${NODE_LIST[@]})
  local cnt=${#slist[@]}
  if [ $(getSid ${slist[0]}) = $MY_SID ]; then return 0; fi

  # update config
  updateHostsInfo
  updateMongoConf

  # re-enable balancer
  shellStartMongosForAdmin
  retry 60 3 0 msGetHostDbVersion -P $NET_MAINTAIN_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  msEnableBalancer -P $NET_MAINTAIN_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  msStopMongosForAdmin
}

MONGOD_BIN=/opt/mongodb/current/bin/mongod
shellStartMongodForAdmin() {
  runuser mongod -g svc -s "/bin/bash" -c "$MONGOD_BIN -f $MONGODB_CONF_PATH/mongo-admin.conf --setParameter disableLogicalSessionCacheRefresh=true"
}

shellStopMongodForAdmin() {
  runuser mongod -g svc -s "/bin/bash" -c "$MONGOD_BIN -f $MONGODB_CONF_PATH/mongo-admin.conf --shutdown"
}

msGetReplCfgFromLocal() {
  local jsstr=$(cat <<EOF
mydb = db.getSiblingDB("local")
JSON.stringify(mydb.system.replset.findOne())
EOF
  )
  runMongoCmd "$jsstr" -P $NET_MAINTAIN_PORT
}

msUpdateReplCfgToLocal() {
  local jsstr=$(cat <<EOF
newlist=[$1]
mydb = db.getSiblingDB('local')
cfg = mydb.system.replset.findOne()
cnt = cfg.members.length
for(i=0; i<cnt; i++) {
  cfg.members[i].host=newlist[i]
}
mydb.system.replset.update({"_id":"$RS_NAME"},cfg)
EOF
  )
  runMongoCmd "$jsstr" -P $NET_MAINTAIN_PORT
}

updateShardInfoForShardNode() {
  local jsstr
  jsstr=$(cat <<EOF
mydb = db.getSiblingDB('admin')
csinfo = mydb.system.version.findOne({"_id": "shardIdentity"}).configsvrConnectionString
EOF
  )
  local csconnstr=$(runMongoCmd "$jsstr" $@)
  local oldcslist=($(getItemFromFile EXT_LIST $HOSTS_INFO_FILE))
  local oldcsport=$(getItemFromFile EXT_PORT $HOSTS_INFO_FILE)
  local newcslist=($(getItemFromFile EXT_LIST $HOSTS_INFO_FILE.new))
  local newcsport=$(getItemFromFile EXT_PORT $HOSTS_INFO_FILE.new)
  local cnt=${#oldcslist[@]}
  local nid
  local oldip
  local newip
  for((i=0;i<$cnt;i++)); do
    nid=$(getNodeId ${oldcslist[i]})
    oldip=$(getIp ${oldcslist[i]})
    for((j=0;j<$cnt;j++)); do
      if [ ! $nid = "$(getNodeId ${newcslist[j]})" ]; then continue; fi
      newip=$(getIp ${newcslist[j]})
      csconnstr=$(echo "$csconnstr" | sed 's/'$oldip:$oldcsport'/'$newip:$newcsport'/')
    done
  done

  jsstr=$(cat <<EOF
mydb = db.getSiblingDB('admin')
mydb.system.version.updateOne({"_id": "shardIdentity"}, {\$set: {"configsvrConnectionString": "$csconnstr"}})
EOF
  )
  runMongoCmd "$jsstr" $@
}

updateShardInfoForCsNode() {
  local jsstr
  jsstr=$(cat <<EOF
mydb = db.getSiblingDB('config')
mydb.shards.find()
EOF
  )
  local tmpstr=$(runMongoCmd "$jsstr" $@ | jq '.host' | sed 's/"//g')
  local oldshardlist=($(getItemFromFile EXT_LIST $HOSTS_INFO_FILE))
  local oldport=$(getItemFromFile EXT_PORT $HOSTS_INFO_FILE)
  local newshardlist=($(getItemFromFile EXT_LIST $HOSTS_INFO_FILE.new))
  local newport=$(getItemFromFile EXT_PORT $HOSTS_INFO_FILE.new)
  local cnt=${#oldshardlist[@]}
  local replname
  local oldip
  local nid
  local newip
  local shardhoststr
  for line in $(echo "$tmpstr"); do
    shardhoststr=$line
    replname=$(echo "$line" | cut -d'/' -f1)
    for((i=0;i<$cnt;i++)); do
      nid=$(getNodeId ${oldshardlist[i]})
      oldip=$(getIp ${oldshardlist[i]})
      for((j=0;j<$cnt;j++)); do
        if [ ! $nid = "$(getNodeId ${newshardlist[j]})" ]; then continue; fi
        newip=$(getIp ${newshardlist[j]})
        shardhoststr=$(echo "$shardhoststr" | sed 's/'$oldip:$oldport'/'$newip:$newport'/')
      done
    done
    jsstr=$(cat <<EOF
mydb = db.getSiblingDB('config')
mydb.shards.updateOne({"_id": "$replname"}, {\$set: {"host": "$shardhoststr"}})
EOF
  )
  runMongoCmd "$jsstr" $@
  done
}

changeReplNodeNetInfo() {
  # start mongod in admin mode
  shellStartMongodForAdmin

  local replcfg
  retry 60 3 0 msGetHostDbVersion -P $NET_MAINTAIN_PORT
  replcfg=$(msGetReplCfgFromLocal)
  local cnt=${#NODE_LIST[@]}
  local oldinfo=$(getItemFromFile NODE_LIST $HOSTS_INFO_FILE)
  local oldport=$(getItemFromFile PORT $HOSTS_INFO_FILE)
  local tmpstr
  local newlist
  for((i=0;i<$cnt;i++)); do
    # old ip:port
    tmpstr=$(echo "$replcfg" | jq ".members[$i] | .host" | sed s/\"//g)
    # nodeid
    tmpstr=$(echo "$oldinfo" | sed 's/\/cln-/:'$oldport'\/cln-/g' | sed 's/ /\n/g' | sed -n /$tmpstr/p)
    tmpstr=$(getNodeId $tmpstr)
    # newip
    tmpstr=$(echo ${NODE_LIST[@]} | grep -o '[[:digit:].]\+/'$tmpstr | cut -d'/' -f1)
    newlist="$newlist\"$tmpstr:$MY_PORT\","
  done
  # update replicaset config
  # js array: "ip:port","ip:port","ip:port"
  newlist=${newlist:0:-1}
  msUpdateReplCfgToLocal "$newlist"

  # update shard info
  if [ $MY_ROLE = "cs_node" ]; then
    updateShardInfoForCsNode -P $NET_MAINTAIN_PORT
  else
    updateShardInfoForShardNode -P $NET_MAINTAIN_PORT
  fi

  # stop mongod in admin mode
  shellStopMongodForAdmin
}

updateCSShardInfo() {
  retry 60 3 0 msGetHostDbVersion -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  retry 60 3 0 msIsReplStatusOk ${#NODE_LIST[@]} -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
}

doWhenReplPreStart() {
  if [ $MY_ROLE = "mongos_node" ]; then return 0; fi
  if [ $CHANGE_VXNET_FLAG = "true" ]; then
    changeReplNodeNetInfo
  else
    :
  fi
}

refreshZabbixAgentStatus() {
  zEnabled=$(getItemFromFile Enabled $CONF_ZABBIX_INFO_FILE)
  if [ $zEnabled = "yes" ]; then
    systemctl restart zabbix-agent2.service || :
    log "zabbix-agent2 restarted"
  else
    systemctl stop zabbix-agent2.service || :
    log "zabbix-agent2 stopped"
  fi
}

start() {
  doWhenMongosPreStart
  doWhenReplPreStart
  # updat conf files
  updateHostsInfo
  updateMongoConf
  _start
  if ! isNodeFirstCreate; then enableHealthCheck; fi
  clearNodeFirstCreateFlag
  # start zabbix-agent2
  updateZabbixConf
  refreshZabbixAgentStatus
}

# sortHostList
# input
#  $1-n: hosts array
# output
#  sorted array, like 'v1 v2 v3 ...'
sortHostList() {
  echo $@ | tr ' ' '\n' | sort
}

getInitNodeList() {
  if [ $MY_ROLE = "cs_node" ] || [ $MY_ROLE = "mongos_node" ]; then
    echo $(sortHostList ${NODE_LIST[@]})
  else
    echo ${NODE_LIST[@]}
  fi
}

MONGOS_BIN=/opt/mongodb/current/bin/mongos
shellStartMongosForAdmin() {
  runuser mongod -g svc -s "/bin/bash" -c "$MONGOS_BIN -f $MONGODB_CONF_PATH/mongo-admin.conf"
}

msStopMongosForAdmin() {
  local jsstr=$(cat <<EOF
db = db.getSiblingDB('admin')
db.shutdownServer()
EOF
  )
  runMongoCmd "$jsstr" -P $NET_MAINTAIN_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
}

# msInitRepl
# init replicaset
#  first node: priority 2
#  other node: priority 1
#  last node: priority 0, hidden true
# init readonly replicaset
#  first node: priority 2
#  other node: priority 1
msInitRepl() {
  local slist=($(getInitNodeList))
  local cnt=${#slist[@]}
  
  local curmem=''
  local memberstr=''
  for((i=0; i<$cnt; i++)); do
    if [ $i -eq 0 ]; then
      curmem="{_id:$i,host:\"$(getIp ${slist[i]}):$MY_PORT\",priority: 2}"
    elif [ $i -eq $((cnt-1)) ]; then
      curmem="{_id:$i,host:\"$(getIp ${slist[i]}):$MY_PORT\",priority: 0, hidden: true}"
    else
      curmem="{_id:$i,host:\"$(getIp ${slist[i]}):$MY_PORT\",priority: 1}"
    fi
    
    memberstr="$memberstr$curmem,"
  done
  memberstr=${memberstr:0:-1}

  local initjs=''
  if [ $MY_ROLE = "cs_node" ]; then
    initjs=$(cat <<EOF
rs.initiate({
  _id:"$RS_NAME",
  configsvr: true,
  members:[$memberstr]
})
EOF
    )
  else
    initjs=$(cat <<EOF
rs.initiate({
  _id:"$RS_NAME",
  members:[$memberstr]
})
EOF
    )
  fi

  runMongoCmd "$initjs" -P $MY_PORT
}

msIsHostMaster() {
  local hostinfo=$1
  shift
  local tmpstr=$(runMongoCmd "JSON.stringify(rs.status().members)" $@)
  local state=$(echo $tmpstr | jq '.[] | select(.name=="'$hostinfo'") | .stateStr' | sed s/\"//g)
  test "$state" = "PRIMARY"
}

msIsHostSecondary() {
  local hostinfo=$1
  shift
  local tmpstr=$(runMongoCmd "JSON.stringify(rs.status().members)" $@)
  local state=$(echo $tmpstr | jq '.[] | select(.name=="'$hostinfo'") | .stateStr' | sed s/\"//g)
  test "$state" = "SECONDARY"
}

msIsHostHidden() {
  local hostinfo=$1
  shift
  local tmpstr=$(runMongoCmd "JSON.stringify(rs.conf().members)" $@)
  local pname=$(echo $tmpstr | jq '.[] | select(.hidden==true) | .host' | sed s/\"//g)
  test "$pname" = "$hostinfo"
}

msAddLocalSysUser() {
  local jsstr=$(cat <<EOF
admin = db.getSiblingDB("admin")
admin.createUser(
  {
    user: "$DB_QC_USER",
    pwd: "$(cat $DB_QC_LOCAL_PASS_FILE)",
    roles: [ { role: "root", db: "admin" },{ role: "__system", db: "admin" } ]
  }
)
EOF
  )
  runMongoCmd "$jsstr" -P $MY_PORT
}

msAddUserZabbix() {
  local zabbix_pass="$(getItemFromFile zabbix_pass $CONF_INFO_FILE)"
  local jsstr=$(cat <<EOF
admin = db.getSiblingDB("admin")
admin.createUser(
  {
    user: "$DB_ZABBIX_USER",
    pwd: "$zabbix_pass",
    roles: [ { role: "clusterMonitor", db: "admin" } ]
  }
)
EOF
  )
  runMongoCmd "$jsstr" $@
}

msAddUserRoot() {
  local user_pass="$(getItemFromFile user_pass $CONF_INFO_FILE)"
  local jsstr=$(cat <<EOF
admin = db.getSiblingDB("admin")
admin.createUser(
  {
    user: "root",
    pwd: "$user_pass",
    roles: [ { role: "root", db: "admin" } ]
  }
)
EOF
  )
  runMongoCmd "$jsstr" $@
}

# run at mongos
msAddShardNodeByGidList() {
  local glist=($(echo $@))
  local cnt=${#glist[@]}
  local subcnt
  local tmpstr
  local currepl
  local tmpip
  for((i=0;i<$cnt;i++)); do
    tmplist=($(eval echo \${INFO_SHARD_${glist[i]}_LIST[@]}))
    currepl=$(eval echo \$INFO_SHARD_${glist[i]}_RSNAME)
    subcnt=${#tmplist[@]}
    tmpstr="$tmpstr;sh.addShard(\"$currepl/"
    retry 60 3 0 msIsReplStatusOk $subcnt -H $(getIp ${tmplist[0]}) -P $INFO_SHARD_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
    for((j=0;j<$subcnt;j++)); do
      tmpip=$(getIp ${tmplist[j]})
      if msIsHostHidden "$tmpip:$INFO_SHARD_PORT" -H $tmpip -P $INFO_SHARD_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE); then continue; fi
      tmpstr="$tmpstr$(getIp ${tmplist[j]}):$INFO_SHARD_PORT,"
    done
    tmpstr="${tmpstr:0:-1}\")"
  done
  tmpstr="${tmpstr:1};"
  echo $tmpstr
  runMongoCmd "$tmpstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
}

msUpdateQingCloudControl() {
  local jsstr=$(cat <<EOF
cfg=db.getSiblingDB("config");
cfg.QingCloudControl.findAndModify(
  {
    query:{_id:"QcCtrlDoc"},
    update:{\$inc:{counter:1}},
    new: true,
    upsert: true,
    writeConcern:{w:"majority",wtimeout:15000}
  }
);
EOF
  )
  runMongoCmd "$jsstr" $@
}

doWhenReplInit() {
  if [ $MY_ROLE = "mongos_node" ]; then return 0; fi
  local slist=($(getInitNodeList))
  if [ ! $(getSid ${slist[0]}) = $MY_SID ]; then return 0; fi
  log "init replicaset begin ..."
  retry 60 3 0 msInitRepl
  retry 60 3 0 msIsReplStatusOk ${#NODE_LIST[@]} -P $MY_PORT
  retry 60 3 0 msIsHostMaster "$MY_IP:$MY_PORT" -P $MY_PORT
  log "add local sys user"
  retry 60 3 0 msAddLocalSysUser
  log "add zabbix user"
  retry 60 3 0 msAddUserZabbix -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  log "update QingCloudControl database"
  msUpdateQingCloudControl -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  log "init replicaset done"
}

doWhenMongosInit() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return 0; fi
  local slist=($(getInitNodeList))
  if [ ! $(getSid ${slist[0]}) = $MY_SID ]; then return 0; fi
  log "init shard cluster begin ..."
  retry 60 3 0 msGetHostDbVersion -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  retry 60 3 0 msAddShardNodeByGidList ${INFO_SHARD_GROUP_LIST[@]}
  log "add user: root"
  retry 60 3 0 runMongoCmd "sh.status()" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  retry 60 3 0 msAddUserRoot -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  log "init shard cluster done"
}

init() {
  doWhenReplInit
  doWhenMongosInit
  enableHealthCheck
}

isMeMaster() {
  msIsHostMaster "$MY_IP:$MY_PORT" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
}

isMeNotMaster() {
  ! msIsHostMaster "$MY_IP:$MY_PORT" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
}

msIsBalancerNotRunning() {
  local jsstr=$(cat <<EOF
if (sh.isBalancerRunning()) {
  quit(1)
}
else {
  quit(0)
}
EOF
  )

  runMongoCmd "$jsstr" $@
}

msIsBalancerOkForStop() {
  local jsstr=$(cat <<EOF
if (!sh.getBalancerState() && !sh.isBalancerRunning()) {
  quit(0)
}
else {
  quit(1)
}
EOF
  )

  runMongoCmd "$jsstr" $@
}

msDisableBalancer() {
  local tmpstr=$(runMongoCmd "JSON.stringify(sh.stopBalancer())" $@)
  local res=$(echo "$tmpstr" | jq '.ok')
  test $res = 1
}

doWhenMongosStop() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return 0; fi
  _stop
  if [ $VERTICAL_SCALING_FLAG = "true" ] || [ $DELETING_HOSTS_FLAG = "true" ]; then log "No need to stop balancer"; return 0; fi

  local slist=(${NODE_LIST[@]})
  local cnt=${#slist[@]}
  if [ $(getSid ${slist[0]}) = $MY_SID ]; then return 0; fi
  # last node check balancer's status
  shellStartMongosForAdmin
  retry 60 3 0 msGetHostDbVersion -P $NET_MAINTAIN_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  # wait for 30 minutes for balancer to be ready
  if retry 1800 3 0 msDisableBalancer -P $NET_MAINTAIN_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE); then
    log "disable balancer: succeeded"
    retry 1800 3 0 msIsBalancerOkForStop -P $NET_MAINTAIN_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
    msStopMongosForAdmin
  else
    log "disable balancer: failed"
    msStopMongosForAdmin
    
    return $ERR_BALANCER_STOP
  fi
}

doWhenReplStop() {
  if [ $MY_ROLE = "mongos_node" ]; then return 0; fi
  if isMeMaster; then
    runMongoCmd "rs.stepDown()" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE) || :
  fi
  # wait for 30 minutes
  retry 1800 3 0 isMeNotMaster
  _stop
  log "node stopped"
}

stopZabbixAgent() {
  systemctl stop zabbix-agent2.service || :
  log "zabbix-agent2 stopped"
}

stop() {
  disableHealthCheck
  doWhenMongosStop
  doWhenReplStop
  # stop zabbix-agent2
  stopZabbixAgent
}

getNodesOrder() {
  local tmpstr
  local cnt
  local subcnt
  local tmplist
  local tmpip
  local curmaster
  if [ "$MY_ROLE" = "mongos_node" ]; then
    tmplist=($(sortHostList ${NODE_LIST[@]}))
    cnt=${#tmplist[@]}
    for((i=0;i<$cnt;i++)); do
      tmpstr="$tmpstr,$(getNodeId ${tmplist[i]})"
    done
    tmpstr=${tmpstr:1}
  elif [ "$MY_ROLE" = "cs_node" ]; then
    tmplist=(${NODE_LIST[@]})
    cnt=${#tmplist[@]}
    for((i=0;i<$cnt;i++)); do
      tmpip=$(getIp ${tmplist[i]})
      if msIsHostMaster "$tmpip:$MY_PORT" -H $tmpip -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE); then
        curmaster=$(getNodeId ${tmplist[i]})
        continue
      fi
      tmpstr="$tmpstr,$(getNodeId ${tmplist[i]})"
    done
    tmpstr="${tmpstr:1},$curmaster"
  else
    cnt=${#INFO_SHARD_GROUP_LIST[@]}
    for((i=1;i<=$cnt;i++)); do
      tmplist=($(eval echo \${INFO_SHARD_${i}_LIST[@]}))
      subcnt=${#tmplist[@]}
      for((j=0;j<$subcnt;j++)); do
        tmpip=$(getIp ${tmplist[j]})
        if msIsHostMaster "$tmpip:$INFO_SHARD_PORT" -H $tmpip -P $INFO_SHARD_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE); then
          curmaster=$(getNodeId ${tmplist[j]})
          continue
        fi
        tmpstr="$tmpstr,$(getNodeId ${tmplist[j]})"
      done
      tmpstr="$tmpstr,$curmaster"
    done
    tmpstr=${tmpstr:1}
  fi
  log "$tmpstr"
  echo $tmpstr
}

doWhenScaleOutForMongos() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return; fi
  if [ $ADDING_ROLE = "shard_node" ]; then
    local sortlist=($(getInitNodeList))
    if [ $(getSid ${sortlist[0]}) = $MY_SID ]; then
      retry 60 3 0 msAddShardNodeByGidList ${ADDING_LIST[@]}
    fi
  fi
}

scaleOut() {
  doWhenScaleOutForMongos
  updateMongoConf
  updateHostsInfo
}

scaleInPreCheck() {
  if [ $DELETING_ROLE = "shard_node" ]; then
    return $ERR_SCALEIN_SHARD_FORBIDDEN
  else
    return 0
  fi
}

scaleIn() {
  :
}

clusterPreInit() {
  # folder
  mkdir -p $MONGODB_DATA_PATH $MONGODB_LOG_PATH $MONGODB_CONF_PATH
  chown -R mongod:svc $MONGODB_DATA_PATH $MONGODB_LOG_PATH $MONGODB_CONF_PATH
  chown -R zabbix:zabbix $ZABBIX_LOG_PATH
  # first create flag
  touch $NODE_FIRST_CREATE_FLAG_FILE
  # repl.key
  echo "$GLOBAL_UUID" | base64 > "$MONGODB_CONF_PATH/repl.key"
  chown mongod:svc $MONGODB_CONF_PATH/repl.key
  chmod 0400 $MONGODB_CONF_PATH/repl.key
  #qc_local_pass
  local encrypted=$(echo -n ${GLOBAL_UUID}${CLUSTER_ID} | sha256sum | base64)
  echo ${encrypted:16:16} > $DB_QC_LOCAL_PASS_FILE
  #create config files
  touch $MONGODB_CONF_PATH/mongo.conf
  chown mongod:svc $MONGODB_CONF_PATH/mongo.conf
  #disable health check
  disableHealthCheck
}

changeVxnetPreCheck() {
  local wantStr=$(echo "cs_node,mongos_node,shard_node,shard_node-replica" | sed 's/,/\n/g' | sort)
  local gotStr=$(echo $CHANGE_VXNET_ROLES | sed 's/,/\n/g' | sort)
  if [ ! "$gotStr" = "$wantStr" ]; then return $ERR_CHGVXNET_PRECHECK; fi
}

# check if node is scaling
# 1: scaleIn
# 0: no change
# 2: scaleOut
getScalingStatus() {
  local oldlist=($(getItemFromFile NODE_LIST $HOSTS_INFO_FILE))
  local newlist=($(getItemFromFile NODE_LIST $HOSTS_INFO_FILE.new))
  local oldcnt=${#oldlist[@]}
  local newcnt=${#newlist[@]}
  if (($oldcnt < $newcnt)); then
    echo 2
  elif (($oldcnt > $newcnt)); then
    echo 1
  else
    echo 0
  fi
}

doWhenMongosConfChanged() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return 0; fi
  local jsstr
  local tmpcnt
  local setParameter_cursorTimeoutMillis
  local user_pass
  local slist
  if ! diff $HOSTS_INFO_FILE $HOSTS_INFO_FILE.new; then
    # net.port, perhaps include setParameter.cursorTimeoutMillis
    # restart mongos
    log "$MY_ROLE: restart mongos.service"
    updateHostsInfo
    updateMongoConf
    systemctl restart mongos.service
  elif ! diff $CONF_INFO_FILE $CONF_INFO_FILE.new; then
    # change root's password
    tmpcnt=$(diff $CONF_INFO_FILE $CONF_INFO_FILE.new | grep user_pass | wc -l) || :
    if (($tmpcnt > 0)); then
      cat $CONF_INFO_FILE.new > $CONF_INFO_FILE
      slist=($(getInitNodeList))
      if [ ! $(getIp ${slist[0]}) = "$MY_IP" ]; then log "$MY_ROLE: skip changing user pass"; return 0; fi
      user_pass="$(getItemFromFile user_pass $CONF_INFO_FILE.new)"
      jsstr=$(cat <<EOF
admin = db.getSiblingDB("admin")
admin.changeUserPassword("root", "$user_pass")
EOF
      )
      runMongoCmd "$jsstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
      log "user root's password has been changed"
      return 0
    fi
    # setParameter.cursorTimeoutMillis
    log "$MY_ROLE: change cursorTimeoutMillis"
    updateMongoConf
    setParameter_cursorTimeoutMillis=$(getItemFromFile setParameter_cursorTimeoutMillis $CONF_INFO_FILE)
    jsstr=$(cat <<EOF
db.adminCommand({setParameter:1,cursorTimeoutMillis: $setParameter_cursorTimeoutMillis})
EOF
    )
    runMongoCmd "$jsstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  fi
}

isMongodNeedRestart() {
  local cnt=$(diff $CONF_INFO_FILE $CONF_INFO_FILE.new | grep replication_enableMajorityReadConcern | wc -l) || :
  if (($cnt > 0)); then return 0; else return 1; fi
}

# sort nodes for changing configue
# secodary node first, primary node last
getRollingList() {
  local cnt=${#NODE_LIST[@]}
  local tmpstr
  local master
  local ip
  for((i=0;i<$cnt;i++)); do
    ip=$(getIp ${NODE_LIST[i]})
    if msIsHostMaster "$ip:$MY_PORT" -H $ip -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE); then
      master=${NODE_LIST[i]}
      continue
    fi
    tmpstr="$tmpstr ${NODE_LIST[i]}"
  done
  tmpstr="$tmpstr $master"
  echo $tmpstr
}

msForceStepDown() {
  runMongoCmd "rs.stepDown()" $@ || :
}

getOperationProfilingModeCode() {
  local res
  case $1 in
    "off") res=0;;
    "slowOp") res=1;;
    "all") res=2;;
  esac
  echo $res
}

# change oplogSize
msReplChangeOplogSize() {
  local replication_oplogSizeMB=$(getItemFromFile replication_oplogSizeMB $CONF_INFO_FILE.new)
  local jsstr=$(cat <<EOF
db.adminCommand({replSetResizeOplog: 1, size: $replication_oplogSizeMB})
EOF
  )
  runMongoCmd "$jsstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  log "replication.oplogSizeMB changed"
}

# change zabbix_pass
msReplChangeZabbixPass() {
  if ! isMeMaster; then log "change zabbix_pass, skip"; return 0; fi
  local zabbix_pass="$(getItemFromFile zabbix_pass $CONF_INFO_FILE.new)"
  local jsstr=$(cat <<EOF
admin = db.getSiblingDB("admin")
admin.changeUserPassword("$DB_ZABBIX_USER", "$zabbix_pass")
EOF
  )
  runMongoCmd "$jsstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  log "user zabbix's password has been changed"
}

# change conf according to $CONF_INFO_FILE.new
msReplChangeConf() {
  local tmpcnt
  local jsstr
  local setParameter_cursorTimeoutMillis
  local operationProfiling_mode
  local operationProfiling_mode_code
  local operationProfiling_slowOpThresholdMs

  # zabbix_pass
  tmpcnt=$(diff $CONF_INFO_FILE $CONF_INFO_FILE.new | grep zabbix_pass | wc -l) || :
  if (($tmpcnt > 0)); then
    msReplChangeZabbixPass
    return 0
  fi

  # replication_oplogSizeMB
  tmpcnt=$(diff $CONF_INFO_FILE $CONF_INFO_FILE.new | grep oplogSizeMB | wc -l) || :
  if (($tmpcnt > 0)); then
    msReplChangeOplogSize
  fi

  # setParameter_cursorTimeoutMillis
  tmpcnt=$(diff $CONF_INFO_FILE $CONF_INFO_FILE.new | grep setParameter | wc -l) || :
  if (($tmpcnt > 0)); then
    setParameter_cursorTimeoutMillis=$(getItemFromFile setParameter_cursorTimeoutMillis $CONF_INFO_FILE.new)
    jsstr=$(cat <<EOF
db.adminCommand({setParameter:1,cursorTimeoutMillis:$setParameter_cursorTimeoutMillis})
EOF
    )
    runMongoCmd "$jsstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
    log "setParameter.cursorTimeoutMillis changed"
  fi

  # operationProfiling_mode
  # operationProfiling_slowOpThresholdMs
  tmpcnt=$(diff $CONF_INFO_FILE $CONF_INFO_FILE.new | grep operationProfiling | wc -l) || :
  if (($tmpcnt > 0)); then
    operationProfiling_mode=$(getItemFromFile operationProfiling_mode $CONF_INFO_FILE.new)
    operationProfiling_slowOpThresholdMs=$(getItemFromFile operationProfiling_slowOpThresholdMs $CONF_INFO_FILE.new)
    operationProfiling_mode_code=$(getOperationProfilingModeCode $operationProfiling_mode)
    jsstr=$(cat <<EOF
rs.slaveOk();
var dblist=db.adminCommand('listDatabases').databases;
var tmpdb;
for (i=0;i<dblist.length;i++) {
  tmpdb=db.getSiblingDB(dblist[i].name);
  tmpdb.setProfilingLevel($operationProfiling_mode_code, { slowms: $operationProfiling_slowOpThresholdMs });
}
EOF
    )
    runMongoCmd "$jsstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
    log "operationProfiling changed"
  fi
}

doWhenReplConfChanged() {
  if [ $MY_ROLE = "mongos_node" ]; then return 0; fi
  if diff $CONF_INFO_FILE $CONF_INFO_FILE.new; then return 0; fi
  local rlist=($(getRollingList))
  local cnt=${#rlist[@]}
  local tmpcnt
  local tmpip
  tmpip=$(getIp ${rlist[0]})
  if [ ! $tmpip = "$MY_IP" ]; then log "$MY_ROLE: skip changing configue"; return 0; fi

  if isMongodNeedRestart; then
    # oplogSizeMB check first
    tmpcnt=$(diff $CONF_INFO_FILE $CONF_INFO_FILE.new | grep oplogSizeMB | wc -l) || :
    if (($tmpcnt > 0)); then
      for((i=0;i<$cnt;i++)); do
        tmpip=$(getIp ${rlist[i]})
        ssh root@$tmpip "appctl msReplChangeOplogSize"
      done
    fi

    log "rolling restart mongod.service"
    for((i=0;i<$cnt;i++)); do
      tmpip=$(getIp ${rlist[i]})
      if msIsHostMaster "$tmpip:$MY_PORT" -H $tmpip -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE); then
        msForceStepDown -H $tmpip -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
      fi
      ssh root@$tmpip "appctl updateMongoConf && systemctl restart mongod.service"
      retry 60 3 0 msIsReplStatusOk $cnt -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
    done
  else
    for((i=0;i<$cnt;i++)); do
      tmpip=$(getIp ${rlist[i]})
      ssh root@$tmpip "appctl msReplChangeConf && appctl updateMongoConf"
    done
  fi
}

doWhenZabbixConfChanged() {
  if diff $CONF_ZABBIX_INFO_FILE $CONF_ZABBIX_INFO_FILE.new; then return 0; fi
  updateZabbixConf
  refreshZabbixAgentStatus
}

checkConfdChange() {
  if [ ! -d /data/appctl/logs ]; then
    log "cluster pre-init"
    clusterPreInit
    return 0
  fi

  if [ -f $BACKUP_FLAG_FILE ]; then
    log "restore from backup, skipping"
    return 0
  fi

  if [ $VERTICAL_SCALING_FLAG = "true" ] || [ $ADDING_HOSTS_FLAG = "true" ] || [ $DELETING_HOSTS_FLAG = "true" ] || [ $CHANGE_VXNET_FLAG = "true" ]; then return 0; fi
  local sstatus=$(getScalingStatus)
  case $sstatus in
    "0") :;;
    "1") updateHostsInfo; return 0;;
    "2") return 0;;
  esac
  
  doWhenMongosConfChanged
  doWhenReplConfChanged

  doWhenZabbixConfChanged
}

msGetServerStatus() {
  local tmpstr=$(runMongoCmd "JSON.stringify(db.serverStatus())" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE))
  echo "$tmpstr"
}

msGetServerStatusForMonitor() {
  local tmpstr=$(runMongoCmd "JSON.stringify(db.serverStatus({\"repl\":1}))" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE))
  echo "$tmpstr"
}

# remove " from jq results
moRmQuotation() {
  echo $@ | sed 's/"//g'
}

# remove " from jq results and calculate MB value
moRmQuotationMB() {
  local tmpstr=$(echo $@ | sed 's/"//g')
  echo "scale=0;$tmpstr/1024/1024" | bc
}

# calculate timespan, unit: minute
# scale_factor_when_display=0.1
moCalcTimespan() {
  local tmpstr=$(echo $@)
  local res=$(echo "scale=0;($tmpstr)/6" | sed 's/ /-/g' | bc | sed 's/-//g')
  echo $res
}

# unit "%"
# scale_factor_when_display=0.1
moCalcPer() {
  local type=$1
  shift
  local tmpstr=$(echo $@ | sed 's/"//g')
  local divisor
  local dividend
  local res
  if [ "$type" = 1 ]; then
    divisor=${tmpstr% *}
    dividend=${tmpstr##* }
    res=$(echo "scale=0;($divisor)*1000/$dividend" | sed 's/ /+/g' | bc)
  else
    divisor=${tmpstr%% *}
    dividend=${tmpstr#* }
    res=$(echo "scale=0;$divisor*1000/($dividend)" | sed 's/ /+/g' | bc)
  fi
  echo $res
}

getMonFilePath() {
  local monpath
  if [ $MY_ROLE = "mongos_node" ]; then
    monpath="$MONGOS_MONITOR_ITEM_FILE"
  elif [ $MY_ROLE = "cs_node" ]; then
    monpath="$CS_MONITOR_ITEM_FILE"
  else
    monpath="$SHARD_MONITOR_ITEM_FILE"
  fi
  echo $monpath
}

monitor() {
  if ! isSingleThread monitor; then log "a monitor is already running!"; return 1; fi
  local monpath=$(getMonFilePath)
  local serverStr=$(msGetServerStatusForMonitor)
  local tmpstr
  local res
  local pipestr
  local pipep
  local title
  while read line; do
    title=$(echo $line | cut -d'/' -f1)
    pipestr=$(echo $line | cut -d'/' -f2)
    pipep=$(echo $line | cut -d'/' -f3)
    tmpstr=$(echo "$serverStr" |jq "$pipep")
    if [ ! -z "$pipestr" ]; then
      tmpstr=$(eval $pipestr $tmpstr)
    fi
    res="$res,\"$title\":$tmpstr"
  done < $monpath
  echo "{${res:1}}"
}

healthCheck() {
  if ! needHealthCheck; then log "skip health check"; return 0; fi
  if ! isSingleThread healthCheck; then log "a health check is already running!"; return 1; fi
  local srv=$(echo $SERVICES | cut -d'/' -f1).service
  local port=$(echo $SERVICES | cut -d':' -f2)
  if ! systemctl is-active $srv -q; then
    log "$srv has stopped!"
    return $ERR_SERVICE_STOPPED
  fi
  if [ ! $(lsof -b -i -s TCP:LISTEN | grep ':'$port | wc -l) = "1" ]; then
    log "port $port is not listened!"
    return $ERR_PORT_NOT_LISTENED
  fi

  if [ ! $MY_ROLE = "mongos_node" ]; then
    if ! msIsReplStatusOk ${#NODE_LIST[@]} -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE); then
      log "replica cluster is not health"
      return $ERR_REPL_NOT_HEALTH
    fi
  fi
  return 0
}

revive() {
  if ! isSingleThread revive; then log "a revive is already running!"; return 1; fi
  log "invoke revive"
  local srv=$(echo $SERVICES | cut -d'/' -f1).service
  local port=$(echo $SERVICES | cut -d':' -f2)
  if ! systemctl is-active $srv -q; then
    systemctl restart $srv
    log "$srv has been restarted!"
  else
    if [ ! $MY_ROLE = "mongos_node" ]; then
      if [ ! $(lsof -b -i -s TCP:LISTEN | grep ':'$port | wc -l) = "1" ]; then
        log "port $port is not listened! do nothing"
      elif msIsReplOther -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE); then
        systemctl restart $srv
        log "status: OTHER, $srv has been restarted!"
      else
        log "status: NOT OTHER, do nothing"
      fi
    fi
  fi
}

doWhenBackupMongos() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return 0; fi
  local ip=$(getIp ${NODE_LIST[0]})
  # select only one node
  if [ ! $ip = "$MY_IP" ]; then return 0; fi
  # stop balancer
  if retry 1800 3 0 msDisableBalancer -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE); then
    log "disable balancer: succeeded"
    retry 1800 3 0 msIsBalancerOkForStop -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  else
    log "disable balancer: failed"
    return $ERR_BALANCER_STOP
  fi
}

doWhenCleanupMongos() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return 0; fi
  local ip=$(getIp ${NODE_LIST[0]})
  # select only one node
  if [ ! $ip = "$MY_IP" ]; then return 0; fi
  # start balancer
  retry 1800 3 0 msEnableBalancer -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
}

doWhenGetBackupNodeIdMongos() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return 0; fi
  local ip=$(getIp ${NODE_LIST[0]})
  # select only one node
  if [ ! $ip = "$MY_IP" ]; then return 0; fi
  echo $(getNodeId ${NODE_LIST[0]})
}

doWhenGetBackupNodeIdCs() {
  if [ ! $MY_ROLE = "cs_node" ]; then return 0; fi
  local ip=$(getIp ${NODE_LIST[0]})
  # select only one node
  if [ ! $ip = "$MY_IP" ]; then return 0; fi
  local cnt=${#NODE_LIST[@]}
  local tmpip
  local res
  for((i=0;i<$cnt;i++)); do
    tmpip=$(getIp ${NODE_LIST[i]})
    if msIsHostHidden "$tmpip:$MY_PORT" -H $MY_IP -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE) >/dev/null 2>&1; then
      res=$(getNodeId ${NODE_LIST[i]})
      break
    fi
  done
  echo $res
}

doWhenGetBackupNodeIdShard() {
  if [ $MY_ROLE = "cs_node" ] || [ $MY_ROLE = "mongos_node" ]; then return 0; fi
  if [ ! $MY_ROLE = "shard_node" ]; then return 0; fi
  local ip=$(getIp ${SHARD_NODE_LIST[0]})
  if [ ! $ip = "$MY_IP" ]; then return 0; fi
  local cnt=${#SHARD_NODE_LIST[@]}
  local tmpstr=""
  local tmpip
  for((i=0;i<$cnt;i++)); do
    tmpstr="$tmpstr,$(getNodeId ${SHARD_NODE_LIST[i]})"
  done
  tmpstr="${tmpstr:1}"
  echo $tmpstr
}

getBackupNodeId() {
  log "info"
  doWhenGetBackupNodeIdMongos
  doWhenGetBackupNodeIdCs
  doWhenGetBackupNodeIdShard
}

backup() {
  log "info"
  # set backup flag
  touch $BACKUP_FLAG_FILE

  # back qc_master's old password
  cp $DB_QC_LOCAL_PASS_FILE $DB_QC_LOCAL_PASS_FILE.old
  
  # stuff for backup preparation
  doWhenBackupMongos
}

cleanup() {
  log "info"
  # reset backup flag
  rm -rf $BACKUP_FLAG_FILE

  # stuff for backup cleanup
  doWhenCleanupMongos
}

preRestore() {
  disableHealthCheck
  if [ $MY_ROLE = "mongos_node" ]; then
    systemctl stop mongos.service || :
  else
    systemctl stop mongod.service || :
  fi
  # repl.key
  echo "$GLOBAL_UUID" | base64 > "$MONGODB_CONF_PATH/repl.key"
  chown mongod:svc $MONGODB_CONF_PATH/repl.key
  chmod 0400 $MONGODB_CONF_PATH/repl.key
  #qc_local_pass
  encrypted=$(echo -n ${GLOBAL_UUID}${CLUSTER_ID} | sha256sum | base64)
  echo ${encrypted:16:16} > $DB_QC_LOCAL_PASS_FILE
}

getShardReplName() {
  local glist=($(getItemFromFile SHARD_GROUP_LIST $HOSTS_INFO_FILE.new))
  local gid=$(getGid $1)
  local cnt=${#glist[@]}
  local res
  for((i=0;i<$cnt;i++)); do
    if [ ${glist[i]} = $gid ]; then
      res=$i
      break
    fi
  done
  echo "shard_$res"
}

restoreCsShardInfo() {
  if [ ! $MY_ROLE = "cs_node" ]; then return 0; fi
  local tmpstr
  local tmprepl
  local newshardlist=($(getItemFromFile EXT_LIST $HOSTS_INFO_FILE.new))
  local newport=$(getItemFromFile EXT_PORT $HOSTS_INFO_FILE.new)
  local cnt=${#newshardlist[@]}
  for((i=0;i<$cnt;i+=3)); do
    tmpstr=""
    tmprepl=$(getShardReplName ${newshardlist[i]})
    for((j=0;j<2;j++)); do
      tmpstr="$tmpstr,$(getIp ${newshardlist[$((i+j))]}):$newport"
    done
    tmpstr="$tmprepl/${tmpstr:1}"
    jsstr=$(cat <<EOF
mydb = db.getSiblingDB('config')
mydb.shards.updateOne({"_id": "$tmprepl"}, {\$set: {"host": "$tmpstr"}})
EOF
    )
    runMongoCmd "$jsstr" $@
  done
}

restoreShardShardInfo() {
  if [ $MY_ROLE = "cs_node" ]; then return 0; fi
  local tmpstr
  local newshardlist=($(getItemFromFile EXT_LIST $HOSTS_INFO_FILE.new))
  local newport=$(getItemFromFile EXT_PORT $HOSTS_INFO_FILE.new)
  local cnt=${#newshardlist[@]}
  for((i=0;i<2;i++)); do
    tmpstr="$tmpstr,$(getIp ${newshardlist[i]}):$newport"
  done
  tmpstr="repl_cs/${tmpstr:1}"
  jsstr=$(cat <<EOF
mydb = db.getSiblingDB('admin')
mydb.system.version.updateOne({"_id": "shardIdentity"}, {\$set: {"configsvrConnectionString": "$tmpstr"}})
EOF
  )
  runMongoCmd "$jsstr" $@
}

isValidShardRestore() {
  if [ $MY_ROLE = "cs_node" ]; then return 0; fi
  test $(getItemFromFile replication_replSetName $CONF_INFO_FILE.new) = $(getItemFromFile replication_replSetName $CONF_INFO_FILE)
}

doWhenRestoreRepl() {
  if [ $MY_ROLE = "mongos_node" ]; then return 0; fi
  if ! isValidShardRestore; then return $ERR_NOTVALID_SHARD_RESTORE; fi
  # sync from host.info.new & recreate mongo conf
  updateHostsInfo
  updateMongoConf

  local cnt=${#NODE_LIST[@]}
  local ip=$(getIp ${NODE_LIST[0]})
  if [ ! $ip = "$MY_IP" ]; then
    rm -rf $MONGODB_DATA_PATH/*
    _start
    return 0
  fi

  # start mongod in admin mode
  shellStartMongodForAdmin

  local jsstr
  retry 60 3 0 msGetHostDbVersion -P $NET_MAINTAIN_PORT

  # change qc_master and zabbix's passowrd
  local newpass=$(cat $DB_QC_LOCAL_PASS_FILE)
  local zabbix_pass=$(getItemFromFile zabbix_pass $CONF_INFO_FILE.new)
  jsstr=$(cat <<EOF
mydb = db.getSiblingDB("admin");
mydb.changeUserPassword("$DB_QC_USER", "$newpass");
mydb.changeUserPassword("$DB_ZABBIX_USER", "$zabbix_pass");
EOF
  )
  runMongoCmd "$jsstr" -P $NET_MAINTAIN_PORT

  # drop local database
  jsstr=$(cat <<EOF
mydb = db.getSiblingDB("local");
mydb.dropDatabase();
EOF
  )
  runMongoCmd "$jsstr" -P $NET_MAINTAIN_PORT

  restoreCsShardInfo -P $NET_MAINTAIN_PORT
  restoreShardShardInfo -P $NET_MAINTAIN_PORT

  # stop mongod in admin mode
  shellStopMongodForAdmin

  # start mongod in normal mode
  _start

  # waiting for mongod status ok
  retry 60 3 0 msGetHostDbVersion -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)

  # init repl
  jsstr=$(cat <<EOF
rs.initiate(
  {
    _id: "$RS_NAME",
    members:[{_id: 0, host: "$MY_IP:$MY_PORT", priority: 2}]
  }
);
EOF
  )
  runMongoCmd "$jsstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)

  # add other members
  cnt=${#NODE_LIST[@]}
  jsstr=""
  for((i=1;i<$cnt;i++)); do
    if [ $i -eq $((cnt-1)) ]; then
      tmpstr="{host:\"$(getIp ${NODE_LIST[i]}):$MY_PORT\",priority: 0, hidden: true}"
    else
      tmpstr="{host:\"$(getIp ${NODE_LIST[i]}):$MY_PORT\",priority: 1}"
    fi
    jsstr="$jsstr;rs.add($tmpstr)"
  done
  jsstr="${jsstr:1};"
  runMongoCmd "$jsstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
}

doWhenRestoreMongos() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return 0; fi
  # sync from host.info.new & recreate mongo conf
  updateHostsInfo
  updateMongoConf

  _start
  local slist=($(getInitNodeList))
  if [ ! $MY_IP = $(getIp ${slist[0]}) ]; then return 0; fi
  # waiting for mongos to be ready
  retry 60 3 0 msGetHostDbVersion -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  # change user root's password
  local user_pass="$(getItemFromFile user_pass $CONF_INFO_FILE.new)"
  jsstr=$(cat <<EOF
admin = db.getSiblingDB("admin")
admin.changeUserPassword("root", "$user_pass")
EOF
  )
  runMongoCmd "$jsstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  log "user root's password has been changed"

  # start balancer
  msEnableBalancer -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
}

msReplOnlyChangeConf() {
  local jsstr
  local setParameter_cursorTimeoutMillis
  local operationProfiling_mode
  local operationProfiling_mode_code
  local operationProfiling_slowOpThresholdMs
  setParameter_cursorTimeoutMillis=$(getItemFromFile setParameter_cursorTimeoutMillis $CONF_INFO_FILE.new)
  jsstr=$(cat <<EOF
db.adminCommand({setParameter:1,cursorTimeoutMillis:$setParameter_cursorTimeoutMillis})
EOF
  )
  runMongoCmd "$jsstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  log "setParameter.cursorTimeoutMillis changed"

  operationProfiling_mode=$(getItemFromFile operationProfiling_mode $CONF_INFO_FILE.new)
  operationProfiling_slowOpThresholdMs=$(getItemFromFile operationProfiling_slowOpThresholdMs $CONF_INFO_FILE.new)
  operationProfiling_mode_code=$(getOperationProfilingModeCode $operationProfiling_mode)
  jsstr=$(cat <<EOF
rs.slaveOk();
var dblist=db.adminCommand('listDatabases').databases;
var tmpdb;
for (i=0;i<dblist.length;i++) {
tmpdb=db.getSiblingDB(dblist[i].name);
tmpdb.setProfilingLevel($operationProfiling_mode_code, { slowms: $operationProfiling_slowOpThresholdMs });
}
EOF
  )
  runMongoCmd "$jsstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  log "operationProfiling changed"
}

doWhenReplPostRestore() {
  if [ $MY_ROLE = "mongos_node" ]; then return 0; fi
  # waiting for 24 hours to restore data
  local cnt=${#NODE_LIST[@]}
  retry 86400 3 0 msIsReplStatusOk $cnt -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  sleep 5s

  local rlist=($(getRollingList))
  local tmpip=$(getIp ${rlist[0]})
  cnt=${#rlist[@]}
  if [ ! $tmpip = "$MY_IP" ]; then log "$MY_ROLE: skip changing configue"; return 0; fi
  # change oplog
  for((i=0;i<$cnt;i++)); do
    tmpip=$(getIp ${rlist[i]})
    ssh root@$tmpip "appctl msReplChangeOplogSize"
  done
  # change other configure
  for((i=0;i<$cnt;i++)); do
    tmpip=$(getIp ${rlist[i]})
    ssh root@$tmpip "appctl msReplOnlyChangeConf"
  done
}

doWhenMongosPostRestore() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return 0; fi
  # waiting for mongos to be ready
  retry 60 3 0 msGetHostDbVersion -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  # change configure
  local jsstr
  local setParameter_cursorTimeoutMillis

  setParameter_cursorTimeoutMillis=$(getItemFromFile setParameter_cursorTimeoutMillis $CONF_INFO_FILE.new)
  jsstr=$(cat <<EOF
db.adminCommand({setParameter:1,cursorTimeoutMillis: $setParameter_cursorTimeoutMillis})
EOF
  )
  runMongoCmd "$jsstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  log "$MY_ROLE: change cursorTimeoutMillis"
}

postRestore() {
  doWhenReplPostRestore
  doWhenMongosPostRestore
  rm -rf $BACKUP_FLAG_FILE
  enableHealthCheck
  # refresh zabbix's status
  updateZabbixConf
  refreshZabbixAgentStatus
}

restore() {
  preRestore
  doWhenRestoreMongos
  doWhenRestoreRepl
  postRestore
}
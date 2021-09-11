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

getItemFromFile() {
  local res=$(cat $2 | sed '/^'$1'=/!d;s/.*=//')
  echo "$res"
}

start() {
  _start
  log "service started"
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

msIsHostMaster() {
  local hostinfo=$1
  shift
  local tmpstr=$(runMongoCmd "JSON.stringify(rs.status().members)" $@)
  local pname=$(echo $tmpstr | jq '.[] | select(.stateStr=="PRIMARY") | .name' | sed s/\"//g)
  test "$pname" = "$hostinfo"
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
    roles: [ { role: "root", db: "admin" } ]
  }
)
EOF
  )
  runMongoCmd "$jsstr" -P $MY_PORT
}

msInitShardCluster() {
  local cnt=${#INFO_SHARD_GROUP_LIST[@]}
  local subcnt
  local tmpstr
  local currepl
  local tmplist
  local tmpip
  for((i=1;i<=$cnt;i++)); do
    tmplist=($(eval echo \${INFO_SHARD_${i}_LIST[@]}))
    currepl=$(eval echo \$INFO_SHARD_${i}_RSNAME)
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
  
  runMongoCmd "$tmpstr" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
}

msGetHostDbVersion() {
  local jsstr=$(cat <<EOF
db.version()
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
  log "init replicaset done"
}

doWhenMongosInit() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return 0; fi
  local slist=($(getInitNodeList))
  if [ ! $(getSid ${slist[0]}) = $MY_SID ]; then return 0; fi
  log "init shard cluster begin ..."
  retry 60 3 0 msGetHostDbVersion -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
  retry 60 3 0 msInitShardCluster
  log "init shard cluster done"
}

init() {
  doWhenReplInit
  doWhenMongosInit
}

isMeMaster() {
  msIsHostMaster "$MY_IP:$MY_PORT" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE)
}

doWhenMongosStop() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return 0; fi
  _stop
}

msCheckReplSingleMaster() {
  local allcnt=$1
  shift
  local tmpstr=$(runMongoCmd "JSON.stringify(rs.status())" $@ | jq .members[].stateStr)
  local pcnt=$(echo "$tmpstr" | grep SECONDARY | wc -l)
  local scnt=$(echo "$tmpstr" | grep "not reachable/healthy" | wc -l)
  test $pcnt -eq 1
  test $((pcnt+scnt)) -eq $allcnt
  return 0
}

doWhenReplStop() {
  if [ $MY_ROLE = "mongos_node" ]; then return 0; fi
  if isMeMaster; then log "stop primary node"; retry 60 3 0 msCheckReplSingleMaster; fi
  _stop
  log "node stopped"
}

stop() {
  doWhenMongosStop
  doWhenReplStop
}

getNodesOrder() {
  if [ "$MY_ROLE" = "mongos_node" ]; then return 0; fi
  local tmpstr
  local cnt
  local subcnt
  local tmplist
  local tmpip
  local curmaster
  if [ "$MY_ROLE" = "cs_node" ]; then
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
security:
  keyFile: $MONGODB_CONF_PATH/repl.key
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
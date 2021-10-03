# sourced by /opt/app/current/bin/ctl.sh
# error code
ERR_BALANCER_STOP=201
ERR_CHGVXNET_PRECHECK=202
ERR_SCALEIN_SHARD_FORBIDDEN=203

# path info
MONGODB_DATA_PATH=/data/mongodb-data
MONGODB_LOG_PATH=/data/mongodb-logs
MONGODB_CONF_PATH=/data/mongodb-conf
DB_QC_CLUSTER_PASS_FILE=/data/appctl/data/qc_cluster_pass
DB_QC_LOCAL_PASS_FILE=/data/appctl/data/qc_local_pass
HOSTS_INFO_FILE=/data/appctl/data/hosts.info
CONF_INFO_FILE=/data/appctl/data/conf.info
NODE_FIRST_CREATE_FLAG_FILE=/data/appctl/data/node.first.create.flag
REPL_MONITOR_ITEM_FILE=/opt/app/current/bin/node/repl.monitor

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

isNodeFirstCreate() {
  test -f $NODE_FIRST_CREATE_FLAG_FILE
}

clearNodeFirstCreateFlag() {
  if [ -f $NODE_FIRST_CREATE_FLAG_FILE ]; then rm -f $NODE_FIRST_CREATE_FLAG_FILE; fi
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
  runuser mongod -g svc -s "/bin/bash" -c "$MONGOD_BIN -f $MONGODB_CONF_PATH/mongo-admin.conf"
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

start() {
  doWhenMongosPreStart
  doWhenReplPreStart
  # updat conf files
  updateHostsInfo
  updateMongoConf
  _start
  clearNodeFirstCreateFlag
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
    roles: [ { role: "root", db: "admin" } ]
  }
)
EOF
  )
  runMongoCmd "$jsstr" -P $MY_PORT
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
  retry 60 3 0 msAddShardNodeByGidList ${INFO_SHARD_GROUP_LIST[@]}
  log "init shard cluster done"
}

init() {
  doWhenReplInit
  doWhenMongosInit
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

stop() {
  doWhenMongosStop
  doWhenReplStop
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
  # first create flag
  touch $NODE_FIRST_CREATE_FLAG_FILE
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
  touch $MONGODB_CONF_PATH/mongo.conf
  chown mongod:svc $MONGODB_CONF_PATH/mongo.conf
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
  local setParameter_cursorTimeoutMillis
  if ! diff $HOSTS_INFO_FILE $HOSTS_INFO_FILE.new; then
    # net.port, perhaps include setParameter.cursorTimeoutMillis
    # restart mongos
    log "$MY_ROLE: restart mongos.service"
    updateHostsInfo
    updateMongoConf
    systemctl restart mongos.service
  elif ! diff $CONF_INFO_FILE $CONF_INFO_FILE.new; then
    # only setParameter.cursorTimeoutMillis
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

# change conf according to $CONF_INFO_FILE.new
msReplChangeConf() {
  local tmpcnt
  local jsstr
  local setParameter_cursorTimeoutMillis
  local operationProfiling_mode
  local operationProfiling_mode_code
  local operationProfiling_slowOpThresholdMs

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
  local tmpip
  tmpip=$(getIp ${rlist[0]})
  if [ ! $tmpip = "$MY_IP" ]; then log "$MY_ROLE: skip changing configue"; return 0; fi

  if isMongodNeedRestart; then
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

checkConfdChange() {
  if [ ! -d /data/appctl/logs ]; then
    log "cluster pre-init"
    clusterPreInit
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
}

msGetServerStatus() {
  local tmpstr=$(runMongoCmd "JSON.stringify(db.serverStatus())" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE))
  echo "$tmpstr"
}

# calculate replLag, unit: minute
# secondary's optime - primary's optime
# if cluster's status is not ok (1 primary, 2 secondary) 
#  replLag is set to ''
# scale_factor_when_display=0.1
monitorGetReplLag() {
  local tmpstr=$(runMongoCmd "JSON.stringify(rs.status().members)" -P $MY_PORT -u $DB_QC_USER -p $(cat $DB_QC_LOCAL_PASS_FILE))
  local res
  local timepri
  if isMeMaster; then
    res=0
  else
    timepri=$(echo $tmpstr | jq '.[] | select(.stateStr=="PRIMARY") | .optime.ts."$timestamp".t')
    timeme=$(echo $tmpstr | jq '.[] | select(.name=="'$MY_IP':'$MY_PORT'") | .optime.ts."$timestamp".t')
    if [ -z "$timepri" ] || [ -z "$timeme" ]; then
      res=""
    else
      res=$(echo "scale=0;($timeme-$timepri)/6" | bc)
    fi
  fi
  echo "\"repl-lag\":$res"
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

doWhenMonitorMongos() {
  if [ ! $MY_ROLE = "mongos_node" ]; then return 0; fi
}

doWhenMonitorRepl() {
  if [ $MY_ROLE = "mongos_node" ]; then return 0; fi
  local serverStr=$(msGetServerStatus)
  local cnt=${#milist[@]}
  local tmpstr
  local res
  local pipestr
  local pipep
  local title
  while read line; do
    title=$(echo $line | cut -d'|' -f1)
    pipestr=$(echo $line | cut -d'|' -f2)
    pipep=$(echo $line | cut -d'|' -f3)
    tmpstr=$(echo "$serverStr" |jq "$pipep")
    if [ ! -z "$pipestr" ]; then
      tmpstr=$(eval $pipestr $tmpstr)
    fi
    res="$res,\"$title\":$tmpstr"
  done < $REPL_MONITOR_ITEM_FILE
  # conn-usage
  #res="$res,$(monitorGetConnUsage {${res:1}})"
  # repl-lag
  #res="$res,$(monitorGetReplLag)"
  echo "{${res:1}}"
}

monitor() {
  doWhenMonitorMongos
  doWhenMonitorRepl
}
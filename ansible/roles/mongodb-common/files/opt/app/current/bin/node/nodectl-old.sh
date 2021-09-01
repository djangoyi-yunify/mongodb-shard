# sourced by /opt/app/current/bin/ctl.sh

MONGODB_DATA_PATH=/data/mongodb-data
MONGODB_LOG_PATH=/data/mongodb-logs
MONGODB_CONF_PATH=/data/mongodb-conf
HOSTS_INFO_FILE=/data/appctl/data/hosts.info
CONFD_MONGOD_FILE=/data/appctl/data/confd.mongod

# error code
ERR_GET_PRIMARY_IP=130

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

# createMongodCfg
# get item from ini-like files
# $1: item-name
# $2: file path
getItemFromFile() {
  local res=$(cat $2 | sed '/^'$1'=/!d;s/.*=//')
  echo "$res"
}

# createMongodCfg
# desc: create mongod.conf
# file: mongod.conf, mongod-admin.conf
# location: MONGODB_CONF_PATH=/data/mongodb-conf
createMongodConf() {
  local engine=$(getItemFromFile ENGINE $CONFD_MONGOD_FILE)
  local port=$(getItemFromFile PORT $CONFD_MONGOD_FILE)
  local oplogsize=$(getItemFromFile replication_oplogSizeMB $CONFD_MONGOD_FILE)
  cat > $MONGODB_CONF_PATH/mongod.conf <<MONGOD_CONF
systemLog:
  destination: file
  path: $MONGODB_LOG_PATH/mongod.log
  logAppend: true
  logRotate: reopen
net:
  port: $port
  bindIp: 0.0.0.0
  maxIncomingConnections: 51200
security:
  keyFile: $MONGODB_CONF_PATH/repl.key
  authorization: enabled
storage:
  dbPath: $MONGODB_DATA_PATH
  journal:
    enabled: true
  engine: $engine
operationProfiling:
  slowOpThresholdMs: 200
replication:
  oplogSizeMB: $oplogsize
  replSetName: $RS_NAME
MONGOD_CONF

  cat > $MONGODB_CONF_PATH/mongod-admin.conf <<MONGOD_CONF
systemLog:
  destination: syslog
net:
  port: ${CONF_MAINTAIN_NET_PORT}
  bindIp: 0.0.0.0
storage:
  dbPath: $MONGODB_DATA_PATH
  engine: $engine
processManagement:
   fork: true
MONGOD_CONF
}

createMongoShakeConf() {
  if [ ! -f $MONGOSHAKE_HOSTS_FILE ]; then
    cat $MONGOSHAKE_HOSTS_FILE.new > $MONGOSHAKE_HOSTS_FILE
  fi
  log "create conf file"
}

NODECTL_MANUAL_FILE="/data/appctl/data/nodectl.manual"
NODECTL_TRANS_FILE="/data/appctl/data/nodectl.trans"
DB_QC_PASS_FILE="/data/appctl/data/qc_pass"
initCluster() {
  _initCluster

  # folder
  mkdir -p $MONGODB_DATA_PATH $MONGODB_LOG_PATH
  chown -R mongod:svc $MONGODB_DATA_PATH $MONGODB_LOG_PATH
  # repl.key
  echo "$GLOBAL_UUID" | base64 > "$MONGODB_CONF_PATH/repl.key"
  chown mongod:svc $MONGODB_CONF_PATH/repl.key
  chmod 0400 $MONGODB_CONF_PATH/repl.key
  #qc_pass
  local encrypted=$(echo -n ${CLUSTER_ID}${GLOBAL_UUID} | sha256sum | base64)
  echo ${encrypted:0:16} > $DB_QC_PASS_FILE
}

initNode() {
  # lxc check
  systemd-detect-virt -cq && test -d /data
  _initNode
}

updateHostInfo() {
  cat $HOSTS_INFO_FILE.new > $HOSTS_INFO_FILE
}

updateConfdMongodParam() {
  cat $CONFD_MONGOD_FILE.new > $CONFD_MONGOD_FILE
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

# sortHostList
# input
#  $1-n: hosts array
# output
#  sorted array, like 'v1 v2 v3 ...'
sortHostList() {
  echo $@ | tr ' ' '\n' | sort
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
  local slist=($(sortHostList ${NODE_CUR_LIST[@]}))
  local cnt=${#slist[@]}
  # only first node can do init action
  [ $(getSid ${slist[0]}) = $MY_SID ] || return 0
  
  local curmem=''
  local memberstr=''
  for((i=0; i<$cnt; i++)); do
    if [ $i -eq 0 ]; then
      curmem="{_id:$i,host:\"$(getIp ${slist[i]}):$CONF_NET_PORT\",priority: 2}"
    elif [ $i -eq $((cnt-1)) ]; then
      if [ $MY_ROLE = "repl_node" ]; then
        curmem="{_id:$i,host:\"$(getIp ${slist[i]}):$CONF_NET_PORT\",priority: 0, hidden: true}"
      else
        curmem="{_id:$i,host:\"$(getIp ${slist[i]}):$CONF_NET_PORT\",priority: 1}"
      fi
    else
      curmem="{_id:$i,host:\"$(getIp ${slist[i]}):$CONF_NET_PORT\",priority: 1}"
    fi
    
    if [ $i -eq 0 ]; then
      memberstr=$curmem
    else
      memberstr="$memberstr,$curmem"
    fi
  done

  local initjs=$(cat <<EOF
rs.initiate({
  _id:"$RS_NAME",
  members:[$memberstr]
})
EOF
  )

  runMongoCmd "$initjs" -P $CONF_NET_PORT
}

msInitUsers() {
  local jsstr=$(cat <<EOF
admin = db.getSiblingDB("admin")
admin.createUser(
  {
    user: "$DB_QC_USER",
    pwd: "$(cat $DB_QC_PASS_FILE)",
    roles: [ { role: "root", db: "admin" } ]
  }
)
EOF
  )
  runMongoCmd "$jsstr" -P $CONF_NET_PORT

  jsstr=$(cat <<EOF
admin = db.getSiblingDB("admin")
admin.createUser(
  {
    user: "$DB_ROOT_USER",
    pwd: "$DB_ROOT_PWD",
    roles: [ { role: "root", db: "admin" } ]
  }
)
EOF
  )
  runMongoCmd "$jsstr" -P $CONF_NET_PORT -u $DB_QC_USER -p $(cat $DB_QC_PASS_FILE)
}

checkNodeCanDoReplInit() {
  local slist=''
  slist=($(sortHostList ${NODE_CUR_LIST[@]}))
  test $(getSid ${slist[0]}) = $MY_SID
}

# msIsReplStatusOk
# check if replia set's status is ok
# 1 primary, other's secondary
msIsReplStatusOk() {
  local tmpstr=$(runMongoCmd "JSON.stringify(rs.status())" $@ | jq .members[].stateStr)
  local pcnt=$(echo "$tmpstr" | grep PRIMARY | wc -l)
  local scnt=$(echo "$tmpstr" | grep SECONDARY | wc -l)
  local allcnt=${#NODE_CUR_LIST[@]}
  test $pcnt -eq 1
  test $((pcnt+scnt)) -eq $allcnt
}

msIsMeMaster() {
  local tmpstr=$(runMongoCmd "JSON.stringify(rs.status().members)" $@)
  local pname=$(echo $tmpstr | jq '.[] | select(.stateStr=="PRIMARY") | .name' | sed s/\"//g)
  test "$pname" = "$MY_IP:$CONF_NET_PORT"
}

msIsMeHidden() {
  local tmpstr=$(runMongoCmd "JSON.stringify(rs.conf().members)" -P $CONF_NET_PORT -u $DB_QC_USER -p $(cat $DB_QC_PASS_FILE))
  local pname=$(echo $tmpstr | jq '.[] | select(.hidden==true) | .host' | sed s/\"//g)
  test "$pname" = "$MY_IP:$CONF_NET_PORT"
}

getNodesOrder() {
  local cnt=${#NODE_CUR_LIST[@]}
  local tmpstr=$(runMongoCmd "JSON.stringify(rs.status())" -P $CONF_NET_PORT -u $DB_QC_USER -p $(cat $DB_QC_PASS_FILE))
  local curstate=''
  local curip=''
  local reslist=''
  local masternode=''
  for((i=0; i<$cnt; i++)); do
    curip=$(getIp ${NODE_CUR_LIST[i]})
    curstate=$(echo $tmpstr | jq '.members[] | select(.name | test("'$curip'")) | .stateStr' | sed s/\"//g)
    if [ $curstate = "PRIMARY" ]; then
      masternode=$(getNodeId ${NODE_CUR_LIST[i]})
    else
      reslist="$reslist$(getNodeId ${NODE_CUR_LIST[i]}),"
    fi
  done
  if [ -z $masternode ]; then
    reslist=${reslist: 0:-1}
  else
    reslist="$reslist$masternode"
  fi
  log "action"
  echo $reslist
}

msGetReplCfgFromLocal() {
  local jsstr=$(cat <<EOF
mydb = db.getSiblingDB("local")
JSON.stringify(mydb.system.replset.findOne())
EOF
  )
  runMongoCmd "$jsstr" -P $CONF_MAINTAIN_NET_PORT
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
  runMongoCmd "$jsstr" -P $CONF_MAINTAIN_NET_PORT
}

MONGOD_BIN=/opt/mongodb/current/bin/mongod
shellStartMongodForAdmin() {
  # start mongod in admin mode
  runuser mongod -g svc -s "/bin/bash" -c "$MONGOD_BIN -f $MONGODB_CONF_PATH/mongod-admin.conf"
}

shellStopMongodForAdmin() {
  # stop mongod in admin mode
  runuser mongod -g svc -s "/bin/bash" -c "$MONGOD_BIN -f $MONGODB_CONF_PATH/mongod-admin.conf --shutdown"
}

changeNodeNetInfo() {
  # start mongod in admin mode
  shellStartMongodForAdmin

  local replcfg=''
  retry 60 3 0 msGetReplCfgFromLocal
  replcfg=$(msGetReplCfgFromLocal)

  local cnt=${#NODE_CUR_LIST[@]}
  local oldinfo=$(getItemFromFile NODE_CUR_LIST $HOSTS_INFO_FILE)
  local oldport=$(getItemFromFile PORT $HOSTS_INFO_FILE)
  local tmpstr=''
  local newlist=''
  for((i=0; i<$cnt; i++)); do
    # ip:port
    tmpstr=$(echo "$replcfg" | jq ".members[$i] | .host" | sed s/\"//g)
    # nodeid
    tmpstr=$(echo "$oldinfo" | sed 's/\/cln-/:'$oldport'\/cln-/g' | sed 's/ /\n/g' | sed -n /$tmpstr/p)
    tmpstr=$(getNodeId $tmpstr)
    # newip
    tmpstr=$(echo ${NODE_CUR_LIST[@]} | grep -o '[[:digit:].]\+/'$tmpstr | cut -d'/' -f1)
    # result
    newlist="$newlist\"$tmpstr:$CONF_NET_PORT\","
  done
  # js array: "ip:port","ip:port","ip:port"
  newlist=${newlist:0:-1}
  msUpdateReplCfgToLocal "$newlist"

  # stop mongod in admin mode
  shellStopMongodForAdmin
}

isAddNodesFromZero() {
  local cnt=''
  local str1=''
  local str2=''
  cnt=${#ADDING_LIST[@]}
  for((i=0; i<$cnt; i++)); do
    str1=$str1${ADDING_LIST[i]}'\n'
  done
  str1=$(echo -e "$str1" | sort)

  cnt=${#NODE_CUR_LIST[@]}
  for((i=0; i<$cnt; i++)); do
    str2=$str2${NODE_CUR_LIST[i]%/*}'\n'
  done
  str2=$(echo -e "$str2" | sort)
  
  test "$str1" = "$str2"
}

isMyRoleNeedChangeVxnet() {
  local tmp=$(echo "$CHANGE_VXNET_ROLES" | grep -o "$MY_ROLE")
  test "$tmp" = "$MY_ROLE"
}

TRANS_LIST=(VERTICAL_SCALING_FLAG CHANGE_VXNET_FLAG ADDING_HOSTS_FLAG DELETING_HOSTS_FLAG)
MANUAL_LIST=(CREATE NORMAL)
manualChangeCheck() {
  local ma=$(cat $NODECTL_MANUAL_FILE)
  test -z "$ma" && return 1
  :
}

isNodeCreate() {
  if ! manualChangeCheck; then return 1; fi
  local ma=$(cat $NODECTL_MANUAL_FILE)
  test "$ma" -eq "0" && return
  return 1
}

doWhenNodeCreate() {
  if ! isNodeCreate; then return; fi
  if [ $ADDING_HOSTS_FLAG = "true" ]; then
    : > $NODECTL_MANUAL_FILE
    echo 2 > $NODECTL_TRANS_FILE
    log "newly added node"
    if ! isAddNodesFromZero; then return; fi
  fi

  if ! checkNodeCanDoReplInit; then
    log "init replica: skip this node"
    if ! isAddNodesFromZero; then checkConfdChange; fi
    return 
  fi

  log "init replica begin ..."
  retry 60 3 0 msInitRepl
  retry 60 3 0 msIsReplStatusOk -P $CONF_NET_PORT
  retry 60 3 0 msIsMeMaster -P $CONF_NET_PORT
  log "add db users"
  retry 60 3 0 msInitUsers
  log "init replica done"

  if ! isAddNodesFromZero; then checkConfdChange; fi
}

doWhenNormalStart() {
  if manualChangeCheck; then return; fi
  local cnt=${#TRANS_LIST[@]}
  local tmpstr=''
  for((i=0; i<$cnt; i++)); do
    tmpstr=$(eval echo \$${TRANS_LIST[i]})
    test "$tmpstr" = "true" && return
  done
  echo 1 > $NODECTL_MANUAL_FILE
  checkConfdChange
}

doWhenVerticalScale() {
  test ! $VERTICAL_SCALING_FLAG = "true" && return
  log "vertical scaling begin ..."
  retry 60 3 0 msIsReplStatusOk -P $CONF_NET_PORT -u $DB_QC_USER -p $(cat $DB_QC_PASS_FILE)
  log "vertical scaling done"
}

doWhenChangeVxnet() {
  test ! $CHANGE_VXNET_FLAG = "true" && return
  if ! isMyRoleNeedChangeVxnet; then log "change net info: skip this node"; return; fi
  log "change net info begin ..."
  changeNodeNetInfo
  updateHostInfo
  log "change net info done"
}

start() {
  isNodeInitialized || initNode
  doWhenChangeVxnet
  _start
  doWhenVerticalScale
  doWhenNormalStart
  doWhenNodeCreate
}

stop() {
  local jsstr=$(cat <<EOF
if(rs.isMaster().ismaster) {
  rs.stepDown()
}
EOF
  )
  runMongoCmd "$jsstr" -P $CONF_NET_PORT -u $DB_QC_USER -p $(cat $DB_QC_PASS_FILE) || :
  _stop
}

msAddNodes() {
  local jsstr=''
  local cnt=${#ADDING_LIST[@]}
  for((i=0; i<$cnt; i++)); do
    jsstr=$jsstr'rs.add({host:"'$(getIp ${ADDING_LIST[i]})\:$CONF_NET_PORT'", priority: 1});'
  done
  runMongoCmd "$jsstr" -H $1 -P $CONF_NET_PORT -u $DB_QC_USER -p $(cat $DB_QC_PASS_FILE)
}

msRmNodes() {
  local cnt=${#DELETING_LIST[@]}
  local cmpstr=''
  for((i=0; i<$cnt; i++)); do
    cmpstr=$cmpstr$(getIp ${DELETING_LIST[i]})\:$CONF_NET_PORT" "
  done
  local jsstr=$(cat <<EOF
tmpstr="$cmpstr"
members=[]
cfg=rs.conf()
for(i=0;i<cfg.members.length;i++) {
  if (tmpstr.indexOf(cfg.members[i].host) != -1) {
    continue
  }
  members.push(cfg.members[i])
}
cfg.members=members
rs.reconfig(cfg)
EOF
  )
  runMongoCmd "$jsstr" -H $1 -P $CONF_NET_PORT -u $DB_QC_USER -p $(cat $DB_QC_PASS_FILE)
}

msGetAvailableNodeIp() {
  local cnt=${#NODE_CUR_LIST[@]}
  local res=''
  for((i=0; i<$cnt; i++)); do
    res=$(getIp ${NODE_CUR_LIST[i]})
    runMongoCmd "rs.status()" -H $res -P $CONF_NET_PORT -u $DB_QC_USER -p $(cat $DB_QC_PASS_FILE) >/dev/null 2>&1 && break
  done
  echo "$res"
}

msGetReplStatus() {
  local hostip=$(msGetAvailableNodeIp)
  local tmpstr=''

  if [ -z "$hostip" ]; then return; fi
  tmpstr=$(runMongoCmd "JSON.stringify(rs.status())" -H $hostip -P $CONF_NET_PORT -u $DB_QC_USER -p $(cat $DB_QC_PASS_FILE) 2>/dev/null)
  echo "$tmpstr"
}

msGetReplConf() {
  local hostip=$(msGetAvailableNodeIp)
  local tmpstr=''

  if [ -z "$hostip" ]; then return; fi
  tmpstr=$(runMongoCmd "JSON.stringify(rs.conf())" -H $hostip -P $CONF_NET_PORT -u $DB_QC_USER -p $(cat $DB_QC_PASS_FILE) 2>/dev/null)
  echo "$tmpstr"
}

msGetAvailableMasterIp() {
  local tmpstr=$(msGetReplStatus)
  tmpstr=$(echo $tmpstr | jq '.members[] | select(.stateStr=="PRIMARY") | .name' |  sed s/\"//g)
  echo ${tmpstr%:*}
}

isOtherRoleScaling() {
  local cnt=''
  if [ "$1" = "out" ]; then
    cnt=${#ADDING_LIST[@]}
  else
    cnt=${#DELETING_LIST[@]}
  fi
  test $cnt -eq 0
}

scaleOut() {
  if isOtherRoleScaling out; then
    log "other role scale out, skip this node"
    return
  fi

  log "node count check"
  log "node role check"

  local hostip=$(msGetAvailableMasterIp)
  if [ -z "$hostip" ]; then log "Can't get primary node's ip"; return ERR_GET_PRIMARY_IP; fi
  log "add nodes begin ..."
  msAddNodes $hostip
  log "add nodes done"
}

scaleIn() {
  if isOtherRoleScaling in; then
    log "other role scale in, skip this node"
    return
  fi

  log "node count check"
  log "node role check"
  
  local hostip=$(msGetAvailableMasterIp)
  if [ -z "$hostip" ]; then log "Can't get primary node's ip"; return ERR_GET_PRIMARY_IP; fi
  log "del nodes begin ..."
  msRmNodes $hostip
  log "del nodes done"
}

# delete
checkAndDoMongoRepl() {
  :
}

# delete
checkAndDoMongoShake() {
  local changed=false
  diff $MONGOSHAKE_HOSTS_FILE $MONGOSHAKE_HOSTS_FILE.new || changed=true
  test $changed = "false" && return
  createMongoShakeconf
  log "mongoshake's conf changed, find a proper node to restart the service"
}

restartMongoShake() {
  log "find a proper node to restart mongoshake"
}

# recordTransStatus
# desc: record the transaction's begin flag
# VERTICAL_SCALING_FLAG 0
# CHANGE_VXNET_FLAG 1
# ADDING_HOSTS_FLAG 2
# DELETING_HOSTS_FLAG 3
# no detected ""
recordTransStatus() {
  local tmpstr=""
  local curtrans=""
  local cnt=${#TRANS_LIST[@]}
  for((i=0; i<$cnt; i++)); do
    tmpstr=$(eval echo \$${TRANS_LIST[i]})
    if [ $tmpstr = "true" ]; then
      curtrans=$(cat $NODECTL_TRANS_FILE)
      test -n "$curtrans" && return
      echo $i > $NODECTL_TRANS_FILE
      return
    fi
  done
}

detectTransOver() {
  local curtrans=$(cat $NODECTL_TRANS_FILE)
  test -z "$curtrans" && return
  local state=$(eval echo \$${TRANS_LIST[$curtrans]})
  test $state = "true" && return
  : > $NODECTL_TRANS_FILE
  echo $curtrans
}

getManualEvent() {
  local ma=$(cat $NODECTL_MANUAL_FILE)
  local res=$(echo ${MANUAL_LIST[$ma]})
  echo $res
}

normalChangeCheck() {
  local ma=$(cat $NODECTL_MANUAL_FILE)
  test -n "$ma" && return 1
  local curtrans=$(cat $NODECTL_TRANS_FILE)
  test -n "$curtrans" && return 1
  local cnt=${#TRANS_LIST[@]}
  for((i=0; i<$cnt; i++)); do
    tmpstr=$(eval echo \$${TRANS_LIST[i]})
    test $tmpstr = "true" && return 1
  done
  :
}

isAddingNodes() {
  local list1=($(getItemFromFile NODE_LIST $HOSTS_INFO_FILE))
  local list2=($(getItemFromFile NODE_LIST $HOSTS_INFO_FILE.new))
  local cnt1=${#list1[@]}
  local cnt2=${#list2[@]}

  test "$cnt1" -ne "$cnt2" && return 0

  list1=($(getItemFromFile NODE_RO_LIST $HOSTS_INFO_FILE))
  list2=($(getItemFromFile NODE_RO_LIST $HOSTS_INFO_FILE.new))
  cnt1=${#list1[@]}
  cnt2=${#list2[@]}

  test "$cnt1" -ne "$cnt2" && return 0
  return 1
}

checkConfdChange() {
  if [ ! -d /data/appctl/logs ]; then
    log "cluster init"
    echo 0 > $NODECTL_MANUAL_FILE
    touch $NODECTL_TRANS_FILE
    updateHostInfo
    updateConfdMongodParam
    log "create mongod config file"
    createMongodConf
    return
  fi
  if normalChangeCheck; then
    if isAddingNodes; then log "detect adding nodes, skipping"; return; fi
    log "normal change"
  elif manualChangeCheck; then
    manual=$(getManualEvent)
    : > $NODECTL_MANUAL_FILE
    log "manual event: $manual"
  else
    recordTransStatus
    curtrans=$(detectTransOver)
    test -z "$curtrans" && return
    case ${TRANS_LIST[$curtrans]} in
      "VERTICAL_SCALING_FLAG") log "vertical scale ends";;
      "CHANGE_VXNET_FLAG") ;&
      "ADDING_HOSTS_FLAG") ;&
      "DELETING_HOSTS_FLAG") log "host info changed"; updateHostInfo; restartMongoShake;;
    esac
  fi
}

mytest() {
  :
}
#!/bin/bash
#set -x

# Installation location
maprHome="${MAPR_HOME:-__HPE_HOME__}"
drillBase="${maprHome}/drill"
hadoopVer=`cat ${maprHome}/hadoop/hadoopversion`
hadoopHome="${maprHome}/hadoop/hadoop-$hadoopVer"

# Installation parameters
drillLogPrefix="Drill:"
newZKConnectValue=""
drill_secure="false"
drill_qs_only=0
drill_only=0
drill_qs_and_drill=0

# Warden parameters
wardenTmpDrillConf="/tmp/warden.drill-bits.conf"
wardenInstalledDrillConf="warden.drill-bits.conf"
wardenConfFileInstalled=0
drillBitsNodeOn=0
DRILL_CONF_ASSUME_RUNNING_CORE=0

# Security parameters
httpUserName=""
httpUserPwd=""
httpPort="8047"
httpsLoginResult=""
httpsCookieFile="/tmp/installer_cookie.txt"
drillClientAuthMech="[\"MAPRSASL\", \"PLAIN\"]"
drillBitAuthMech="MAPRSASL"
drillAuthPackage="org.apache.drill.exec.rpc.user.security"
drillAuthImpl="pam4j"
drillPamProfiles="[\"sudo\", \"login\"]"
maprPamProfile="mapr-admin"
pamLocation="/etc/pam.d/"
useHadoopSSL=ssl.useHadoopConfig
hadoopSSLConfig=$hadoopHome/etc/hadoop
hadoopSSLServerConfig=ssl-server.xml
hadoopSSLCoreSite=core-site.xml
impersonationPrefix=impersonation
securityUserPrefix=security.user
securityUserAuthPrefix=$securityUserPrefix.auth
securityBitPrefix=security.bit
httpsPrefix=http.ssl_enabled
httpAuthPrefix=http.auth
httpAuthMechanisms=$httpAuthPrefix.mechanisms
oidcUseMaprConfig=$httpAuthPrefix.oidc.useMapRConfig

if [ -e "${maprHome}/server/common-ecosystem.sh" ]; then
    . "${maprHome}/server/common-ecosystem.sh"
else
   echo "Failed to source common-ecosystem.sh"
   exit 0
fi

function GetDrillVersion() {
  local ver=""
  if [ -f "${drillBase}/drillversion" ]; then
    ver=$(cat "${drillBase}/drillversion")
  else
    ver=`ls -t ${drillBase} | sed 's/^.*drill-//' | head -1 | awk '{print $1}'`
  fi
  echo "$ver"
}

#Func: Convert memory value to specific scale
#Args: <memValue> <outputUnit>
function getConvertedValue() {
  local rc
  local inputValue=$1
  local outputUnit=$2
  local output=$1
  #Extract value in bytes
  local memInBytes=$1
  if [[ $memInBytes == [0-9]*[mM] ]]; then
    let memInBytes=${memInBytes:0:${#memInBytes}-1}*1024*1024
  elif [[ $memInBytes == [0-9]*[gG] ]]; then
    let memInBytes=${memInBytes:0:${#memInBytes}-1}*1024*1024*1024
  elif [[ $memInBytes == [0-9]*[kK] ]]; then
    let memInBytes=${memInBytes:0:${#memInBytes}-1}*1024
  else
    let memInBytes=${memInBytes}
  fi
  #Convert to intended
  if [[ $outputUnit == [gG] ]]; then
    let output=$memInBytes/1024/1024/1024
  elif [[ $outputUnit == [mM] ]]; then
    let output=$memInBytes/1024/1024
  elif [[ $outputUnit == [kK] ]]; then
    let output=$memInBytes/1024
  else
    let output=$memInBytes
  fi
  rc=$?
  echo "$output"
  return $rc
}

#Func: Get provided memory value as percentage of system mem
#Args: <memValue>
function getMemAsSystemPercent() {
  local rc
  local memInMB=$(getConvertedValue $1 'm')
  local totalSysMem=`cat /proc/meminfo | grep MemTotal | awk '{print $2}'`
  if [[ `cat /proc/meminfo | grep MemTotal | grep -c kB` == "1" ]]; then
    let totalSysMem=$(getConvertedValue $totalSysMem'k' 'm')
  fi
  let percOfTotal=100*$memInMB/$totalSysMem
  rc=$?
  echo "$percOfTotal"
  return $rc
}

#Func: Extract memory value
#Args: <srcFile> <grepLineWith> <valueToExtract>
function getMemValueFromWardenFile() {
    local rc
    local wFile=$1
    local wIdent=$2
    local wVariable=$3
    local RES=$(cat $wFile | grep "^$wIdent" | sed -e 's/.*'"$wVariable="'\([0-9]*[GgMmKk%]*\).*$/\1/')
    rc=$?
    #Reset if value is absent (testing for wIdent)
    if [[ $RES = *"${wIdent}"* ]]; then RES="" ; fi
    # Translating %age to value in terms of system memory
    if [[ "$RES" = *% ]]; then
      local totalSysMem=`cat /proc/meminfo | grep MemTotal | awk '{print $2}'`
      if [[ `cat /proc/meminfo | grep MemTotal | grep -c kB` == "1" ]]; then
        let totalSysMem=$(getConvertedValue $totalSysMem 'm')
      fi
      let RES=${RES:0:${#RES}-1}*$totalSysMem/100
      #Capturing % value for writing updated values
      echo "${RES}G"
    else
      echo "$RES"
    fi
    return $rc
}

#Func: Round up to next GB and return in MB (detects and autoscales unit)
#Args: <inputValue>
function roundUpToNextGB() {
    if [ -z "$1" ]; then echo 0; return 0; fi
    local valueInMB=$(getConvertedValue $1 'm')
    let valueInGB=$valueInMB/1024
    let deltaInMB=$valueInMB%1024
    if [ $deltaInMB -gt 0 ]; then let valueInGB+=1 ; fi
    let valueInMB=$valueInGB*1024
    echo "$valueInMB"
    return
}

# Copy libjpam out of /opt/mapr/lib directory inside DRILL_HOME/jars/pam directory.
# Then use this path inside distrib-env.sh for java.library.path. We want to avoid
# directly pointing to /opt/mapr/lib since it contains lots of other native libraries
# and jars which can conflict with Drill's version of same libs/jars
#
# Only called in case when security is enabled
#
# Deprecated/Unused - By default libpam4j will be configured instead of libjpam implementations
# Hence we don't need to copy any native library for that.
function CopyAndUpdatePamLib() {
    logInfo "$drillLogPrefix Starting to copy and update pam module"

    # Copy the pam library from $maprHome/lib to $DRILL_HOME/conf/pam
    local libPamBase="$maprHome/lib"
    local libPamSrc="$libPamBase/libjpam.so"

    if [ ! -f "$libPamSrc" ]
    then
        logInfo "$drillLogPrefix ERROR: Failed to find libjpam.so under $libPamSrc path"
    return
    fi

    # If we are here that means file is present so copy it inside the DRILL_HOME/conf/path
    local drillBaseDir=${drillBase}/drill-${DRILL_VER}
    local drillConfDir="$drillBaseDir/jars"
    local libPamDest="$drillConfDir/pam"

    if [ ! -d "$drillConfDir" ]
    then
        logInfo "$drillLogPrefix ERROR: Failed to find Drill's conf path: $drillConfDir"
    return
    fi

    # Remove and create pam directory
    rm -rf $libPamDest
    mkdir $libPamDest

    if [ $? -ne 0 ]
    then
        logInfo "$drillLogPrefix ERROR: Failed to create pam directory: $libPamDest"
        return
    fi

    # Copy the libjpam.so
    cp -f $libPamSrc $libPamDest

    if [ $? -ne 0 ]
    then
        logInfo "$drillLogPrefix ERROR: Failed to copy the libjpam under $libPamDest"
    return
    fi
}

# Function to update the PAM library path in distrib-env.sh file based on security is enabled
# or disabled.
#
# In this function ~ is chosen as deliminiter for sed purposefully since pamPath has / in its value
# which causes invalid sed expression to form.
#
# Parameters:
#           $1 = true/false -- security is enabled/disabled
#           $2 = DRILL_JAVA_OPTS
#           $3 = path of libjpam.so
#
# Deprecated/Unused - By default libpam4j will be configured instead of libjpam implementations
# Hence we don't need to update the java.library.path
function UpdatePamPath() {
     logInfo "$drillLogPrefix Starting to update pam path with security $1 and path $3"

     local javaLibPath="-Djava.library.path="

     # Remove trailing spaces at the end of line
     #sed -i -e "s/\([^ ]*\)\([ ]*$\)/\1/g" $drillDistribEnv

     # Seucrity is disabled hence remove the java.library.path from DRILL_JAVA_OPTS
     if [ "$1" == "false" ]
     then
         sed -i -e "s~ $javaLibPath[^ \"]*~~g" $drillDistribEnv
         return
     fi

     # Check if this string exists in distrib-env.sh
     if grep -q "$2.*$javaLibPath" $drillDistribEnv
     then
         # Remove the java.library.path pointing to old pam module path
     sed -i -e "s~ $javaLibPath[^ \"]*~~g" $drillDistribEnv
     fi

     # Remove the last " in the DRILL_JAVA_OPTS
     sed -i -e "s~\(^export $2=.*\)\"$~\1~g" $drillDistribEnv

     # Add java.library.path in DRILL_JAVA_OPTS with pam module path
     sed -i -e "s~\(^export $2=.*\)~\1 $javaLibPath$3\"~g" $drillDistribEnv

     logInfo "$drillLogPrefix Completed updating pam path"
}

# Add or Update zkSaslClientString as true/false for DRILL_JAVA_OPTS
# and SQLLINE_JAVA_OPTS variable inside distrib-env.sh file
#
# Parameters:
#           $1 = DRILL_JAVA_OPTS / SQLLINE_JAVA_OPTS
#           $2 = true/false -- value to change from
#           $3 = true/false -- value to change to
#
function UpdateZookeeperAuth() {
    logInfo "$drillLogPrefix Updating ZookeeperAuth for $1 from $2 to $3"

    # Update the distrib-env.sh file to enable zookeeper security.
    local zkSaslClientString="zookeeper.sasl.client"

    # Check if this string exists in the distrib-env.sh
    if grep -q "$1.*$zkSaslClientString" $drillDistribEnv
    then
        # Update the value to true if it's set to $3 from $2 for all occurences
        sed -i -e "s/\($1.*$zkSaslClientString=\)\($2\)\(.*\)/\1$3\3/g" $drillDistribEnv
    else
        # The zkSaslClientString is absent we need to add one to the end
        # Remove the last " from $1
        sed -i -e "s/\(^export $1=.*\)\"$/\1/g" $drillDistribEnv

        # Now add the zkSaslClientString with " in the end
        sed -i -e "s/\(^export $1=.*\)/\1 -D$zkSaslClientString=$3\"/g" $drillDistribEnv
    fi
}


# Add or remove -Dzookeeper.saslprovider=com.mapr.security.maprsasl.MaprSaslProvider
# for DRILL_JAVA_OPTS and SQLLINE_JAVA_OPTS variable inside distrib-env.sh file
#
# Parameters:
#           $1 = DRILL_JAVA_OPTS / SQLLINE_JAVA_OPTS
#           $2 = add / remove -- to add or to remove zkSaslProviderString
#
function UpdateZookeeperAuthProvider() {
    local zkSaslProviderString="zookeeper.saslprovider"
    local zkSaslProvider="com.mapr.security.maprsasl.MaprSaslProvider"

    if [[ $2 == "add" ]];
    then
        if grep -q "$1.*$zkSaslProviderString" $drillDistribEnv
        then
            # Update zkSaslProviderString with zkSaslProvider
            sed -i -e "s/\($1.*$zkSaslProviderString=\)\([a-zA-Z\.\$\_0-9]*\)\(.*\)/\1$zkSaslProvider\3/g" $drillDistribEnv
        else
            # The zkSaslProviderString is absent we need to add one to the end
            # Remove the last " from $1
            sed -i -e "s/\(^export $1=.*\)\"$/\1/g" $drillDistribEnv

            # Now add the zkSaslClientString with " in the end
            sed -i -e "s/\(^export $1=.*\)/\1 -D$zkSaslProviderString=$zkSaslProvider\"/g" $drillDistribEnv
        fi
    elif [[ $2 == "remove" ]];
    then
        # Remove zkSaslProviderString from $1
        sed -i -e "s/\($1.*\)\(-D$zkSaslProviderString=[a-zA-Z\.\$\_0-9]*\)\(.*\)/\1\3/g" $drillDistribEnv
    fi
}

# Add impersonation config inside the drill-distrib.conf file for Drill
#
# It expects drill-distrib.conf file to be in correct state such that just adding the
# impersonation config string will be done inside drill.exec block by default
#
function AddImpersonationConfig() {
    echo -e "\n  $impersonationPrefix.enabled: true," >> $drillDistribConf
    echo -e "  $impersonationPrefix.max_chained_user_hops: 3," >> $drillDistribConf
    echo -e "  options.exec.$impersonationPrefix.inbound_policies: \"[{proxy_principals:{users:[\\\"$MAPR_USER\\\"]},target_principals:{users:[\\\"*\\\"]}}]\"," >> $drillDistribConf
}

# Verify whether cluster SSO configuration is set.
# Returns:
#      0 - SSO is enabled
#      1 - SSO is disabled
function isClusterSSOEnabled() {
  export MAPR_TICKETFILE_LOCATION="${MAPR_HOME}/conf/mapruserticket"
  jwt_conf=$(maprcli cluster getssoconf -json)
  issuer=$(echo $jwt_conf | grep -o '"issuerendpoint":"[^"]*' | grep -o '[^"]*$')

  if [ -z ${issuer} ]; then
    return 1
  fi
  return 0
}

# Add or Update security configuration inside distrib.conf file for Drill
#
# Enable Authentication/Encryption on Drill
# Enable HTTPS for web channel
#
# Parameter:
#    $1: true/false -- value of enabled: field in security parameters.
#
function EnableSecurityInDistribConf() {
    logInfo "$drillLogPrefix Starting to update security config in $drillDistribConf with security $1"

    # Remove the last closing brace from the config file
    sed -i -e "/}$/d" $drillDistribConf

    # Add config to enable ZK ACL's
    echo -e "\n  zk.apply_secure_acl: $1," >> $drillDistribConf

    # Add client related security config
    AddImpersonationConfig

    echo -e "\n  security.auth.mechanisms: $drillClientAuthMech," >> $drillDistribConf
    echo -e "  $securityUserAuthPrefix.enabled: $1," >> $drillDistribConf
    echo -e "  $securityUserAuthPrefix.packages += \"$drillAuthPackage\"," >> $drillDistribConf
    echo -e "  $securityUserAuthPrefix.impl: \"$drillAuthImpl\"," >> $drillDistribConf

    local maprAdminPamLocation=$pamLocation$maprPamProfile

    # Check if we have mapr-admin pam profile installed inside pam directory and configure
    # pam profile accordingly
    if [ -e $maprAdminPamLocation ]; then
        drillPamProfiles=`echo $drillPamProfiles | sed "s/\]$/, \"$maprPamProfile\"\]"/g`
    fi

    echo -e "  $securityUserAuthPrefix.pam_profiles: $drillPamProfiles," >> $drillDistribConf
    echo -e "  $securityUserPrefix.encryption.sasl.enabled: $1," >> $drillDistribConf

    if isClusterSSOEnabled; then
      echo -e "  $httpAuthMechanisms: [\"FORM\", \"OPENID\"]," >> $drillDistribConf
      echo -e "  $oidcUseMaprConfig: $1," >> $drillDistribConf
    else
      echo -e "  $httpAuthMechanisms: [\"FORM\"]," >> $drillDistribConf
    fi
    # Remove the sasl encryption related parameter if present for user to bit path
    #sed -i -e "/$securityUserPrefix.encryption.sasl.*/d" $drillDistribConf

    # Enable SSL configuration
    #echo -e "  $securityUserPrefix.encryption.ssl.enabled: $1," >> $drillDistribConf

    # Add server related security config
    echo -e "\n  $securityBitPrefix.auth.enabled: $1," >> $drillDistribConf
    echo -e "  $securityBitPrefix.auth.mechanism: \"$drillBitAuthMech\"," >> $drillDistribConf
    echo -e "  $securityBitPrefix.encryption.sasl.enabled: $1," >> $drillDistribConf

    # Add config to enable ssl
    echo -e "\n  $httpsPrefix: $1," >> $drillDistribConf
    echo -e "  $useHadoopSSL: $1" >> $drillDistribConf

    # Add closing braces in the end
    echo -e "\n}" >> $drillDistribConf
}

# Delete all the security related configuration added while enabling the security
# for drillbit
# The reason this function is removing last brace, then all the configs and then blank
# lines from end is because it's very though to write something which can remove all the
# blank lines between 2 braces and that too inline. Also I can't just remove all the blank
# lines since there are blank lines in license section of file
function RemoveDrillSecurityConfig() {
    logInfo "$drillLogPrefix Removing all security configs from $drillDistribConf"

    # Remove the last brance
    sed -i -e "/}$/d" $drillDistribConf

    # Remove ZK ACL's related configs
    sed -i -e "/zk.apply_secure_acl.*/d" $drillDistribConf

    # Remove impersonation related configs
    sed -i -e "/$impersonationPrefix.*/d" $drillDistribConf

    # Remove drill client related security configs
    sed -i -e "/security.auth.*/d" $drillDistribConf
    sed -i -e "/$securityUserPrefix.*/d" $drillDistribConf
    sed -i -e "/$httpAuthPrefix.*/d" $drillDistribConf

    # Remove drill bit related security configs
    sed -i -e "/$securityBitPrefix.*/d" $drillDistribConf

    # Remove HTTPS ssl configuration
    sed -i -e "/$httpsPrefix.*/d" $drillDistribConf
    sed -i -e "/$useHadoopSSL.*/d" $drillDistribConf

    # Remove all the blank lines from end
    sed -i -e :a -e '/^\n*$/{$d;N;ba' -e '}' $drillDistribConf

    # Add the last brance
    echo -e "}" >> $drillDistribConf
}

# Enable security for drill
function EnableSecurity() {

    logInfo "$drillLogPrefix Starting to configure with security on"

    # Remove trailing spaces at the end of lines in distrib-env.sh
    sed -i -e "s/\([^ ]*\)\([ ]*$\)/\1/g" $drillDistribEnv

    # Update DRILL_JAVA_OPTS inside distrib-env.sh file
    UpdateZookeeperAuth DRILL_JAVA_OPTS false true

    # Update SQQLINE_JAVA_OPTS inside distrib-env.sh file
    # Authentication to Zookeeper from DrillClient's
    UpdateZookeeperAuth SQLLINE_JAVA_OPTS false true
    UpdateZookeeperAuthProvider SQLLINE_JAVA_OPTS add

    # Copy and update the libjpam.so path inside distrib-env.sh file
    # Commenting below logic since now we will be using pam4j instead of jpam
    # implementation by default. This doesn't require to copy and native libraries
    # But keeping the code here for future purpose if needed to fallback to jpam
    #CopyAndUpdatePamLib

    # Remove drill security config from distrib.conf
    RemoveDrillSecurityConfig

    # Update distrib.conf file with security enabled
    EnableSecurityInDistribConf true

    # Add soft link to ssl-server.xml file
    rm -f ${DRILL_CONF_DIR}/$hadoopSSLServerConfig
    logInfo "$drillLogPrefix Link creation source: $hadoopSSLConfig/$hadoopSSLServerConfig and dest: ${DRILL_CONF_DIR}/$hadoopSSLServerConfig"
    ln -s $hadoopSSLConfig/$hadoopSSLServerConfig ${DRILL_CONF_DIR}/$hadoopSSLServerConfig

    # Add soft link to core-site.xml file
    rm -f ${DRILL_CONF_DIR}/$hadoopSSLCoreSite
    logInfo "$drillLogPrefix Link creation source: $hadoopSSLConfig/$hadoopSSLCoreSite and dest: ${DRILL_CONF_DIR}/$hadoopSSLCoreSite"
    ln -s $hadoopSSLConfig/$hadoopSSLCoreSite ${DRILL_CONF_DIR}/$hadoopSSLCoreSite

    logInfo "$drillLogPrefix Completed configuring with security on"
}

# Disable security for drill
function DisableSecurity() {
    logInfo "$drillLogPrefix Starting to configure with security off"

    # Remove trailing spaces at the end of lines in distrib-env.sh
    sed -i -e "s/\([^ ]*\)\([ ]*$\)/\1/g" $drillDistribEnv

    # Update DRILL_JAVA_OPTS inside distrib-env.sh file
    UpdateZookeeperAuth DRILL_JAVA_OPTS true false

    # Update SQQLINE_JAVA_OPTS inside distrib-env.sh file
    # Authentication to Zookeeper from DrillClient's
    UpdateZookeeperAuth SQLLINE_JAVA_OPTS true false
    UpdateZookeeperAuthProvider SQLLINE_JAVA_OPTS remove

    # Update the Pam lib path inside distrib-env.sh file
    #UpdatePamPath false DRILL_JAVA_OPTS

    # Remove drill security config from distrib.conf
    RemoveDrillSecurityConfig

    # Remove soft link to ssl-server.xml file
    logInfo "$drillLogPrefix Removing SSL config link: ${DRILL_CONF_DIR}/$hadoopSSLServerConfig"
    rm -f ${DRILL_CONF_DIR}/$hadoopSSLServerConfig

    # Remove soft link to core-site.xml file
    logInfo "$drillLogPrefix Removing core-site.xml link: ${DRILL_CONF_DIR}/$hadoopSSLCoreSite"
    rm -f ${DRILL_CONF_DIR}/$hadoopSSLCoreSite

    # If configuration is done for Drill as a QS only or for both QS and Drill
    # We will add impersonation setting in insecure case too
    if [ "$drill_qs_only" -eq 1 -o "$drill_qs_and_drill" -eq 1 ]; then
        logInfo "$drillLogPrefix Configured as QS so adding Impersonation Config"

        # Remove the last braces in config file
        sed -i -e "/}$/d" $drillDistribConf

        # Add Impersonation settings
        AddImpersonationConfig

        # Add last braces in config file
        echo -e "\n}" >> $drillDistribConf
    fi
}

#Configure the Drillbit config file
function ConfigureDrillBitsRole() {

  local clusterIdOption="cluster-id"
  local oldClusterIdValue="drillbits1"
  local clusterName=$(getClusterName)
  local newClusterIdValue="${clusterName}-drillbits"
  local clusterIdStanza="\n  ${clusterIdOption}: \"${newClusterIdValue}\""

  local zkConnectOption="zk.connect"
  local oldZKConnectValue="localhost:2181"
  local newZKConnectValue=$(getZKServers)
  local zkConnectStanza="\n  ${zkConnectOption}: \"${newZKConnectValue}\""

  local rpcUserClientThreadsOption="rpc.user.client.threads"
  local rpcUserClientThreadsValue=4
  local rpcUserClientThreadsStanza="\n  ${rpcUserClientThreadsOption}: ${rpcUserClientThreadsValue}"

  local storeParquetBlockSizeOption="options.store.parquet.block-size"
  local storeParquetBlockSizeValue=268435456
  local storeParquetBlockSizeStanza="\n  ${storeParquetBlockSizeOption}: ${storeParquetBlockSizeValue}"

  local sysStoreProviderZKBlobRootOption="sys.store.provider.zk.blobroot"
  local sysStoreProviderZKBlobRootValue="maprfs:///apps/drill"
  local sysStoreProviderZKBlobRootStanza="\n  ${sysStoreProviderZKBlobRootOption}: \"${sysStoreProviderZKBlobRootValue}\""

  local spillDirectoryOption="spill.directories"
  local spillDirectoryValue="/tmp/drill/spill"
  local spillDirectoryStanza="\n  ${spillDirectoryOption}: [ \"${spillDirectoryValue}\" ]"

  local spillFSOption="spill.fs"
  local spillFSValue="maprfs:///"
  local spillFSStanza="\n  ${spillFSOption}: \"${spillFSValue}\""

  local totalMem=""
  local forceDefaultMemoryValues=0

  if ! hasRole 'drill-bits' ; then
    logInfo "Skipping Drill Bits Role configuration... Not found"
    return
  else
    drillBitsNodeOn=1
  fi
  logInfo "Configuring Drill Bits Role"
  local DRILL_HOME_DIR=${drillBase}/drill-${DRILL_VER}

  # DRILL_CONF_DIR used somewhere else so cannot be local
  DRILL_CONF_DIR="${DRILL_HOME_DIR}/conf"
  local drillConf="${DRILL_CONF_DIR}/drill-override.conf"
  local drillDistribQSMemConf="${DRILL_CONF_DIR}/drill-distrib-mem-qs.conf"
  local wardenPkgDrillConf="${DRILL_CONF_DIR}/warden.drill-bits.conf.template"
  # These are used elsewhere so they cannot be locals
  drillDistribEnv="${DRILL_CONF_DIR}/distrib-env.sh"
  drillDistribConf="${DRILL_CONF_DIR}/drill-distrib.conf"

  if hasRole 'drill-qs' ; then
    if [ $drill_only -eq 1 -o $drill_qs_and_drill -eq 1 ]; then
       rm -f "${maprHome}/roles/drill-qs"
       drill_qs_only=0
       # Force default due to role switch
       forceDefaultMemoryValues=1
    else
       # -QS option was already used - keep using it
       drill_qs_only=1
    fi
  else
    if [ $drill_qs_only -eq 1 ]; then
      # -QS option was not set => force QS defaults due to role switch
      forceDefaultMemoryValues=1
    fi
  fi

  if grep -q "cluster-id: \"$oldClusterIdValue\"" $drillConf; then
    # we have a new install with default information - remove it
    sed -i -e "/${clusterIdOption}: \"$oldClusterIdValue\"/d" $drillConf
    sed -i -e "/${zkConnectOption}: \"$oldZKConnectValue\"/d" $drillConf
  fi

  # always configure the number of threads required for QS
  if [ -f "$drillDistribQSMemConf" ]; then
    rpcUserClientThreadsValue=$(grep DrillRpcUserThreads "$drillDistribQSMemConf" | cut -d'=' -f2)
    if [ -z "$rpcUserClientThreadsValue" ]; then
      rpcUserClientThreadsValue="4"
    fi
  fi

  if [ "$drill_qs_only" -eq 1 ]; then
    touch "${maprHome}/roles/drill-qs"
    if [ -f "$drillDistribQSMemConf" ]; then
      heapMem=$(grep DRILL_HEAP "$drillDistribQSMemConf" | cut -d'=' -f2)
      directMem=$(grep DRILL_MAX_DIRECT_MEMORY "$drillDistribQSMemConf" | cut -d'=' -f2)
      codeCacheMem=$(grep DRILLBIT_CODE_CACHE_SIZE "$drillDistribQSMemConf" | cut -d'=' -f2)
      let totalMem=$heapMem+$directMem+$codeCacheMem
    fi
  fi

  if [ ! -e "$drillDistribConf" ]; then
    echo -e "drill.exec {${clusterIdStanza},${zkConnectStanza},${rpcUserClientThreadsStanza},${storeParquetBlockSizeStanza},${sysStoreProviderZKBlobRootStanza},${spillDirectoryStanza},${spillFSStanza}\n}" > $drillDistribConf
  else
    sed -i -e "s%\(${clusterIdOption}: \).*%\1\"${newClusterIdValue}\",%" "$drillDistribConf"
    sed -i -e "s%\(${zkConnectOption}: \).*%\1\"${newZKConnectValue}\",%" "$drillDistribConf"
    sed -i -e "s%\(${rpcUserClientThreadsOption}: \).*%\1\"${rpcUserClientThreadsValue}\",%" "$drillDistribConf"
    sed -i -e "s%\(${storeParquetBlockSizeOption}: \).*%\1\"${storeParquetBlockSizeValue}\",%" "$drillDistribConf"
    sed -i -e "s%\(${sysStoreProviderZKBlobRootOption}: \).*%\1\"${sysStoreProviderZKBlobRootValue}\",%" "$drillDistribConf"
    sed -i -e "s%\(${spillDirectoryOption}: \).*%\1[ \"${spillDirectoryValue}\" ],%" "$drillDistribConf"
    sed -i -e "s%\(${spillFSOption}: \).*%\1\"${spillFSValue}\",%" "$drillDistribConf"
  fi

  # get a warden file to work on
  if [ -e "${MAPR_CONF_CONFD_DIR}/$wardenInstalledDrillConf" ]; then
    cp "${MAPR_CONF_CONFD_DIR}/$wardenInstalledDrillConf" "$wardenTmpDrillConf"
    wardenConfFileInstalled=1
  else
    cp "$wardenPkgDrillConf" "$wardenTmpDrillConf"
  fi
  wardenConfFile="$wardenTmpDrillConf"

  ## Reading Configured values
  # Step 1: Defaults in Drill Package
  local def_drill_proc_mem_max=$(getMemValueFromWardenFile $wardenPkgDrillConf service.env DRILLBIT_MAX_PROC_MEM)
  #No default Heap, Direct or CodeCache defined, so nothing to scan
  local def_drill_heap_min=$(getMemValueFromWardenFile $wardenPkgDrillConf service.heapsize.min service.heapsize.min)
  local def_drill_heap_max=$(getMemValueFromWardenFile $wardenPkgDrillConf service.heapsize.max service.heapsize.max)
  # Step 2: Defaults in QuerySvc Package
  local def_qs_heap=$(getMemValueFromWardenFile $drillDistribQSMemConf 'export DRILL_HEAP' DRILL_HEAP)
  local def_qs_mdm=$(getMemValueFromWardenFile $drillDistribQSMemConf 'export DRILL_MAX_DIRECT_MEMORY' DRILL_MAX_DIRECT_MEMORY)
  local def_qs_dccs=$(getMemValueFromWardenFile $drillDistribQSMemConf 'export DRILLBIT_CODE_CACHE_SIZE' DRILLBIT_CODE_CACHE_SIZE)
  # total Process Mem
  let def_qs_proc_mem_max=$def_qs_heap+$def_qs_mdm+$def_qs_dccs
  def_qs_proc_mem_max=$(roundUpToNextGB $def_qs_proc_mem_max'm')
  # redefining drillQSHeapMinMax (in MB) based on def_qs_proc_mem_max
  local def_qs_heap_min=$def_qs_proc_mem_max
  local def_qs_heap_max=$def_qs_proc_mem_max
  # Step 3: Existing in wardenConf
  local conf_drill_proc_mem_max=$(getMemValueFromWardenFile $wardenConfFile service.env DRILLBIT_MAX_PROC_MEM)
  local conf_drill_heap=$(getMemValueFromWardenFile $wardenConfFile service.env DRILL_HEAP)
  local conf_drill_mdm=$(getMemValueFromWardenFile $wardenConfFile service.env DRILL_MAX_DIRECT_MEMORY)
  local conf_drill_dccs=$(getMemValueFromWardenFile $wardenConfFile service.env DRILLBIT_CODE_CACHE_SIZE)
  local conf_drill_heap_min=$(getMemValueFromWardenFile $wardenConfFile service.heapsize.min service.heapsize.min)
  local conf_drill_heap_max=$(getMemValueFromWardenFile $wardenConfFile service.heapsize.max service.heapsize.max)
  #Capture %age value if exists (not for qsOnly modes)
  local conf_drill_proc_mem_override=""
  if [ "$drill_qs_only" -eq 0 ]; then
    if [ "$forceDefaultMemoryValues" -eq 1 ]; then
      conf_drill_proc_mem_override=$(cat $wardenPkgDrillConf | grep "^service.env" | sed -e 's/.*'"DRILLBIT_MAX_PROC_MEM="'\([0-9]*[%]*\).*$/\1/')
    else
      conf_drill_proc_mem_override=$(cat $wardenConfFile | grep "^service.env" | sed -e 's/.*'"DRILLBIT_MAX_PROC_MEM="'\([0-9]*[%]*\).*$/\1/')
    fi
    if [[ "$conf_drill_proc_mem_override" = *% ]]; then
      conf_drill_proc_mem_override=${conf_drill_proc_mem_override:0:${#conf_drill_proc_mem_override}-1}
    else
      conf_drill_proc_mem_override=""
    fi
  fi
  # declare variable
  local new_drill_proc_mem_max=""
  local new_drill_heap=""
  local new_drill_mdm=""
  local new_drill_dccs=""
  local new_drill_heap_min=""
  local new_drill_heap_max=""
  local new_drill_heap_percent=""

  # configure memory requirement
  if [ "$drill_qs_only" -eq 0 ]; then
      # Drill-Only Mode
      if [ "$forceDefaultMemoryValues" -eq 1 ]; then
        new_drill_proc_mem_max=$def_drill_proc_mem_max
      else
        new_drill_proc_mem_max=$conf_drill_proc_mem_max
        #Capture user-defined if any
        if [ -n "$conf_drill_heap" ]; then new_drill_heap=$conf_drill_heap; fi
        if [ -n "$conf_drill_mdm" ]; then new_drill_mdm=$conf_drill_mdm; fi
        if [ -n "$conf_drill_dccs" ]; then new_drill_dccs=$conf_drill_dccs; fi
      fi
  else
      # Query-Service Mode
      new_drill_heap="${def_qs_heap}m"
      new_drill_mdm="${def_qs_mdm}m"
      new_drill_dccs="${def_qs_dccs}m"
      new_drill_proc_mem_max=$(roundUpToNextGB $def_qs_proc_mem_max'm')'m'
      if [ "$forceDefaultMemoryValues" -eq 0 ]; then
        #Expecting all config to be in MB (also check if config values are missing)
        if [ -n "$conf_drill_heap" ]; then new_drill_heap=$conf_drill_heap;
        else new_drill_heap="${def_qs_heap}m"; fi
        if [ -n "$conf_drill_mdm" ]; then new_drill_mdm=$conf_drill_mdm;
        else new_drill_mdm="${def_qs_mdm}m"; fi
        if [ -n "$conf_drill_dccs" ]; then new_drill_dccs=$conf_drill_dccs;
        else new_drill_dccs="${def_qs_dccs}m"; fi
      fi
      # total Process Mem (in MB)
      let new_drill_proc_mem_max=$(getConvertedValue $new_drill_heap 'm')+$(getConvertedValue $new_drill_mdm 'm')+$(getConvertedValue $new_drill_dccs 'm')
      new_drill_proc_mem_max=$(roundUpToNextGB $new_drill_proc_mem_max'm')'m'
  fi
  #Redefining drillHeapMax (in MB) based on new_drill_proc_mem_max
  new_drill_heap_min=$(getConvertedValue $new_drill_proc_mem_max 'm')
  new_drill_heap_max=$new_drill_heap_min
  new_drill_heap_percent=$(getMemAsSystemPercent $new_drill_heap_max'm')

  #Updating and undo existing file contents if switching between QS and Drill
  if [ "$drill_qs_only" -eq 0 ]; then
    #Retain Heap, Direct and CodeCache params if user specified
    new_drill_service_env="";
    if [ -n "$new_drill_heap" ]; then new_drill_service_env="${new_drill_service_env},DRILL_HEAP=$new_drill_heap"; fi
    if [ -n "$new_drill_mdm" ]; then new_drill_service_env="${new_drill_service_env},DRILL_MAX_DIRECT_MEMORY=$new_drill_mdm"; fi
    if [ -n "$new_drill_dccs" ]; then new_drill_service_env="${new_drill_service_env},DRILLBIT_CODE_CACHE_SIZE=$new_drill_dccs"; fi
    #Prepending total (Note: comma already exists)
    if [ -z "$conf_drill_proc_mem_override" ]; then
      new_drill_service_env="DRILLBIT_MAX_PROC_MEM=${new_drill_proc_mem_max}${new_drill_service_env}"
    else
      new_drill_service_env="DRILLBIT_MAX_PROC_MEM=${conf_drill_proc_mem_override}%${new_drill_service_env}"
    fi
    # Marking settings as Drillbit
    sed -i -e 's|\#Default [a-zA-Z ]* Mem Distrib.*|\#Default Drill Mem Distrib: 20% (Min Recommended: Heap=4G,MaxDirect=8G,CodeCache=1G)|g' $wardenTmpDrillConf
  else
    #Place holder for listing missing params in QS
    new_drill_service_env="";
    new_drill_service_env="${new_drill_service_env},DRILL_HEAP=$new_drill_heap"
    new_drill_service_env="${new_drill_service_env},DRILL_MAX_DIRECT_MEMORY=$new_drill_mdm"
    new_drill_service_env="${new_drill_service_env},DRILLBIT_CODE_CACHE_SIZE=$new_drill_dccs"
    #Prepending total (Note: comma already exists)
    new_drill_service_env="DRILLBIT_MAX_PROC_MEM=${new_drill_proc_mem_max}${new_drill_service_env}"
    # Marking settings as QueryService
    sed -i -e 's|\#Default [a-zA-Z ]* Mem Distrib.*|\#Default QS Mem Distrib: Heap=3G,MaxDirect=1G,CodeCache=512M|g' $wardenTmpDrillConf
  fi

  # Substituting all entries in service.env
  sed -i -e "s/\(service.env=\).*/\1$new_drill_service_env/" $wardenTmpDrillConf
  # Force Update (warden-specific) heapMin, heapMax and heapPercent for Regular Drill Service (these are auto-managed)
  sed -i -e "s/\(service.heapsize.min=\)[0-9]*[GgMmKkBb]*/\1$new_drill_heap_min/" $wardenTmpDrillConf
  sed -i -e "s/\(service.heapsize.max=\)[0-9]*[GgMmKkBb]*/\1$new_drill_heap_max/" $wardenTmpDrillConf
  if [ -z "$conf_drill_proc_mem_override" ]; then
    sed -i -e "s/\(service.heapsize.percent=\)[0-9]*[%]*/\1$new_drill_heap_percent/" $wardenTmpDrillConf
  else
    sed -i -e "s/\(service.heapsize.percent=\)[0-9]*[%]*/\1$conf_drill_proc_mem_override/" $wardenTmpDrillConf
  fi

}

function CopyDrillbitWardenFile() {
  if [ "$drillBitsNodeOn" -eq 1 ]; then
    if [ "$wardenConfFileInstalled" -eq 0 ]; then
      if ! [ -d "${MAPR_CONF_CONFD_DIR}" ]; then
        mkdir -p "${MAPR_CONF_CONFD_DIR}"
      fi
    fi
    if ! grep -q "drill-$DRILL_VER" "$wardenTmpDrillConf" ; then
      sed -i -e "s@/drill-[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/@/drill-$DRILL_VER/@" "$wardenTmpDrillConf"
    fi
    cp "$wardenTmpDrillConf" "${MAPR_CONF_CONFD_DIR}/$wardenInstalledDrillConf"
    chown "$MAPR_USER":"$MAPR_GROUP" "${MAPR_CONF_CONFD_DIR}/$wardenInstalledDrillConf"
    rm -f "$wardenTmpDrillConf"
  fi
}

#
# Update symlinks, in case MapR is upgraded but Drill is not
#
function UpdateSymlinks() {
    find ${drillHome}/jars -type l -exec rm -f {} \;

    # Enable extended globbing to use more accurate patterns
    shopt -s extglob

    declare -A dependencies
    dependencies["maprfs-[0-9]*-mapr?(-SNAPSHOT).jar"]="${drillHome}/jars/3rdparty"
    dependencies["maprdb-[0-9]*-mapr?(-SNAPSHOT).jar"]="${drillHome}/jars/3rdparty"
    dependencies["maprdb-mapreduce-[0-9]*.jar"]="${drillHome}/jars/3rdparty"
    dependencies["mapr-hbase-[0-9]*.jar"]="${drillHome}/jars/3rdparty"
    dependencies["ojai-[0-9]*.jar"]="${drillHome}/jars/3rdparty"
    dependencies["ojai-mapreduce-[0-9]*.jar"]="${drillHome}/jars/3rdparty"
    dependencies["mapr-security-web-[0-9]*.jar"]="${drillHome}/jars/3rdparty"
    dependencies["jmxagent-[0-9]*.jar"]="${drillHome}/jars/3rdparty"
    dependencies["bc-fips-[0-9]*.jar"]="${drillHome}/jars/3rdparty"
    dependencies["bctls-fips-[0-9]*.jar"]="${drillHome}/jars/3rdparty"
    dependencies["zookeeper-[0-9]*.jar"]="${drillHome}/jars/ext"
    dependencies["zookeeper-jute-[0-9]*.jar"]="${drillHome}/jars/ext"

    #Replace jars from package with symlinks on jars from env
    for dep in "${!dependencies[@]}"
    do
      if ls ${maprHome}/lib/${dep} 1> /dev/null 2>&1; then
         rm -f ${dependencies[$dep]}/${dep}
         ln -s ${maprHome}/lib/${dep} ${dependencies[$dep]}/
      fi
    done

    shopt -u extglob
}

#
# Get MAPR_USER and MAPR_GROUP if not set
#
find_mapr_user_and_group() {
  DAEMON_CONF="${MAPR_HOME}/conf/daemon.conf"

  MAPR_USER=${MAPR_USER:-$( [ -f "$DAEMON_CONF" ] && awk -F = '$1 == "mapr.daemon.user" { print $2 }' "$DAEMON_CONF" )}
  MAPR_USER=${MAPR_USER:-"mapr"}
  export MAPR_USER

  MAPR_GROUP=${MAPR_GROUP:-$( [ -f "$DAEMON_CONF" ] && awk -F = '$1 == "mapr.daemon.group" { print $2 }' "$DAEMON_CONF" )}
  MAPR_GROUP=${MAPR_GROUP:-"$MAPR_USER"}
  export MAPR_GROUP
}

# Main

#sets MAPR_USER/MAPR_GROUP/logfile
initCfgEnv
find_mapr_user_and_group

#get opts
drill_usage="usage: $0 [-EC <common opts>] [-R] [--secure|--unsecure|--customSecure ] [-qsOnly] [-qsDrill] [-drillOnly] [-username <user>] [-password <passwd>] [-httpPort <port>]"
if [ ${#} -ge 1 ] ; then
    OPTS=`getopt -a -o h -l EC: -l R -l secure -l unsecure -l customSecure -l qsOnly -l qsDrill -l drillOnly -l username: -l password: -l httpPort: -- "$@"`
    if [ $? != 0 ] ; then
        echo ${drill_usage}
        exit 2
    fi
    eval set -- "$OPTS"

    for i ; do
        case "$i" in
            --EC)
                #Parse Common options
                #Ingore ones we don't care about
                ecOpts=($2)
                shift 2
                restOpts="$@"
                eval set -- "${ecOpts[@]} --"
                for j in "$@" ; do
                    case "$j" in
                        --OT|-OT)
                            OTNodesList="$2"
                            shift 2;;
                        --R|-R)
                            DRILL_CONF_ASSUME_RUNNING_CORE=1
                            shift 1
                            ;;
                        --QS|-QS)
                            drill_qs_only=1
                            shift 1
                            ;;
                        --) shift
                            break;;
                        *)
                            #echo "Ignoring common option $j"
                            shift 1;;
                    esac
                done
                shift 2
                eval set -- "$restOpts"
                ;;
            --R)
                DRILL_CONF_ASSUME_RUNNING_CORE=1
                shift 1
                ;;
            --customSecure)
                drill_secure="custom";
                shift 1;;
            -h)
                echo ${drill_usage}
                exit 2
                ;;
            --drillOnly)
                drill_only=1;
                shift 1;;
            --qsDrill)
                drill_qs_and_drill=1;
                shift 1;;
            --qsOnly)
                drill_qs_only=1;
                shift 1;;
            --secure)
                drill_secure="true";
                shift 1;;
            --unsecure)
                drill_secure="false";
                shift 1;;
            --username)
                shift 1
                httpUserName=$1
                shift 1;;
            --password)
                shift 1
                httpUserPwd=$1
                shift 1;;
            --httpPort)
                shift 1
                httpPort=$1
                shift 1;;
            --)
                shift
                break;;
        esac
    done
fi

DRILL_VER=$(GetDrillVersion)
drillHome=${drillBase}/drill-${DRILL_VER}
notConfiguredYet="${drillHome}/conf/.not_configured_yet"

# change the access permissions
chmod -R a+r ${drillHome}/conf
chmod a+x ${drillHome}/conf/*.sh

# add binaries to /usr/bin
ln -sf ${drillHome}/bin/sqlline /usr/bin/sqlline
ln -sf ${drillHome}/bin/drill-config.sh /usr/bin/drill-config.sh

# copy libjpam for jmxauth
CopyAndUpdatePamLib

UpdateSymlinks
newZKConnectValue=$(getZKServers)
if [ $? -ne 0 -o -z "$newZKConnectValue" ]; then
    logInfo "WARNING: Skipping drillbit role configuration - ZooKeeper connection not set in drill-distrib.conf"
    exit
fi

echo "OTNodesList: $OTNodesList"
clusterName=$(getClusterName)
ConfigureDrillBitsRole

if [ "$drill_secure" == "true" ]
then
    EnableSecurity
elif [ "$drill_secure" == "false" ]
then
    DisableSecurity
else
    # Called with --customSecure flag. Do nothing, preserve the configuration as is
    # except in the case where we are added newly to a node in customSecure mode.
    # Create initial configuration as if it's running in MapR secure mode and then
    # customer will need to edit to match their own configuration and restart drill
    if [ -f "$notConfiguredYet" ]; then
        EnableSecurity
    fi
fi

# Restart Drillbit by copying the Drillbit warden file
CopyDrillbitWardenFile

# change ownership
find "${drillHome}" \
  -path "${drillHome}/logs/*" -type d -prune \
  -or \
  -exec chown -h "${MAPR_USER}":"${MAPR_GROUP}" \
    "${drillBase}/current" \
    "${drillBase}/drillversion" \
    {} + \
    >> $logFile 2>&1

# remove state flag indicating fresh install
if [ -f "$notConfiguredYet" ]; then
    rm -f "$notConfiguredYet"
fi

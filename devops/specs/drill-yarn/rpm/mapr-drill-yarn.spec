%undefine __check_files
%define _binaries_in_noarch_packages_terminate_build 0

summary:     HPE DataFabric Ecosystem Pack: Apache Drill on Yarn
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise
name:        mapr-drill-yarn
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       HPE
buildarch:   noarch
requires:    mapr-client
conflicts:   mapr-drill, mapr-drill-internal
AutoReqProv: no


%description
Apache Drill on Yarn package included in HPE DataFabric Software Ecosystem Pack
Tag: __RELEASE_BRANCH__
Commit: __GIT_COMMIT__


%clean
echo "NOOP"


%files
__PREFIX__/

%pre
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "pre install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

OLD_DRILL_DIRS=`find __PREFIX__/drill -type d -name "drill-*" -maxdepth 1 2> /dev/null`
if [ ! -z "${OLD_DRILL_DIRS}" ]; then
  mkdir -p __PREFIX__/drill/OLD_DRILL_VERSIONS
  for OLD_DRILL_DIR in ${OLD_DRILL_DIRS} ; do
    if [ -d "${OLD_DRILL_DIR}/conf" ]; then
      OLD_DRILL_DIRNAME=`basename ${OLD_DRILL_DIR}`
      mkdir -p __PREFIX__/drill/OLD_DRILL_VERSIONS/${OLD_DRILL_DIRNAME}
      cp -rf ${OLD_DRILL_DIR}/conf __PREFIX__/drill/OLD_DRILL_VERSIONS/${OLD_DRILL_DIRNAME}/.
    fi
  done
fi

%post
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "post install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

VERSION_SHORT="$(echo __VERSION__ | cut -d'.' -f1-3)"
echo "$VERSION_SHORT" > __PREFIX__/drill/drillversion

#
# change ownership
#
DAEMON_CONF="__PREFIX__/conf/daemon.conf"

if [ -f "$DAEMON_CONF" ]; then
    MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' $DAEMON_CONF)

    if [ ! -z "$MAPR_USER" ]; then
        chown -R $MAPR_USER __PREFIX__/drill/drill-${VERSION_SHORT}
    fi
fi

chmod 1777 -R __PREFIX__/drill/drill-${VERSION_SHORT}/logs
touch __PREFIX__/drill/drill-${VERSION_SHORT}/logs/sqlline.log
chmod 666 __PREFIX__/drill/drill-${VERSION_SHORT}/logs/sqlline.log

# distrib-env.sh is replaced at build time - no longer a need to generate its contents at install time

if [ ! -f /opt/mapr/conf/mapr.login.conf ]; then
    cp -f __PREFIX__/drill/drill-${VERSION_SHORT}/conf/mapr.login.conf /opt/mapr/conf/.
fi
if [ -f __INSTALL_3DIGIT__/conf/mapr.login.conf ]; then
  rm -f __INSTALL_3DIGIT__/conf/mapr.login.conf
fi

ln -sf __PREFIX__/drill/drill-${VERSION_SHORT}/bin/sqlline /usr/bin/sqlline

#
# get ZK list and cluster ID for drill-override.conf
#
drillOverrideConf="__PREFIX__/drill/drill-${VERSION_SHORT}/conf/drill-override.conf"
oldClusterId="drillbits1"
oldZkConnect="localhost:2181"

isZkAddressDiscovered=0
zkLine=$(grep zookeeper.servers /opt/mapr/conf/warden.conf)
if [ -n "$zkLine" ]; then
  zkServers=$(echo $zkLine | sed 's/zookeeper.servers=//' | tr ' ' ',')
  isZkAddressDiscovered=1
fi

if [ "${isZkAddressDiscovered}" = "1" ]; then
  sed -i -e "s/$oldZkConnect/$zkServers/g" $drillOverrideConf
else
  echo "Could not detect Zookeeper information"
  echo "Not found zookeeper.servers in file /opt/mapr/conf/warden.conf"
  echo "Once you configure this MapR node to connect to Zookeeper, then you need to edit ${drillOverrideConf}"
fi

if [ -f /opt/mapr/conf/mapr-clusters.conf ]; then
  clusters=`cat /opt/mapr/conf/mapr-clusters.conf`
  clusterName=`echo ${clusters} | awk '{print $1;}'`
  clusterName=`echo ${clusterName//./_}`
  clusterName=`echo ${clusterName// /_}`
  newClusterId="${clusterName}-drillbits"
else
  echo "Did not detect file /opt/mapr/conf/mapr-clusters.conf"
  echo "Drillbit cluster name will default to 'default-drillbit-cluster-name-drillbits'"
  newClusterId="default-drillbit-cluster-name-drillbits"
fi
sed -i -e "s/$oldClusterId/$newClusterId/g" $drillOverrideConf

%preun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "preun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :


%postun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "postun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

if [ $1 -eq 0 ]; then
  rm -rf __PREFIX__/drill/drill-${VERSION_SHORT}
  rm -f __PREFIX__/drill/drillversion

  rm -Rf /usr/bin/sqlline
  rm -Rf /usr/bin/drill-config.sh
fi

%posttrans
# $1 -eq 0 install
# $1 -eq 0 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "posttrans install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :


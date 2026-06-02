%undefine __check_files
%define _binaries_in_noarch_packages_terminate_build 0

summary:     HPE DataFabric Ecosystem Pack: Apache Drill Role
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise
name:        mapr-drill
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       HPE
buildarch:   noarch
requires:    mapr-core, mapr-drill-internal >= __RELEASE_VERSION__
conflicts:   mapr-drill-yarn
AutoReqProv: no

%description
Apache Drill distibution role package. Part of HPE DataFabric Software Ecosystem Pack
Tag: __RELEASE_BRANCH__
Commit: __GIT_COMMIT__
Commit ID: __GIT_COMMIT__

%clean
echo "NOOP"


%files
__HPE_HOME__

%pre
MY_DRILL_HOME="__DRILL_HOME__"
MAPR_HOME="__HPE_HOME__"
MY_DRILL_VERSION="__VERSION__"
MY_DRILL_BASE="$( dirname $MY_DRILL_HOME )"
MY_DRILL_OLD_VERSION_FILE="%{_localstatedir}/lib/rpm-state/drill-old-version"
MY_DRILL_STATE_DIR="$( dirname $MY_DRILL_OLD_VERSION_FILE )"
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "pre install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

if [ "$1" = "2" ]; then
  MY_DRILL_OLD_VERSION="$( ls -1 $MY_DRILL_BASE | head -1 | cut -d'-' -f2 )"
  if [ -n "$MY_DRILL_OLD_VERSION" ]; then
    if [ ! -d "$MY_DRILL_STATE_DIR" ]; then
      mkdir -p "$MY_DRILL_STATE_DIR"
    fi
    echo "$MY_DRILL_OLD_VERSION" >  $MY_DRILL_OLD_VERSION_FILE
  fi
  isSecure="false"
  if [ -f "${MAPR_HOME}/conf/mapr-clusters.conf" ]; then
      isSecure=$(head -1 ${MAPR_HOME}/conf/mapr-clusters.conf | grep -o 'secure=\w*' | cut -d= -f2)
  fi
  if [ "$isSecure" = "true" ] && [ -f "${MAPR_HOME}/conf/mapruserticket" ]; then
    export MAPR_TICKETFILE_LOCATION="${MAPR_HOME}/conf/mapruserticket"
  fi
  echo "Stopping drillbit with command 'maprcli node services -nodes `hostname -f` -name drill-bits -action stop'"
  RESULTS=$(maprcli node services -nodes `hostname -f` -name drill-bits -action stop)
  STATUS=$?
  if [ $STATUS -ne 0 ]; then
    echo $RESULTS
  fi
  sleep 10s
fi

%post

MY_DRILL_HOME="__DRILL_HOME__"
MAPR_HOME="__HPE_HOME__"
MY_DRILL_VERSION="__VERSION__"
MY_DRILL_BASE="$( dirname $MY_DRILL_HOME )"
MY_DRILL_OLD_VERSION_FILE="%{_localstatedir}/lib/rpm-state/drill-old-version"

if [ "$1" = "2" ]; then
  echo "POSTINST upgrade"
  if [ -f $MY_DRILL_OLD_VERSION_FILE ]; then
    echo "Saving old config files"
    MY_DRILL_OLD_VERSION="$( cat $MY_DRILL_OLD_VERSION_FILE )"
    MY_DRILL_OLD_HOME="$MY_DRILL_BASE/drill-$MY_DRILL_OLD_VERSION"
    if [ -e $MAPR_HOME/conf/conf.d/warden.drill-bits.conf ]; then
        cp $MAPR_HOME/conf/conf.d/warden.drill-bits.conf $MY_DRILL_HOME/conf/warden.drill-bits.conf-$MY_DRILL_OLD_VERSION
        # remove so configure.sh will install the new one
        rm -f $MAPR_HOME/conf/conf.d/warden.drill-bits.conf
    fi
    rm -f $MY_DRILL_OLD_VERSION_FILE
  fi
fi


%preun
MAPR_HOME="__HPE_HOME__"
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "preun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

if [ "$1" = "0" ]; then
  if $MAPR_HOME/initscripts/mapr-warden status > /dev/null 2>&1 ; then
    isSecure=$(head -1 ${MAPR_HOME}/conf/mapr-clusters.conf | grep -o 'secure=\w*' | cut -d= -f2)
    if [ "$isSecure" = "true" ] && [ -f "${MAPR_HOME}/conf/mapruserticket" ]; then
      export MAPR_TICKETFILE_LOCATION="${MAPR_HOME}/conf/mapruserticket"
    fi
    RESULTS=$(maprcli node services -nodes `hostname -f` -name drill-bits -action stop)
    STATUS=$?
    if [ $STATUS -ne 0 ]; then
      echo "$RESULTS"
    fi
  fi
fi
if [ -f $MAPR_HOME/conf/conf.d/warden.drill-bits.conf ] ; then
    rm  $MAPR_HOME/conf/conf.d/warden.drill-bits.conf
fi

%postun
# If this is an uninstall, last version is removed
# if package is getting purged remove entire directory
if [ "$1" = "0" ]; then
    if [ -f __HPE_HOME__/conf/conf.d/warden.drill-bits.conf ]; then
        rm -Rf __HPE_HOME__/conf/conf.d/warden.drill-bits.conf
    fi

    if [ -f __HPE_HOME__/roles/drill-qs ]; then
        rm -Rf  __HPE_HOME__/roles/drill-qs
    fi
    rm -Rf /usr/bin/sqlline
    rm -Rf /usr/bin/drill-config.sh
fi


%posttrans

#
# To compensate for the 0.7 to 0.8 upgrade
#
maprCoreMajorVer=""
installWardenFile=0
if [ -e __HPE_HOME__/MapRBuildVersion ]; then
    maprCoreMajorVer=$(cat __HPE_HOME__/MapRBuildVersion | cut -d'.' -f1)
fi
if [ -f __DRILL_HOME__/conf/warden.drill-bits.conf ]; then
    if [ -z "$maprCoreMajorVer" ]; then
        installWardenFile=1
    elif [ "$maprCoreMajorVer" -lt 6 ]; then
        installWardenFile=1
    fi
    if [ "$installWardenFile" -eq 1 ]; then
        cp -fp __DRILL_HOME__/conf/warden.drill-bits.conf __HPE_HOME__/conf/conf.d/.
    fi
fi

%undefine __check_files
%define _binaries_in_noarch_packages_terminate_build 0

summary:     HPE DataFabric Ecosystem Pack: Apache Drill
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise
name:        mapr-drill-internal
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       HPE
buildarch:   noarch
requires:    mapr-client
AutoReqProv: no


%description
Apache Drill distribution included in HPE DataFabric Software Ecosystem Pack
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

# RPM case - new package is installed first, then old package is uninstalled after
if [ "$1" = "2" ]; then
  OLD_DRILL_VERSION=$(rpm -qi mapr-drill-internal | awk -F': ' '/Version/ {print $2}')
  OLD_DRILL_3DIGIT_VERSION=$(echo $OLD_DRILL_VERSION | cut -d "." -f1-3)
  OLD_DIR="__PREFIX__/drill/drill-${OLD_DRILL_3DIGIT_VERSION}"
  BACKUP_TARGET="__PREFIX__/drill/drill-${OLD_DRILL_VERSION}"

  mkdir -p "${BACKUP_TARGET}"
  chown --reference="${OLD_DIR}" "${BACKUP_TARGET}"

  if [ -d "${OLD_DIR}/conf" ]; then
    cp -rfa "${OLD_DIR}"/conf "${BACKUP_TARGET}"
  fi
  if [ -d "${OLD_DIR}/logs" ]; then
    mv "${OLD_DIR}"/logs "${BACKUP_TARGET}"
  fi
  if [ -d "${OLD_DIR}/jars/3rdparty" ]; then
    cp -rfaT "${OLD_DIR}"/jars/3rdparty "${BACKUP_TARGET}"/jars
  fi

  rm -rf "${OLD_DIR}"/jars/*
  create_dummy_rpm_files() {
    rpm_file_paths="$(rpm -ql mapr-drill-internal | grep "/jars/" | grep -e "jar$")"
    while read file_path
    do
      parent_dir="$(dirname "${file_path}")"
      if [ ! -d "${parent_dir}" ]
      then
        mkdir -p "${parent_dir}"
      fi
      touch "${file_path}"
    done <<< "${rpm_file_paths}"
  }
  create_dummy_rpm_files
fi
:

%post
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "post install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

VERSION_SHORT="$(echo __VERSION__ | cut -d'.' -f1-3)"
ln -sfn __PREFIX__/drill/drill-${VERSION_SHORT} __PREFIX__/drill/current

if [ -f __PREFIX__/drill/drillversion ]; then
  rm -f __PREFIX__/drill/drillversion
fi
echo "$VERSION_SHORT" > __PREFIX__/drill/drillversion

%preun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "preun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

if [ "$1" -eq 0 ]; then
  OLD_DRILL_VERSION=$(rpm -qi mapr-drill-internal | awk -F': ' '/Version/ {print $2}')
  OLD_DRILL_3DIGIT_VERSION=$(echo $OLD_DRILL_VERSION | cut -d "." -f1-3)
  OLD_DIR="__PREFIX__/drill/drill-${OLD_DRILL_3DIGIT_VERSION}"
  BACKUP_TARGET="__PREFIX__/drill/drill-${OLD_DRILL_VERSION}"

  mkdir -p "${BACKUP_TARGET}"
  chown --reference="${OLD_DIR}" "${BACKUP_TARGET}"

  if [ -d "${OLD_DIR}/conf" ]; then
    cp -rfa "${OLD_DIR}"/conf "${BACKUP_TARGET}"
  fi
  if [ -d "${OLD_DIR}/logs" ]; then
    cp -rfa "${OLD_DIR}"/logs "${BACKUP_TARGET}"
  fi
  if [ -d "${OLD_DIR}/jars/3rdparty" ]; then
    cp -rfaT "${OLD_DIR}"/jars/3rdparty "${BACKUP_TARGET}"/jars
  fi

  create_dummy_rpm_files() {
    rpm_file_paths="$(rpm -ql mapr-drill-internal | grep "/jars/" | grep -e "jar$")"
    while read file_path
    do
      parent_dir="$(dirname "${file_path}")"
      if [ ! -d "${parent_dir}" ]
      then
        mkdir -p "${parent_dir}"
      fi
      touch "${file_path}"
    done <<< "${rpm_file_paths}"
  }
  create_dummy_rpm_files
fi
:

%postun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "postun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

VERSION_SHORT="$(echo __VERSION__ | cut -d'.' -f1-3)"
if [ "$1" = "0" ]; then
  rm -Rf __PREFIX__/drill/drill-${VERSION_SHORT}
  rm -f __PREFIX__/drill/current
fi
# RPM format - post-uninstall happens after new package's install, so we know the drillversion file exists
if [ "$1" = "1" ]; then
  NEW_DRILL_VERSION=`cat __PREFIX__/drill/drillversion`
  if [ "${VERSION_SHORT}" != "${NEW_DRILL_VERSION}" ]; then
    rm -Rf __PREFIX__/drill/drill-${VERSION_SHORT}
  fi
fi
:

%posttrans
# $1 -eq 1 install
# $1 -eq 1 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "posttrans install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

if [ "$1" = "1" ]; then
  deprecated_drill_backup_dir="__PREFIX__/drill/OLD_DRILL_VERSIONS"
  if [ -d "$deprecated_drill_backup_dir" ]; then
    num_entries=$(ls -A "$deprecated_drill_backup_dir" | wc -l)

    if [ "$num_entries" -eq 1 ]; then
      rm -rf "$deprecated_drill_backup_dir"
    fi
  fi

  OLD_DRILL_DIRS=`find "__PREFIX__/drill/" -maxdepth 1 -regex '.*drill-[0-9]*\.[0-9]*\.[0-9]*$' ! -name "*$VERSION_SHORT"`
  rm -rf ${OLD_DRILL_DIRS}
fi
:


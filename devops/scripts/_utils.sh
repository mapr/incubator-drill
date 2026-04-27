#!/bin/bash

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
. "${SCRIPT_DIR}/_initialize_package_variables.sh"

OS="redhat"
if [ -e "/etc/debian_version" ]; then
  OS="debian"
elif [ -e "/etc/SuSE-release" ] || grep -q "ID_LIKE.*suse" "/etc/os-release" 2>/dev/null; then
  OS="suse"
fi

_exclude_hpe_jars() {
  rm -fv "${package_path}"/jars/3rdparty/maprfs*.jar
  rm -fv "${package_path}"/jars/3rdparty/maprdb*.jar
  rm -fv "${package_path}"/jars/3rdparty/mapr-hbase*.jar
  rm -fv "${package_path}"/jars/3rdparty/ojai*.jar
}

_setup_native_client() {
  local native_dir_path="${package_path}/native"

  mkdir -pv "${native_dir_path}"
  cp -fpv "${START_DIR}/contrib/native/client/build/libdrillClient.so" \
  	"${native_dir_path}/libdrillClient.so.${PKG_VERSION}"
  (
    cd "${native_dir_path}" && \
    ln -svf "libdrillClient.so.${PKG_VERSION}" "libdrillClient.so"
  )
}

overwrite_community_drill_resources() {
  cp -fv ${START_DIR}/ext-resources/bootstrap-storage-plugins.json \
  	     ${START_DIR}/exec/java-exec/src/main/resources/.
  cp -fv ${START_DIR}/ext-resources/drill-on-yarn-defaults.conf \
  	     ${START_DIR}/exec/java-exec/src/main/resources/drill-on-yarn-defaults.conf
  cp -fv ${START_DIR}/ext-resources/distrib-env.sh \
  	     ${START_DIR}/distribution/src/main/resources/distrib-env.sh
}

_overwrite_community_drill_conf_and_bin() {
  cp -fv "${START_DIR}/ext-conf/drill-on-yarn.conf" "${package_path}/conf/."
  cp -fv "${START_DIR}/ext-conf/drill-distrib.conf" "${package_path}/conf/."
  cp -fv "${START_DIR}/ext-conf/drill-distrib-mem-qs.conf" "${package_path}/conf/."
  cp -fv "${START_DIR}/ext-bin/configure.sh" "${package_path}/bin/."
}

setup_role() {
  local role_name="${1}"
  local pkg_home_dir="$2"
  local pkg_config_command="$3"
  local pkg_build_dir_name="${4:-${1}}"

  local role_package_path="${BUILD_ROOT}/root/${pkg_build_dir_name}${HPE_HOME_DIRECTORY}"

  mkdir -p "${role_package_path}/roles/"

  cat <<EOF > "${role_package_path}/roles/${role_name}"
PKG_HOME_DIR=${pkg_home_dir}
PKG_CONFIG_COMMAND=${pkg_config_command}
EOF
}

replace_build_variables() {
  REPLACE_DIR="${1}"
  find "$REPLACE_DIR" -type f \
    -exec sed -i \
      -e "s|__PREFIX__|${HPE_HOME_DIRECTORY}|g" \
      -e "s|__VERSION__|${PKG_VERSION}|g" \
      -e "s|__VERSION_3DIGIT__|${PKG_3DIGIT_VERSION}|g" \
      -e "s|__RELEASE_BRANCH__|${PACKAGE_INFO_BRANCH}|g" \
      -e "s|__RELEASE_VERSION__|${PKG_VERSION}.${TIMESTAMP}|g" \
      -e "s|__INSTALL_3DIGIT__|${PKG_INSTALL_ROOT}|g" \
      -e "s|__GIT_COMMIT__|${GIT_COMMIT}|g" \
    {} \;
}

setup_drill_internal_package() {
  local role_name="drill-internal"
  local package_path="${BUILD_ROOT}/root/${role_name}${DRILL_ROOT_DIR}"

  rm -rf "${package_path}"
  mkdir -pv "${package_path}"

  mv "${START_DIR}"/distribution/target/apache-drill-*/apache-drill-*/* "${package_path}"

  _setup_native_client

  chmod -R a+r "${package_path}"/conf
  chmod a+x "${package_path}"/conf/*.sh
  mkdir -pv "${package_path}"/logs
  chmod -R 1777 "${package_path}"/logs

  _overwrite_community_drill_conf_and_bin
  _exclude_hpe_jars
}

setup_drill_yarn_package() {
  local role_name="drill-yarn"
  local package_path="${BUILD_ROOT}/root/${role_name}${DRILL_ROOT_DIR}"

	rm -fv "${package_path}"/conf/warden*
	rm -fv "${package_path}"/conf/distrib-env.sh
	cp -fv "${START_DIR}"/ext-conf/distrib-env-yarn.sh "${package_path}"/conf/distrib-env.sh
	(
	  cd "${package_path}/.." || exit
		cp -rPp "drill-${PKG_3DIGIT_VERSION}" drill
		tar -czvf drill.tar.gz drill
		mv -v drill.tar.gz "${package_path}/."
		rm -rf drill
	)
}

build_package() {
  if [ "$OS" = "debian" ]; then
    _build_deb $@
  else
    _build_rpm $@
  fi
}

run_and_log() {
  echo "Executing: $*"
  "$@"
}

_build_rpm() {
  local role_name="${1}"

  local rpm_root="${BUILD_ROOT}/package/${role_name}/rpm"

  mkdir -p "${rpm_root}/SOURCES"
  mv -T "${BUILD_ROOT}/root/${role_name}" "${rpm_root}/SOURCES"

  mkdir -p "${rpm_root}/SPECS"
  cp "${START_DIR}/devops/specs/${role_name}"/rpm/*.spec "${rpm_root}/SPECS"

  rpmbuild --bb --define "_topdir ${rpm_root}" --buildroot="${rpm_root}/SOURCES" "${rpm_root}"/SPECS/*
  mkdir -p "$DIST_DIR"
  mv "${rpm_root}"/RPMS/*/*rpm "$DIST_DIR"
}

_build_deb() {
  role_name="${1}"

  deb_root="${BUILD_ROOT}/package/${role_name}/deb"

  mkdir -p "$deb_root"
  mv -T "${BUILD_ROOT}/root/${role_name}" "${deb_root}"

  mkdir -p "${deb_root}/DEBIAN"
  cp devops/specs/"${role_name}"/deb/* "${deb_root}/DEBIAN"

  find "$deb_root" -type f -exec md5sum \{\} \; 2>/dev/null |
    sed -e "s|${deb_root}||" -e "s| \/| |" |
    grep -v DEBIAN > "${deb_root}/DEBIAN/md5sums"
  echo "" >> "${deb_root}/DEBIAN/control"

  mkdir -p "$DIST_DIR"
  dpkg-deb --build "$deb_root" "$DIST_DIR"
}


#!/bin/bash

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
. "${SCRIPT_DIR}/_initialize_package_variables.sh"

verify_docker() {
  # Check if the docker command exists
  if ! command -v docker > /dev/null 2>&1; then
    echo "Error: 'docker' is not installed." >&2
    return 1
  fi

  # Optional but recommended: Check if the Docker daemon is actually running
  if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is installed, but the Docker daemon is not running. Please start it and try again." >&2
    return 1
  fi
}

_exclude_hpe_jars() {
  local pkg_path="$1"

  rm -fv "${pkg_path}"/jars/3rdparty/maprfs*.jar
  rm -fv "${pkg_path}"/jars/3rdparty/maprdb*.jar
  rm -fv "${pkg_path}"/jars/3rdparty/mapr-hbase*.jar
  rm -fv "${pkg_path}"/jars/3rdparty/ojai*.jar
}

_setup_native_client() {
  local pkg_path="$1"
  local native_dir_path="${pkg_path}/native"

  mkdir -pv "${native_dir_path}"
  cp -fpv "${START_DIR}/contrib/native/client/build/libdrillClient.so" \
  	"${native_dir_path}/libdrillClient.so.${PKG_VERSION}"
  (
    cd "${native_dir_path}" && \
    ln -svf "libdrillClient.so.${PKG_VERSION}" "libdrillClient.so"
  )
}

overwrite_community_drill_resources() {
  local resources_dir="$1"

  if [[ -z "$resources_dir" ]]; then
    echo "Error: You must provide a directory path as an argument."
    return 1
  fi

  cp -fv "${resources_dir}/ext-resources/bootstrap-storage-plugins.json" \
         "${START_DIR}/exec/java-exec/src/main/resources/."
  cp -fv "${resources_dir}/ext-resources/drill-on-yarn-defaults.conf" \
         "${START_DIR}/exec/java-exec/src/main/resources/drill-on-yarn-defaults.conf"
  cp -fv "${resources_dir}/ext-resources/distrib-env.sh" \
         "${START_DIR}/distribution/src/main/resources/distrib-env.sh"
}

_overwrite_community_drill_conf_and_bin() {
  local pkg_path="$1"

  echo "Copying conf and bin files to BUILD_ROOT..."
  cp -r "${START_DIR}/ext-conf" "${BUILD_ROOT}/"
  cp -r "${START_DIR}/ext-bin" "${BUILD_ROOT}/"

  echo "Replacing build placeholders in resources with actual values ..."
  # Target the copied versions in BUILD_ROOT instead of modifying the source
  replace_build_variables "${BUILD_ROOT}/ext-conf"
  replace_build_variables "${BUILD_ROOT}/ext-bin"


  cp -fv "${BUILD_ROOT}/ext-conf/drill-on-yarn.conf" "${pkg_path}/conf/."
  cp -fv "${BUILD_ROOT}/ext-conf/drill-distrib.conf" "${pkg_path}/conf/."
  cp -fv "${BUILD_ROOT}/ext-conf/drill-distrib-mem-qs.conf" "${pkg_path}/conf/."
  cp -fv "${BUILD_ROOT}/ext-bin/configure.sh" "${pkg_path}/bin/."
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
  local target_path="${1}"

  # Using + instead of \; groups files together and runs sed faster
  find "$target_path" -type f -exec sed -i \
      -e "s|__PREFIX__|${HPE_HOME_DIRECTORY}|g" \
      -e "s|__VERSION__|${PKG_VERSION}|g" \
      -e "s|__VERSION_3DIGIT__|${PKG_3DIGIT_VERSION}|g" \
      -e "s|__RELEASE_BRANCH__|${PACKAGE_INFO_BRANCH}|g" \
      -e "s|__RELEASE_VERSION__|${PKG_VERSION}.${TIMESTAMP}|g" \
      -e "s|__INSTALL_3DIGIT__|${PKG_INSTALL_ROOT}|g" \
      -e "s|__GIT_COMMIT__|${GIT_COMMIT}|g" \
    {} +
}

setup_drill_internal_package() {
  local role_name="drill-internal"
  local package_path="${BUILD_ROOT}/root/${role_name}${DRILL_ROOT_DIR}"

  rm -rf "${package_path}"
  mkdir -pv "${package_path}"

  mv "${START_DIR}"/distribution/target/apache-drill-*/apache-drill-*/* "${package_path}"

  _setup_native_client "${package_path}"

  chmod -R a+r "${package_path}"/conf
  chmod a+x "${package_path}"/conf/*.sh
  mkdir -pv "${package_path}"/logs
  chmod -R 1777 "${package_path}"/logs

  _overwrite_community_drill_conf_and_bin "${package_path}"
  _exclude_hpe_jars "${package_path}"
}

setup_drill_yarn_package() {
  local role_name="drill-yarn"
  local package_path="${BUILD_ROOT}/root/${role_name}${DRILL_ROOT_DIR}"

	rm -fv "${package_path}"/conf/warden*
	rm -fv "${package_path}"/conf/distrib-env.sh
	cp -fv "${START_DIR}"/ext-conf/distrib-env-yarn.sh "${package_path}"/conf/distrib-env.sh
	replace_build_variables "${package_path}"/conf/distrib-env.sh
	(
	  cd "${package_path}/.." || exit
		cp -rPp "drill-${PKG_3DIGIT_VERSION}" drill
		tar -czvf drill.tar.gz drill
		mv -v drill.tar.gz "${package_path}/."
		rm -rf drill
	)
}

build_package() {
  local package_name="$1"
  local os="$2"
  local keep_sources="$3"

  if [ "$os" = "debian" ]; then
    _build_deb "$package_name" "$keep_sources"
  else
    _build_rpm "$package_name" "$keep_sources"
  fi
}

run_and_log() {
  echo "Executing: $*"
  "$@"
}

_build_rpm() {
  # Check if rpmbuild is available in the system
  if ! command -v rpmbuild > /dev/null 2>&1; then
      echo "Error: 'rpmbuild' is not installed." >&2
      return 1
  fi

  local role_name="${1}"
  local keep_sources="${2:-false}"

  local rpm_root="${BUILD_ROOT}/package/${role_name}/rpm"

  mkdir -p "${rpm_root}/SOURCES"
  if [ "$keep_sources" = "true" ]; then
    cp -rTPp "${BUILD_ROOT}/root/${role_name}" "${rpm_root}/SOURCES"
  else
    mv -T "${BUILD_ROOT}/root/${role_name}" "${rpm_root}/SOURCES"
  fi

  mkdir -p "${rpm_root}/SPECS"
  cp "${START_DIR}/devops/specs/${role_name}"/rpm/*.spec "${rpm_root}/SPECS"
  replace_build_variables "${rpm_root}/SPECS"

  rpmbuild --bb --define "_topdir ${rpm_root}" --buildroot="${rpm_root}/SOURCES" "${rpm_root}"/SPECS/*
  mkdir -p "$DIST_DIR"
  mv "${rpm_root}"/RPMS/*/*rpm "$DIST_DIR"
}

_build_deb() {
  # Check if rpmbuild is available in the system
  if ! command -v dpkg-deb > /dev/null 2>&1; then
      echo "Error: 'dpkg-deb' is not installed." >&2
      return 1
  fi

  local role_name="${1}"
  local keep_sources="${2:-false}"
  local deb_root="${BUILD_ROOT}/package/${role_name}/deb"

  mkdir -p "$deb_root"
  if [ "$keep_sources" = "true" ]; then
    cp -rTPp "${BUILD_ROOT}/root/${role_name}" "${deb_root}"
  else
    mv -T "${BUILD_ROOT}/root/${role_name}" "${deb_root}"
  fi

  mkdir -p "${deb_root}/DEBIAN"
  cp devops/specs/"${role_name}"/deb/* "${deb_root}/DEBIAN"
  replace_build_variables "${deb_root}/DEBIAN"

  find "$deb_root" -type f -exec md5sum \{\} + 2>/dev/null |
    sed -e "s|${deb_root}||" -e "s| \/| |" |
    grep -v DEBIAN > "${deb_root}/DEBIAN/md5sums"
  echo "" >> "${deb_root}/DEBIAN/control"

  mkdir -p "$DIST_DIR"
  dpkg-deb --build "$deb_root" "$DIST_DIR"
}



#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "$0")
. "${SCRIPT_DIR}/_initialize_package_variables.sh"
. "${SCRIPT_DIR}/_utils.sh"

build_native_client() {
  pushd "${START_DIR}/contrib/native/client"
    sed -i -e "s|#    set(Boost_NAMESPACE drill_boost)|set(Boost_NAMESPACE drill_boost)|g" CMakeLists.txt

  	rm -rf build
  	mkdir -pv build

  	pushd build ; \
  	  export DRILL_BOOST_VERSION="1_64_0"
      export BOOST_INCLUDEDIR="/opt/drill-boost-build/drill_boost_${DRILL_BOOST_VERSION}"
      export BOOST_LIBRARYDIR="${BOOST_INCLUDEDIR}/stage/lib"
      export Boost_NO_SYSTEM_PATHS=ON

      cmake -G "Unix Makefiles" -D CMAKE_BUILD_TYPE=Release ..
      make

      mv -fv src/clientlib/libdrillClient.so .
  	popd
  popd
}

main() {
  local maven_target_phase=${1:-"install"}

  echo "Cleaning '${BUILD_ROOT}' dir..."
  rm -rf "$BUILD_ROOT"

  echo "Replacing build variables..."
  replace_build_variables "${START_DIR}/ext-bin"
  replace_build_variables "${START_DIR}/ext-conf"
  replace_build_variables "${START_DIR}/ext-resources"
  replace_build_variables "${START_DIR}/devops/specs"

  echo "Overwriting community Drill resources with HPE versions..."
  overwrite_community_drill_resources


  echo "Building project..."
  run_and_log mvn clean "${maven_target_phase}" \
    -B \
    -DskipTests \
    -Dskip.tar.assembly=true \
    -Drat.numUnapprovedLicenses=200 \
    -Dcheckstyle.skip=true \
    -Denforcer.skip=true \
    -U \
    -DdeployAtEnd=true \
    -DaltDeploymentRepository="${REPOSITORY_ID}::${MAPR_MAVEN_REPO}"

  echo "Building Drill native client..."
  build_native_client

  echo "Preparing drill-internal package..."
  setup_drill_internal_package
  setup_role "drill-internal" "${DRILL_ROOT_DIR}"

  echo "Preparing drill package..."
  setup_role "drill-bits" "${DRILL_ROOT_DIR}" "${DRILL_ROOT_DIR}/bin/configure.sh" "drill"

  echo "Preparing drill-yarn package..."
  mkdir -p "${BUILD_ROOT}/root/drill-yarn${DRILL_ROOT_DIR}"
  cp -rPp "${BUILD_ROOT}/root/drill-internal${DRILL_ROOT_DIR}"/* "${BUILD_ROOT}/root/drill-yarn${DRILL_ROOT_DIR}"
  setup_drill_yarn_package
  setup_role "drill-on-yarn" "${DRILL_ROOT_DIR}"

  echo "Building packages..."
  build_package "drill"
  build_package "drill-internal"
  build_package "drill-yarn"

  echo "Resulting packages:"
  find "$DIST_DIR" -exec readlink -f {} \;
}

main "${1}"

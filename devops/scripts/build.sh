#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "$0")
. "${SCRIPT_DIR}/_initialize_package_variables.sh"
. "${SCRIPT_DIR}/_utils.sh"

OS="redhat"
if [ -e "/etc/debian_version" ]; then
  OS="debian"
elif [ -e "/etc/SuSE-release" ] || grep -q "ID_LIKE.*suse" "/etc/os-release" 2>/dev/null; then
  OS="suse"
fi

replace_community_resources() {
  echo "Copying resources to BUILD_ROOT..."
  cp -r "${START_DIR}/ext-resources" "${BUILD_ROOT}/"

  echo "Replacing build placeholders in resources with actual values ..."
  # Target the copied versions in BUILD_ROOT instead of modifying the source
  replace_build_variables "${BUILD_ROOT}/ext-resources"

  echo "Overwriting community Drill resources with HPE versions..."
  overwrite_community_drill_resources "${BUILD_ROOT}"
}

build_project() {
  replace_community_resources

  if [[ "$1" == "deploy" ]]; then
    echo "Building project and deploying artifacts to Maven repository ${REPOSITORY_ID}::${MAPR_MAVEN_REPO}..."
    run_and_log mvn clean deploy \
      -B \
      -DskipTests \
      -Dskip.tar.assembly=true \
      -Drat.numUnapprovedLicenses=200 \
      -Dcheckstyle.skip=true \
      -Denforcer.skip=true \
      -U \
      -DdeployAtEnd=true \
      -DaltDeploymentRepository="${REPOSITORY_ID}::${MAPR_MAVEN_REPO}"
  else
    echo "Building project with Maven (install)..."
    run_and_log mvn clean install \
      -B \
      -DskipTests \
      -Dskip.tar.assembly=true \
      -Drat.numUnapprovedLicenses=200 \
      -Dcheckstyle.skip=true \
      -Denforcer.skip=true \
      -U
  fi
}

build_internal_package() {
  local os="$1"
  local keep_sources="$2"
  echo "Verifying presence of distribution files..."
  # Use ls to expand the wildcards and silently check if anything matches
  if ! ls "${START_DIR}"/distribution/target/apache-drill-*/apache-drill-*/* > /dev/null 2>&1; then
    echo "Error: Drill distribution files not found."
    echo "Expected files at: ${START_DIR}/distribution/target/apache-drill-*/apache-drill-*/*"
    exit 1
  fi

  echo "Preparing drill-internal package..."
  setup_drill_internal_package
  setup_role "drill-internal" "${DRILL_HOME_DIRECTORY}"

  echo "Building package..."
  build_package "drill-internal" "$os" "$keep_sources"
}

build_role_package() {
  local os="$1"
  echo "Preparing drill package..."
  setup_role "drill-bits" "${DRILL_HOME_DIRECTORY}" "${DRILL_HOME_DIRECTORY}/bin/configure.sh" "drill"

  echo "Building package..."
  build_package "drill" "$os"
}

build_drill_yarn_package() {
    local os="$1"
    echo "Preparing drill-yarn package..."
    mkdir -p "${BUILD_ROOT}/root/drill-yarn${DRILL_HOME_DIRECTORY}"
    cp -rPp "${BUILD_ROOT}/root/drill-internal${DRILL_HOME_DIRECTORY}"/* "${BUILD_ROOT}/root/drill-yarn${DRILL_HOME_DIRECTORY}"

    echo "Building package..."
    setup_role "drill-on-yarn" "${DRILL_HOME_DIRECTORY}"
    build_package "drill-yarn" "$os"
}

build_native_client() {
local mode="${1:-local}"
  local target_dir="${START_DIR}/contrib/native/client"

  if [ "$mode" = "docker" ]; then
    echo "Building Drill native client inside Docker..."

    verify_docker || return 1

    local docker_image="centos8-java17-mvn3.9.6:drill_zoo_protobuf"

    docker run --rm \
      --user "$(id -u):$(id -g)" \
      -v "${START_DIR}:${START_DIR}" \
      -w "${target_dir}" \
      "dfdkr.ftc.hcocto.hpecorp.net:80/${docker_image}" \
      bash -c '
        set -e
        sed -i -e "s|#    set(Boost_NAMESPACE drill_boost)|set(Boost_NAMESPACE drill_boost)|g" CMakeLists.txt
        rm -rf build && mkdir -pv build && cd build

        export DRILL_BOOST_VERSION="1_64_0"
        export BOOST_INCLUDEDIR="/opt/drill-boost-build/drill_boost_${DRILL_BOOST_VERSION}"
        export BOOST_LIBRARYDIR="${BOOST_INCLUDEDIR}/stage/lib"
        export Boost_NO_SYSTEM_PATHS=ON

        cmake -G "Unix Makefiles" -D CMAKE_BUILD_TYPE=Release ..
        make
        mv -fv src/clientlib/libdrillClient.so .
      '
  else
    echo "Building Drill native client locally..."

    pushd "${target_dir}" > /dev/null
      sed -i -e "s|#    set(Boost_NAMESPACE drill_boost)|set(Boost_NAMESPACE drill_boost)|g" CMakeLists.txt
      rm -rf build && mkdir -pv build

      pushd build > /dev/null
        export DRILL_BOOST_VERSION="1_64_0"
        export BOOST_INCLUDEDIR="/opt/drill-boost-build/drill_boost_${DRILL_BOOST_VERSION}"
        export BOOST_LIBRARYDIR="${BOOST_INCLUDEDIR}/stage/lib"
        export Boost_NO_SYSTEM_PATHS=ON

        cmake -G "Unix Makefiles" -D CMAKE_BUILD_TYPE=Release ..
        make
        mv -fv src/clientlib/libdrillClient.so .
      popd > /dev/null
    popd > /dev/null
  fi

  echo "Build complete! libdrillClient.so is available in ${target_dir}/build"
}

clean_resources() {
  echo "Cleaning up modified and untracked resources..."

  # Define the files we want to reset
  local files_to_clean=(
    "${START_DIR}/exec/java-exec/src/main/resources/bootstrap-storage-plugins.json"
    "${START_DIR}/exec/java-exec/src/main/resources/drill-on-yarn-defaults.conf"
    "${START_DIR}/distribution/src/main/resources/distrib-env.sh"
  )

  for file in "${files_to_clean[@]}"; do
    rm -f "$file"
    git checkout -- "$file" > /dev/null 2>&1 || true
  done
}

print_results() {
  echo "Resulting packages:"
  find "$DIST_DIR" -exec readlink -f {} \;
}

main() {

  # Set default values
  local action=""
  local os="${OS}"       # Defaults to global OS variable if not provided
  local deploy_flag=""
  local execution_mode="local"

  # 2. Parse all arguments iteratively
  for arg in "$@"; do
    case "$arg" in
      all|build|package|build_package|native_client|clean_resources)
        action="$arg"
        ;;
      deploy)
        deploy_flag="deploy"
        ;;
      --docker)
        execution_mode="docker"
        ;;
      *)
        # If it is not a known action and not "deploy", assume it is the OS
        os="$arg"
        ;;
    esac
  done

  echo "Cleaning '${BUILD_ROOT}' dir..."
  rm -rf "$BUILD_ROOT"
  mkdir "${BUILD_ROOT}"

  case "$action" in
    "native_client")
      # Build native client
      build_native_client "$execution_mode" || exit 1
      ;;

    "build")
      # Build project (Maven)
      build_project
      clean_resources
      ;;

    "build_package")
      # Build and package internal and drill packages
      build_project "$deploy_flag"
      build_native_client "$execution_mode" || exit 1
      build_internal_package "$os"
      build_role_package "$os"
      print_results
      clean_resources
      ;;

    "package")
      # Create internal and drill packages
      build_internal_package "$os" || exit 1
      build_role_package "$os"
      print_results
      clean_resources
      ;;

    "all")
      build_project "$deploy_flag"
      build_native_client "$execution_mode" || exit 1
      build_internal_package "$os" "true"
      build_role_package "$os"
      build_drill_yarn_package "$os"
      print_results
      clean_resources
      ;;

    "clean_resources")
      clean_resources
      ;;

    *)
      echo "Unknown action: $action"
      echo "Usage: $0 [action]{all|build|package|build_package|native_client|clean_resources} [os] [deploy] [--docker]"
      exit 1
      ;;
  esac
}

main "$@"

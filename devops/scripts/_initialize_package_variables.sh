BRANCH_NAME=${PACKAGE_INFO_BRANCH:-$(git branch --show-current)}
GIT_COMMIT=$(git log -1 --pretty=format:"%H")
HPE_HOME_DIRECTORY=${HPE_HOME_DIRECTORY:-"/opt/mapr"}
PKG_NAME=${PKG_NAME:-"drill"}
PKG_VERSION=${PKG_VERSION:-"$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout | cut -d "-" -f1)"}
PKG_3DIGIT_VERSION=${PKG_3DIGIT_VERSION:-"$(echo "${PKG_VERSION}" | cut -d "." -f1-3)"}
TIMESTAMP=${TIMESTAMP:-$(date "+%Y%m%d%H%M")}
DRILL_HOME_DIRECTORY="${DRILL_HOME_DIRECTORY:-"${HPE_HOME_DIRECTORY}/${PKG_NAME}/${PKG_NAME}-${PKG_3DIGIT_VERSION}"}"
START_DIR=$(pwd)
# rpmbuild does not work properly when relative path specified here
BUILD_ROOT=${BUILD_ROOT:-"${START_DIR}/devops/buildroot"}
DIST_DIR=${DIST_DIR:-"dist"}

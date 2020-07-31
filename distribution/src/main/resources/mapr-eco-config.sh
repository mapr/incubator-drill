#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This file is sourced by mapr-config.sh to provide get_drill_jars()
# Assumes MAPR_HOME is set, and get_files_in_folder() is defined
# (This code was extracted from the Core mapr-config.sh in order to
#  separate non-core knowledge, and placing it with the Eco component)

export DRILL_VERSION=`cat ${MAPR_HOME}/drill/drillversion`
export DRILL_HOME="${MAPR_HOME}/drill/drill-${DRILL_VERSION}"

# Add Apache Drill jars
get_drill_jars() {
DRILL_CORE_JARS=$(get_files_in_folder ${DRILL_HOME}/jars\
    "drill-auth-mechanism-maprsasl-*.jar"\
    "drill-common-*.jar"\
    "drill-java-exec-*.jar"\
    "drill-logical-*.jar"\
    "drill-memory-base-*.jar"\
    "drill-protocol-*.jar"\
    "drill-rpc-*.jar"\
    "vector-*.jar")
DRILL_3P_JARS=$(get_files_in_folder ${DRILL_HOME}/jars/3rdparty\
    "antlr4-runtime-4*.jar"\
    "curator-*.jar"\
    "commons-lang3*.jar"\
    "config-1*.jar"\
    "guava-*.jar"\
    "hppc-*.jar"\
    "javassist-*.jar"\
    "metrics-*-4*.jar"\
    "netty-*-4*.jar"\
    "xmlenc-*.jar")
DRILL_CLASSB_JARS=$(get_files_in_folder ${DRILL_HOME}/jars/classb\
    "reflections-*.jar")

echo $DRILL_CORE_JARS:$DRILL_3P_JARS:$DRILL_CLASSB_JARS
}


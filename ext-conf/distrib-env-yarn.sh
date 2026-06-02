# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file is empty by default. Default Drill environment settings appear
# in drill-config.sh. Distributions can replace this file with a
# distribution-specific version that sets environment variables and options
# specific to that distribution. Users should not put anything in this file;
# put user options in drill-env.sh instead.

# MapR-specific environment settings for Drill

export HADOOP_VERSION=`cat __HPE_HOME__/hadoop/hadoopversion`
export HADOOP_HOME=${HADOOP_HOME:-"__HPE_HOME__/hadoop/hadoop-${HADOOP_VERSION}"}

export DRILL_JAVA_OPTS="${DRILL_JAVA_OPTS} -Djava.security.auth.login.config=/opt/mapr/conf/mapr.login.conf -Dzookeeper.sasl.client=false"
export DRILL_LOG_DIR=${DRILL_LOG_DIR:-"__DRILL_HOME__/logs"}
export DRILL_PID_DIR=${DRILL_PID_DIR:-"__HPE_HOME__/pid"}
export MAPR_IMPERSONATION_ENABLED=${MAPR_IMPERSONATION_ENABLED:-"true"}

# Only set MAPR_TICKETFILE_LOCATION when invoked in context of drillbit setup NOT sqlline. It is expected
# to generate a separate ticket when sqlline is used.
if [ "$DRILLBIT_CONTEXT" = "1" ]; then
   export MAPR_TICKETFILE_LOCATION=${MAPR_TICKETFILE_LOCATION:-"__HPE_HOME__/conf/mapruserticket"}
fi

export LD_LIBRARY_PATH=""

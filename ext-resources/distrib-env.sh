# This file is empty by default. Default Drill environment settings appear
# in drill-config.sh. Distributions can replace this file with a
# distribution-specific version that sets environment variables and options
# specific to that distribution. Users should not put anything in this file;
# put user options in drill-env.sh instead.

# MapR-specific environment settings for Drill

MAPR_HOME="${MAPR_HOME:-__PREFIX__}"
DRILL_HOME=${DRILL_HOME:-"__PREFIX__/drill/drill-__DRILL_VERSION__"}

export HADOOP_VERSION=`cat __PREFIX__/hadoop/hadoopversion`
export HADOOP_HOME=${HADOOP_HOME:-"__PREFIX__/hadoop/hadoop-${HADOOP_VERSION}"}
export DRILL_JAVA_OPTS="${DRILL_JAVA_OPTS} -Djava.io.tmpdir=/tmp/drill -Djava.security.auth.login.config=/opt/mapr/conf/mapr.login.conf -Dhadoop.login=hybrid_keytab -Dzookeeper.sasl.client=false"
export DRILL_LOG_DIR=${DRILL_LOG_DIR:-"$DRILL_HOME/logs"}
export DRILL_PID_DIR=${DRILL_PID_DIR:-"__PREFIX__/pid"}
export MAPR_IMPERSONATION_ENABLED=${MAPR_IMPERSONATION_ENABLED:-"true"}

# Only set MAPR_TICKETFILE_LOCATION when invoked in context of drillbit setup NOT sqlline. It is expected
# to generate a separate ticket when sqlline is used.
if [ "$DRILLBIT_CONTEXT" = "1" ]; then
    export MAPR_TICKETFILE_LOCATION=${MAPR_TICKETFILE_LOCATION:-"__PREFIX__/conf/mapruserticket"}
fi
export SQLLINE_JAVA_OPTS="${SQLLINE_JAVA_OPTS} -Ddrill.customAuthFactories=org.apache.drill.exec.rpc.security.maprsasl.MapRSaslFactory -Dzookeeper.sasl.client=false -Djava.security.auth.login.config=/opt/mapr/conf/mapr.login.conf"

# Enable JMX for MaprMonitoring - tag to prevent older collectds from changing stuff
# Import JMX variables from /opt/mapr/conf/env_override.sh
# Import security java options if FIPS mode is enabled
MAPR_ENV_FILE="${MAPR_HOME}/conf/env.sh"
if [ -e "${MAPR_ENV_FILE}" ]; then
    . "${MAPR_ENV_FILE}"
fi

#Mapr JMX handling
isSecure="false"
if [ -f "${MAPR_HOME:-/opt/mapr}/conf/mapr-clusters.conf" ]; then
  isSecure=$(head -1 ${MAPR_HOME:-/opt/mapr}/conf/mapr-clusters.conf | grep -o 'secure=\w*' | cut -d= -f2)
fi

MAPR_JMX_PORT=${MAPR_JMX_DRILL_PORT:-6090}

if [ -z "$MAPR_JMXLOCALBINDING" ]; then
  MAPR_JMXLOCALBINDING="false"
fi

if [ -z "$MAPR_JMXAUTH" ]; then
  MAPR_JMXAUTH="false"
fi

if [ -z "$MAPR_JMXSSL" ]; then
  MAPR_JMXSSL="false"
fi

if [ -z "$MAPR_AUTH_LOGIN_CONFIG_FILE" ]; then
  MAPR_AUTH_LOGIN_CONFIG_FILE="${MAPR_HOME:-/opt/mapr}/conf/mapr.login.conf"
fi

if [ -z "$MAPR_LOGIN_CONFIG" ]; then
  MAPR_LOGIN_CONFIG="JMX_AGENT_LOGIN"
fi

if [ -z "$MAPR_JMXDISABLE" ] && [ -z "$MAPR_JMXLOCALHOST" ] && [ -z "$MAPR_JMXREMOTEHOST" ]; then
  echo "No MapR JMX options given - defaulting to local binding"
fi

if [[ ( -z "$MAPR_JMXDISABLE" || "$MAPR_JMXDISABLE" = 'false' ) && \
      ( -z "$MAPR_JMX_DRILL_ENABLE" || "$MAPR_JMX_DRILL_ENABLE" = "true" ) ]]; then

  # default setting for localBinding
  MAPR_JMX_OPTS="-Dcom.sun.management.jmxremote"

  if [ "$MAPR_JMXLOCALHOST" = "true" ] && [ "$MAPR_JMXREMOTEHOST" = "true" ]; then
    echo "WARNING: Both MAPR_JMXLOCALHOST and MAPR_JMXREMOTEHOST options are enabled - defaulting to MAPR_JMXLOCAHOST config"
    MAPR_JMXREMOTEHOST=false
  fi

  if [ "$isSecure" = "true" ] && [ "$MAPR_JMXREMOTEHOST" = "true" ]; then
    JMX_JAR=$(echo ${MAPR_HOME:-/opt/mapr}/lib/jmxagent*)
    if [ -n "$JMX_JAR" ] && [ -f ${JMX_JAR} ]; then
      MAPR_JMX_OPTS="-javaagent:$JMX_JAR \
      -Dmapr.jmx.agent.login.config=$MAPR_LOGIN_CONFIG"
      MAPR_JMXAUTH="true"
    else
      echo "jmxagent jar file missed"
      exit 1
    fi
  fi

  if [ "$MAPR_JMXAUTH" = "true" ]; then
    if [ "$isSecure" = "true" ]; then
      MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Djava.library.path=$DRILL_HOME/jars/pam"
      if [ -f "$MAPR_AUTH_LOGIN_CONFIG_FILE" ] && [ -f "${MAPR_HOME:-/opt/mapr}/conf/jmxremote.access" ]; then
        MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dcom.sun.management.jmxremote.authenticate=true \
        -Djava.security.auth.login.config=$MAPR_AUTH_LOGIN_CONFIG_FILE \
        -Dcom.sun.management.jmxremote.access.file=${MAPR_HOME:-/opt/mapr}/conf/jmxremote.access"
      else
        echo "JMX login config or access file missing - not starting since we are in secure mode"
        exit 1
      fi

      if [ "$MAPR_JMXREMOTEHOST" = "false" ]; then
        MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dcom.sun.management.jmxremote.login.config=$MAPR_LOGIN_CONFIG"
      fi
    else
      echo "JMX Authentication configured - not starting since we are not in secure mode"
      exit 1
    fi
  else
    MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
  fi

  if [ "$MAPR_JMXLOCALHOST" = "true" ] || [ "$MAPR_JMXREMOTEHOST" = "true" ]; then
    if [ "$MAPR_JMXSSL" = "true" ] && [ "$MAPR_JMXLOCALHOST" = "true" ] ; then
      echo "WARNING: ssl is not supported in localhost. Setting default to false"
      MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dcom.sun.management.jmxremote.ssl=false"
    else
      MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dcom.sun.management.jmxremote.ssl=false"
    fi

    if [ "$MAPR_JMXLOCALHOST" = "true" ]; then
      MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Djava.rmi.server.hostname=localhost \
      -Dcom.sun.management.jmxremote.host=localhost \
      -Dcom.sun.management.jmxremote.local.only=true"
    fi

    if [ -z "$MAPR_JMX_PORT" ]; then
      echo "WARNING: No JMX port given for Drill - disabling TCP base JMX service"
      MAPR_JMX_OPTS=""
    else
      if [ "$MAPR_JMXREMOTEHOST" = "true" ] && [ "$isSecure" = "true" ]; then
        MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dmapr.jmx.agent.port=$MAPR_JMX_PORT"
        echo "Enabling TCP JMX for Drill on port $MAPR_JMX_PORT"
      else
        MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dcom.sun.management.jmxremote.port=$MAPR_JMX_PORT"
        if [ "$MAPR_JMXLOCALHOST" = "true" ]; then
          echo "Enabling TCP JMX for Drill only on localhost port $MAPR_JMX_PORT"
        else
          echo "Enabling TCP JMX for Drill on port $MAPR_JMX_PORT"
        fi
      fi
    fi
  fi
    
  if [ "$MAPR_JMXLOCALBINDING" = "true" ] && [ -z "$MAPR_JMX_OPTS" ]; then
    echo "Enabling JMX local binding only"
    MAPR_JMX_OPTS="-Dcom.sun.management.jmxremote"
  fi
else
  echo "JMX disabled by user request"
  MAPR_JMX_OPTS=""
fi

export DRILL_JAVA_OPTS="${MAPR_JMX_OPTS} ${DRILL_JAVA_OPTS} ${MAPR_COMMON_JAVA_OPTS}"

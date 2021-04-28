#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
AGENT_PATH=`ls -1 $DIR/target/jvm-profiler-*.jar`
echo $AGENT_PATH

java -cp $AGENT_PATH:$JAVA_HOME/lib/tools.jar com.uber.profiling.tools.AttachClient $AGENT_PATH $*


#!/bin/sh

#
# Convenience script for submitting examples to Spark for execution.
#

USAGE="Usage: sparkie.sh [java|python|r|scala] [--master spark://url:port]"
LANGUAGE=$1
SPARKIE_HOME="/opt/spark/"
SRC_PATH="src/main"
PACKAGE="openlink/sparkoie"
NAME_COMPILED="UsingJDBC"
NAME_INTERPRETED="using_jdbc"
ID="using-jdbc"
APP_ARGS=""

if [ -z $1 ]; then
    echo $USAGE
    exit 1
fi
shift
set -e
cd $SPARKIE_HOME$ID

if [ $LANGUAGE = "java" ]; then
    mkdir -p target/java
    javac $SRC_PATH/java/$PACKAGE/J$NAME_COMPILED.java -classpath "$SPARK_HOME/jars/*" -d target/java
    cd target/java
    jar -cf ../J$NAME_COMPILED.jar *
    cd ../..
    APP_ARGS="--class openlink.sparkie.J${NAME_COMPILED} target/J${NAME_COMPILED}.jar ${APP_ARGS}"
elif [ $LANGUAGE = "python" ]; then
	APP_ARGS="$SRC_PATH/python/${NAME_INTERPRETED}.py ${APP_ARGS}"
elif [ $LANGUAGE = "r" ]; then
	APP_ARGS="$SRC_PATH/r/${NAME_INTERPRETED}.R ${APP_ARGS}"
elif [ $LANGUAGE = "scala" ]; then
    mkdir -p target/scala
    scalac $SRC_PATH/scala/$PACKAGE/S$NAME_COMPILED.scala -classpath "$SPARK_HOME/jars/*" -d target/scala
    cd target/scala
    jar -cf ../S$NAME_COMPILED.jar *
    cd ../..
    APP_ARGS="--class openlink.sparkie.S${NAME_COMPILED} target/S${NAME_COMPILED}.jar ${APP_ARGS}"
else
    echo $USAGE
    exit 1
fi

$SPARK_HOME/bin/spark-submit "$@" $APP_ARGS

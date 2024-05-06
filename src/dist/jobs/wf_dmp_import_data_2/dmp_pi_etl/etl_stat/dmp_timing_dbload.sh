#!/bin/sh


WORKDIR=/app/dmp_pi/dmp_pi_etl/etl_stat
WORKDATE=`date '+%Y%m%d'`

### DB LOAD

cd $WORKDIR/dmp_timing_dbload

/app/jdk/bin/javac -cp "lib/*" *.java

/app/jdk/bin/java -ea -cp ".:lib/*" Dic_mysql_file_insert $WORKDIR/$WORKDATE




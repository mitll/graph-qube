#!/usr/bin/env bash
mvn install:install-file -Dfile=lib/json-lib-2.4-jdk15.jar -DgroupId=net.sf.json-lib -DartifactId=json-lib -Dversion=2.4 -Dpackaging=jar
mvn install:install-file -Dfile=lib/commons-beanutils-1.8.0.jar -DgroupId=beans -DartifactId=beans -Dversion=1.8.0 -Dpackaging=jar
mvn install:install-file -Dfile=lib/commons-logging-1.1.1.jar -DgroupId=commons-logging -DartifactId=commons-logging -Dversion=1.1.1 -Dpackaging=jar
mvn install:install-file -Dfile=lib/ezmorph-1.0.6.jar -DgroupId=ez -DartifactId=ez -Dversion=1.0.6 -Dpackaging=jar


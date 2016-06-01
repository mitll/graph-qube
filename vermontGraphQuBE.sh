#!/usr/bin/env bash
mvn exec:java -Dexec.mainClass="mitll.xdata.GraphQuBEServer" -Dexec.args="-props=vermont.properties" -Dexec.cleanupDaemonThreads=false

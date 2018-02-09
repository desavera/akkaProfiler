#!/usr/bin/env bash
# sbt assembly
rm -rf dockerbuild/*.jar
cp target/scala-2.11/dmon-api-assembly-1.0.jar dockerbuild/
docker build -t registry.b2w.io/b2wdigital/dmon-api:1.0.057 dockerbuild/.

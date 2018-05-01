#!/bin/bash

set -x

cd `dirname "$0"`;

sbt clean assembly

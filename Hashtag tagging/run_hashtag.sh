#!/bin/bash
ant

hadoop jar 18645-proj3-0.1-latest.jar -program hashtagsim -input data/tweets500/tweets500.txt -output data/$1 -tmpdir tmp

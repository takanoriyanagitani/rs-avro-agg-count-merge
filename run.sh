#!/bin/sh

genavro(){
	export ENV_SCHEMA_FILENAME=sample.d/sample.avsc

	cat sample.d/sample1.jsonl |
		json2avrows |
		cat > ./sample.d/sample1.avro

	cat sample.d/sample2.jsonl |
		json2avrows |
		cat > ./sample.d/sample2.avro

}

set -o pipefail

#genavro || exec sh -c 'echo unable to generate avro files.; exit 1'

filenames=
filenames="${filenames} sample.d/sample1.avro"
filenames="${filenames} sample.d/sample2.avro"

export ENV_MERGE_KEY_NAME=key
export ENV_MERGE_CNT_NAME=val

ENV_SCHEMA_FILENAME=./sample.d/output.avsc \
	./rs-avro-agg-count-merge $filenames |
	rq -aJ |
	jq -c

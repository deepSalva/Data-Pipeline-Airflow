#!/bin/bash
#
# TO-DO: run the following command and observe the JSON output:
# airflow connections get aws_credentials -o json
#
#[{"id": "1",
# "conn_id": "aws_credentials",
# "conn_type": "aws",
# "description": "",
# "host": "",
# "schema": "",
# "login": "AKIAVJNJxxx",
# "password": "2Vxhuxxxxxxxxx",
# "port": null,
# "is_encrypted": "False",
# "is_extra_encrypted": "False",
# "extra_dejson": {},
# "get_uri": "aws://AKIAVJNJxxx:2Vxhuxxxxxxxxx"
#}]
#
# Copy the value after "get_uri":
#
# For example: aws://AKIA4QE4NTH3R7EBEANN:s73eJIJRbnqRtll0%2FYKxyVYgrDWXfoRpJCDkcG2m@
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add 'aws_credentials' --conn-json '{
        "conn_type": "aws",
        "login": "AKIAVJNJxxx",
        "password": "2Vxhuxxxxxxxxx",
        "host": "",
        "port": null,
        "schema": "",
        "extra": {
            "param1": "val1",
            "param2": "val2"
        }
    }'
#
#
# TO-DO: run the follwing command and observe the JSON output:
# airflow connections get redshift -o json
#
# [{"id": "3",
# "conn_id": "redshift",
# "conn_type": "redshift",
# "description": "",
# "host": "default.xxxx.us-east-1.redshift-serverless.amazonaws.com",
# "schema": "dev",
# "login": "awsuser",
# "password": "awsuser",
# "port": "5439",
# "is_encrypted": "False",
# "is_extra_encrypted": "False",
# "extra_dejson": {},
# "get_uri": "redshift://<user>:<user_psw>@<host>:<port>/<schema>"}]
#
# Copy the value after "get_uri":
#
# For example: redshift://<user>:<user_psw>@<host>:<port>/<schema>
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add redshift --conn-uri 'redshift://<user>:<user_psw>@<host>:<port>/<schema>'
#
# TO-DO: update the following bucket name to match the name of your S3 bucket and un-comment it:
#
airflow variables set s3_bucket '<your-bucket-example>'
#
# TO-DO: un-comment the below line:
#
airflow variables set s3_prefix data-pipelines
#!/bin/bash

# 환경 변수 설정
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
export PYSPARK_PYTHON='/bin/python3.6'

# Spark 폴더 내에서 실행
spark-submit \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/bin/python3.6 \
  --conf spark.executorEnv.PYSPARK_PYTHON=/bin/python3.6 \
  seoul_products_preprocessing.py

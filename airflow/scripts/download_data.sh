#!/bin/bash

set -e

# Define the taxi types and years to loop through
TAXI_TYPES=("yellow" "green")
YEARS=("2019" "2020")

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

# download taxi data
for TAXI_TYPE in "${TAXI_TYPES[@]}"; do
  for YEAR in "${YEARS[@]}"; do
    for MONTH in {1..12}; do
      FMONTH=`printf "%02d" ${MONTH}`

      URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
      
      LOCAL_PREFIX="/opt/airflow/data/raw/${TAXI_TYPE}/${YEAR}"
      LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
      LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

      echo "Downloading ${URL} to ${LOCAL_PATH}"
      mkdir -p ${LOCAL_PREFIX}
      wget ${URL} -O ${LOCAL_PATH}

    done
  done
done

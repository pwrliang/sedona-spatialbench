#!/usr/bin/env bash

for dev in cpu gpu; do
  for q in q2 q11; do
    log_file="logs/plan_${q}_${dev}.log"
    if [[ ! -f "${log_file}" ]]; then
      python3 analysis_${q}.py -d benchmark-data-sf1 -p 1 $dev -r 5 |& tee logs/plan_${q}_${dev}.log
    fi
  done
done

#!/usr/bin/env bash
kubectl -n nsdva get pods
kubectl -n nsdva exec -it $1 -c $2  bash

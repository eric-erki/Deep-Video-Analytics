#!/usr/bin/env bash
kubectl get pods
kubectl exec -it $1 -c $2  bash

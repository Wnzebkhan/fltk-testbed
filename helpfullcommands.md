start kubernetes dashboard port forward
```bash
export POD_NAME=$(kubectl get pods -n default -l "app.kubernetes.io/name=kubernetes-dashboard,app.kubernetes.io/instance=kubernetes-dashboard" -o jsonpath="{.items[0].metadata.name}")
kubectl -n default port-forward $POD_NAME 8443:8443
```

start tensorboard port forward
```bash
kubectl port-forward -n test fl-extractor 6006:6006
```

release new version
```bash
DOCKER_BUILDKIT=1 docker build . --tag gcr.io/qpe-lab-325820/fltk
docker push gcr.io/qpe-lab-325820/fltk
```

remove orchestrator
```bash
helm uninstall -n test orchestrator
```
remove old experiments
```bash
kubectl delete pytorchjobs.kubeflow.org --all --all-namespaces
```
reinstall orchestrator
```bash
helm install orchestrator ./orchestrator -f fltk-values.yaml -n test
```

delete logged data
```bash
kubectl exec -n test fl-extractor --  rm -rf logging/*
```


### Show all pods
```bash
kubectl get pods --all-namespaces  
```

### Pod logging
```bash
kubectl logs -n ${NAMESPACE} -f ${POD_NAME}
```

### Drop into pods shell
```bash
kubectl exec -it -n ${NAMESPACE} ${POD_NAME} -- /bin/sh
```

### Restart all pods in namespace
```bash
kubectl -n ${NAMESPACE}> delete pods --all
```

### Switch context
```bash
kubectl config use-context ${PROJECT ID}
```
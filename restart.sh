DOCKER_BUILDKIT=1 docker build . --tag gcr.io/group5fairness/fltk
docker push gcr.io/group5fairness/fltk

cd charts

#helm uninstall -n test extractor
helm uninstall -n test orchestrator
kubectl delete pytorchjobs.kubeflow.org --all --all-namespaces
kubectl exec -n test fl-extractor --  rm -rf logging/*
#helm install extractor ./extractor -f fltk-values.yaml -n test
helm install orchestrator ./orchestrator -f fltk-values.yaml -n test

cd ..

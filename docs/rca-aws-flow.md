# RCA Flow on AWS EC2 + kubeadm

This runbook adapts `GoogleCloudPlatform/microservices-demo` to the RCA flow:

`Load Generator -> Microservices on Kubernetes -> Jaeger -> Trace Puller -> Parse/Clean -> Feature Extraction -> Anomaly Detection -> Graph RCA -> Decision/Recovery`

## 1. Deploy observability

Apply Jaeger in its own namespace:

```bash
kubectl apply -f observability/jaeger/jaeger.yaml
kubectl get pods -n observability
kubectl -n observability port-forward svc/jaeger 16686:16686
```

Open `http://localhost:16686`.

## 2. Deploy the workload

This overlay does three things:

- creates namespace `app`
- keeps `loadgenerator`
- changes `frontend-external` from `LoadBalancer` to `NodePort:30080`
- enables tracing for the services that already support OTel and points them to `jaeger.observability.svc.cluster.local:4317`

Apply it with:

```bash
kubectl apply -k kustomize/overlays/rca-aws-app
kubectl get pods -n app
kubectl get svc -n app frontend-external
```

Open the shop from any node public IP:

```text
http://<node-public-ip>:30080
```

## 3. Verify the flow A (monitoring and anomaly inputs)

Wait for `loadgenerator` to start sending traffic:

```bash
kubectl get pods -n app
kubectl logs -n app deploy/loadgenerator --tail=50
```

Check Jaeger UI for traces from:

- `frontend`
- `checkoutservice`
- `currencyservice`
- `emailservice`
- `paymentservice`
- `productcatalogservice`
- `recommendationservice`

These services are the best initial set for trace-based RCA because the repo already includes OTel hooks for them.

## 4. Build the data pipeline

Create these modules next:

- `pipeline/trace-puller`
- `pipeline/preprocess`
- `ml/anomaly`
- `ml/graph-builder`
- `ml/rca-gat`
- `services/analysis-api`
- `services/decision-engine`
- `services/recovery-executor`

Recommended execution order:

1. `trace-puller`: query Jaeger API by time window and store raw JSON
2. `preprocess`: parse spans, clean bad traces, build per-trace feature vectors
3. `ml/anomaly`: train autoencoder and produce `anomaly_score`
4. `ml/graph-builder`: build service-call graphs for anomalous windows
5. `ml/rca-gat`: train GAT for `top-1` and `top-3` root cause
6. `analysis-api`: orchestrate anomaly first, RCA second
7. `decision-engine`: map RCA outputs to restart/scale/rollback actions
8. `recovery-executor`: call Kubernetes API to apply the action

## 5. Output contract

The integrated inference response should look like:

```json
{
  "anomaly_score": 0.91,
  "is_anomaly": true,
  "root_cause_service": "paymentservice",
  "confidence": 0.84,
  "recommended_action": "restart_pod"
}
```

## 6. Recovery mapping

Start with rule-based recovery:

- `pod crash / readiness failure -> restart deployment`
- `sustained latency + high load -> scale deployment`
- `issue appears right after rollout -> rollout undo`

Use a high confidence threshold before auto-executing actions.

# AIOps CI/CD + GitOps Layout

This directory isolates the deployable AIOps runtime from the existing microservices demo manifests so the project can evolve toward AKS, k3s, or any other Kubernetes target without disturbing the original application manifests.

## Structure

- `docker/`
  - repo-root Dockerfiles for `anomaly-service`, `rca-service`, `orchestrator`, and `dashboard`
- `apps/<service>/base/`
  - service-level Kubernetes base manifests
- `environments/dev/`
  - development overlay used by Argo CD and local cluster validation
- `environments/prod/`
  - production overlay with higher replica count and stricter recovery defaults
- `argocd/`
  - Argo CD `Application` examples

## Recommended repo workflow

1. `main` stores source code, Dockerfiles, and GitOps manifests.
2. GitHub Actions builds images and pushes them to your registry.
3. A follow-up workflow or image updater changes the image tag in `deploy/aiops/environments/<env>/kustomization.yaml`.
4. Argo CD watches the environment path and syncs it to the cluster.

## Image naming convention

Expected images:

- `ghcr.io/<owner>/aiops-anomaly-service:<tag>`
- `ghcr.io/<owner>/aiops-rca-service:<tag>`
- `ghcr.io/<owner>/aiops-orchestrator:<tag>`
- `ghcr.io/<owner>/aiops-dashboard:<tag>`

Replace `ghcr.io/<owner>` with ACR, ECR, or another registry later without changing the base manifests.

## Artifact strategy

Current Dockerfiles bake the trained model artifacts directly into the image for simplicity:

- anomaly image contains `data_anomaly_balanced_v3/models/anomaly_gbrt_balanced`
- RCA image contains `data_rca_balanced_v3/models/rca_gat_like_cuda`
- dashboard image contains sample graph tensors for demo mode

For production later, move these artifacts to object storage or a model volume and replace the baked-in path with a mounted directory.

## Suggested next steps

1. Replace placeholder image registry names in the overlays.
2. Create GitHub secrets for registry authentication.
3. Apply `deploy/aiops/environments/dev` to a cluster and test service-to-service traffic.
4. Install Argo CD and apply `deploy/aiops/argocd/application-dev.yaml`.

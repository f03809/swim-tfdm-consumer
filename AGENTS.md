# SWIM TFDM Consumer — Agent Notes

## Deployment

The project uses GitHub Actions to build a Docker image, push it to GHCR, and deploy to a Kubernetes cluster via a self-hosted runner.

- `.github/workflows/deploy.yml` runs on every push to `main`.
- The `build` job builds `ghcr.io/f03809/swim-tfdm-consumer:latest` and a short SHA tag, then pushes it to GHCR.
- The `deploy` job runs on the self-hosted runner, renders `k8s/overlays/prod` with `kubectl kustomize`, applies the manifests, and restarts the `swim-tfdm-consumer` deployment.

## Agent Instructions

After making code changes, commit and push them to the `main` branch so the `deploy.yml` GitHub Actions pipeline runs automatically.

## Shared Homelab / Proxmox Setup

- All three projects (`swim-kafka-producer`, `swim-tfdm-consumer`, `aircraft-tracker`) are intended to run on a Proxmox homelab.
- The `swim-kafka-producer` and `swim-tfdm-consumer` share the same k3s Kubernetes cluster. The cluster is deployed and managed by a self-hosted GitHub Actions runner.
- The self-hosted runner for this consumer repo is named `proxmox-k3s-tfdm` and runs on the k3s control-plane VM.
- MongoDB is a Docker container on the dedicated MongoDB VM at `10.0.0.16`. It currently uses `MONGODB_URL: mongodb://10.0.0.16:27017`.
- Kafka is at `10.0.0.94:9092` and is shared by the producer and consumer.
- MongoDB change streams are used by the `swim-tfdm-consumer-dispatcher`, which requires MongoDB to run as a replica set.
- Planned MongoDB replica set (`rs0`):
  - Primary data node: `10.0.0.16`
  - Secondary data node: external Proxmox VM (IP and SSH credentials to be provided)
  - Arbiter: lightweight data-less voter on `10.0.0.16`
- The Devin SSH key for the second MongoDB node is `.devin/keys/devin_loki_key` (public key in `.devin/keys/devin_loki_key.pub`). The user will authorize this key on the new VM.
- The `deploy.yml` step deletes the stale `swim-tfdm-consumer-dispatcher` Deployment before applying manifests, so immutable selector changes can be corrected on the next deploy.
- The `aircraft-tracker` is currently local-only and not yet deployed to a Proxmox VM. It should be moved to a VM and auto-deployed later.

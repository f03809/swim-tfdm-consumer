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
- MongoDB is installed as a `systemd` package on the dedicated MongoDB VM at `10.0.0.16`. It is part of the `rs0` replica set.
- Kafka is at `10.0.0.94:9092` and is shared by the producer and consumer.
- MongoDB change streams are used by the `swim-tfdm-consumer-dispatcher`, which requires MongoDB to run as a replica set.
- MongoDB replica set (`rs0`):
  - Primary data node: `10.0.0.16:27017`
  - Secondary data node: `10.1.1.27:27017` (currently initial-syncing from the primary; ~89 GB of `swim_tfdm_consumer` data to copy)
  - Arbiter: `10.0.0.16:27018` (data-less voter)
- Default write concern was set to `{ w: 1 }` while the secondary is initial-syncing, so consumer writes do not block waiting for a majority. After the secondary becomes `SECONDARY`, consider raising this back to majority if durability is more important than availability.
- The Devin SSH key for the MongoDB nodes is `.devin/keys/devin_loki_key` (public key in `.devin/keys/devin_loki_key.pub`). It is authorized on both the primary (`root@10.0.0.16:22`) and secondary (`root@10.1.1.27:22`).
- The `deploy.yml` step deletes the stale `swim-tfdm-consumer-dispatcher` Deployment before applying manifests, so immutable selector changes can be corrected on the next deploy.
- The `aircraft-tracker` is currently local-only and not yet deployed to a Proxmox VM. It should be moved to a VM and auto-deployed later.

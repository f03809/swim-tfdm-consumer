# SWIM TFDM Consumer — Agent Notes

## Deployment

The project uses GitHub Actions to build a Docker image, push it to GHCR, and deploy to a Kubernetes cluster via a self-hosted runner.

- `.github/workflows/deploy.yml` runs on every push to `main`.
- The `build` job builds `ghcr.io/f03809/swim-tfdm-consumer:latest` and a short SHA tag, then pushes it to GHCR.
- The `deploy` job runs on the self-hosted runner, renders `k8s/overlays/prod` with `kubectl kustomize`, applies the manifests, and restarts the `swim-tfdm-consumer` deployment.

## Agent Instructions

After making code changes, commit and push them to the `main` branch so the `deploy.yml` GitHub Actions pipeline runs automatically.

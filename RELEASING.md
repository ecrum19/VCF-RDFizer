# Releasing VCF-RDFizer

This repository releases the Python wrapper to PyPI, a multi-architecture
Docker image to Docker Hub, and the wrapper package to conda-forge.

## Preconditions

- The target branch has passed `.github/workflows/tests.yml`.
- The GitHub `pypi` environment is configured as a trusted publisher for the
  `vcf-rdfizer` PyPI project.
- `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` are configured as repository
  secrets.
- Release from the branch intended to become `main`; do not tag an unmerged
  development branch.

## 1. Prepare And Validate The Source Release

Choose a new semantic version, for example `1.2.4`, then run:

```bash
python3 scripts/release.py 1.2.4
git diff --check
python3 -m unittest discover -s test -p "test_*_unit.py" -q
git add pyproject.toml CITATION.cff README.md conda-recipe/meta.yaml conda-recipe/README.md
git commit -m "Release v1.2.4"
git push origin main
git tag -a v1.2.4 -m "VCF-RDFizer v1.2.4"
git push origin v1.2.4
```

The version must be committed before creating the tag. The PyPI workflow
checks that the tag and all version markers agree; it deliberately does not
modify metadata in CI.

The pushed tag runs both publishing workflows. The Python workflow builds,
checks, installs, and publishes only on a `vMAJOR.MINOR.PATCH` tag. The Docker
workflow publishes `X.Y.Z`, `vX.Y.Z`, and `latest` as a single
`linux/amd64,linux/arm64` manifest.

To exercise the PyPI build checks without uploading, manually dispatch the
`publish-python` workflow and provide an existing release tag. It checks out
that tag and never runs the publish job for a manual dispatch.

## 2. Update Conda-Forge

The conda-forge source archive does not exist until the tag is pushed. After
the tag is available, populate this repository's reference recipe with the
real checksum:

```bash
python3 scripts/release.py 1.2.4 --fetch-conda-sha256
```

Copy the resulting `conda-recipe/meta.yaml` values into a branch in your fork
of [`conda-forge/vcf-rdfizer-feedstock`](https://github.com/conda-forge/vcf-rdfizer-feedstock),
edit `recipe/meta.yaml`, and open a PR. For a version bump, keep
`build/number: 0`. The feedstock CI builds and publishes the conda package
after the PR merges.

## 3. Verify Published Artifacts

```bash
python3 -m pip install --upgrade vcf-rdfizer==1.2.4
vcf-rdfizer --help

conda install -c conda-forge vcf-rdfizer=1.2.4
vcf-rdfizer --help

docker pull ecrum19/vcf-rdfizer:1.2.4
docker buildx imagetools inspect ecrum19/vcf-rdfizer:1.2.4
```

The Docker inspection must list both `linux/amd64` and `linux/arm64`.

# Conda Recipe Notes

This directory contains a starter `meta.yaml` for publishing `vcf-rdfizer` on conda-forge.

## Before submitting to conda-forge

1. Create a GitHub release/tag (for example `v1.0.0`).
2. Download the source tarball and compute sha256:
   ```bash
   curl -L -o vcf-rdfizer.tar.gz \
     https://github.com/ecrum19/VCF-RDFizer/archive/refs/tags/v1.0.0.tar.gz
   shasum -a 256 vcf-rdfizer.tar.gz
   ```
3. Replace `version` and `sha256` in `meta.yaml`.
4. Submit to [`conda-forge/staged-recipes`](https://github.com/conda-forge/staged-recipes).

## Ongoing releases

After the initial feedstock is created:

1. Open PR in your feedstock bumping `version` and `sha256`.
2. Merge once CI passes.
3. Verify install:
   ```bash
   conda install -c conda-forge vcf-rdfizer
   vcf-rdfizer --help
   ```

## Runtime requirement reminder

`vcf-rdfizer` uses Docker for actual conversion/compression execution.
Conda installs the wrapper CLI; users still need Docker installed and running.

## License scope reminder

The conda package distributes the Python wrapper only.
Third-party runtime tools used by the Docker image (for example HDT-cpp and
RMLStreamer) are documented in `THIRD_PARTY_NOTICES.md`.

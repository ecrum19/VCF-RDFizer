# Conda Recipe Notes

This directory contains the release-reference recipe for `vcf-rdfizer`.
The published recipe is maintained in the existing
[`conda-forge/vcf-rdfizer-feedstock`](https://github.com/conda-forge/vcf-rdfizer-feedstock);
do not submit this package to `staged-recipes`.

## Before submitting to conda-forge

1. Commit the version bump, then create and push a Git tag (for example `v1.2.3`).
2. Download the source tarball and compute sha256:
   ```bash
   curl -L -o vcf-rdfizer.tar.gz \
     https://github.com/ecrum19/VCF-RDFizer/archive/refs/tags/v1.2.3.tar.gz
   shasum -a 256 vcf-rdfizer.tar.gz
   ```
3. Replace `version` and `sha256` in the feedstock's `recipe/meta.yaml`.
4. Open a PR against [`conda-forge/vcf-rdfizer-feedstock`](https://github.com/conda-forge/vcf-rdfizer-feedstock).

## Ongoing releases

After the initial feedstock is created:

1. Fork `conda-forge/vcf-rdfizer-feedstock` and create a branch in your fork.
2. Bump `version`, set the SHA256 of the corresponding pushed Git tag archive,
   and reset `build/number` to `0` in `recipe/meta.yaml`.
3. Open a PR from that branch to the feedstock.
4. Merge once CI passes.
5. Verify install:
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

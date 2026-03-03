# Third-Party Notices

VCF-RDFizer (this repository) is released under the MIT License (see `LICENSE`).

The Docker image also includes third-party software. Their licenses remain with
their respective authors and apply to those components.

## Included in Docker image

1. `HDT-cpp`
- Repository: <https://github.com/rdfhdt/hdt-cpp>
- Usage in this project: RDF (`.nt`) to HDT conversion (`rdf2hdt`, `hdt2rdf`)
- Upstream license statement: LGPL (see upstream project documentation and `libhdt/COPYRIGHT`)
- License files copied into image:
  - `/usr/share/licenses/vcf-rdfizer/HDT-CPP.COPYRIGHT`

2. `RMLStreamer`
- Repository: <https://github.com/RMLio/RMLStreamer>
- Usage in this project: TSV-to-RDF conversion
- Upstream license: MIT
- License files copied into image:
  - `/usr/share/licenses/vcf-rdfizer/RMLStreamer.LICENSE`

## Notes for package users

- The `pip` and `conda` packages install the Python wrapper CLI only.
- The third-party binaries above are bundled in the Docker image, not in the
  Python/conda package payload.

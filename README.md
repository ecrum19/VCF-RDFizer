## RMLStreamer
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3887065.svg)](https://doi.org/10.5281/zenodo.3887065)

VCF-RDFizer generates [RDF](https://www.w3.org/2001/sw/wiki/RDF) serializations 
of [VCF files]() as nquads [NQUADS]()
using [RML](http://rml.io/) rules and the [RMLStreamer]() application. 

Documentation regarding the use of (custom) functions can be found [here]().


### VCF Commands to Execute
Download the RMLStreamer STANDALONE jar file:
```
wget --content-disposition --trust-server-names \
  https://github.com/RMLio/RMLStreamer/releases/download/v2.5.0/RMLStreamer-v2.5.0-standalone.jar
```

Install Brotli:
```
sudo apt install brotli
```

Install HDT library:
```
git clone git@github.com:rdfhdt/hdt-java.git
cd hdt-java

sudo apt install openjdk-11-jdk
sudo update-alternatives --config java  # and choose jdk-11 as default 

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
export PATH="$JAVA_HOME/bin:$PATH"

mvn clean install -DskipTests

cd hdt-java-cli
mvn clean install -DskipTests
```

Generate tsv representations of vcf files (for all VCFs to be converted):
```
bash vcf_as_tsv.sh vcf_files/ 
```

Run VCF Conversion:
```
bash run_test.sh
```

## TODO:
Develop a custom conversion implementation for directly converting VCF files (without TSV conversion)
Make the run script automatically sense the vcf (converted to tsv) files for conversion...



### Quick start (standalone)

* Download `RMLStreamer-<version>-standalone.jar` from the [latest release](https://github.com/RMLio/RMLStreamer/releases/latest).
* Run it as
```
$ java -jar RMLStreamer-<version>-standalone.jar <commands and options>
```

See [Basic commands](#basic-commands) (where you replace `$FLINK_BIN run <path to RMLStreamer jar>` with `java -jar RMLStreamer-<version>-standalone.jar`)
and [Complete RMLStreamer usage](#complete-rmlstreamer-usage) for
examples, possible commands and options.

### Quick start (Docker - the fast way to test)

This runs the stand-alone version of RMLStreamer in a Docker container.
This is a good way to quickly test things or run RMLStreamer on a single machine, 
but you don't have the features of a Flink cluster set-up (distributed, failover, checkpointing). 
If you need those features, see [docker/README.md](docker/README.md). 
   
#### Example usage:

```
$ docker run -v $PWD:/data --rm rmlio/rmlstreamer toFile -m /data/mapping.ttl -o /data/output
```

#### Build your own image:

This option builds RMLStreamer from source and puts that build into a Docker container ready to run.
The main purpose is to have a one-time job image.

```
$ ./buildDocker.sh
```

If the build succeeds, you can invoke it as follows.
If you go to the directory where your data and mappings are,
you can run something like (change tag to appropriate version):

```
$ docker run -v $PWD:/data --rm rmlstreamer:v2.5.1-SNAPSHOT toFile -m /data/mapping.ttl -o /data/output.ttl 
```

There are more options for the script, if you want to use specific tags or push to Docker Hub:
```
$ ./buildDocker.sh -h

Build and push Docker images for RMLStreamer

buildDocker.sh [-h]
buildDocker.sh [-a][-n][-p][-u <username>][-v <version>]
options:
-a   Build for platforms linux/arm64 and linux/amd64. Default: perform a standard 'docker build'
-h   Print this help and exit.
-n   Do NOT (re)build RMLStreamer before building the Docker image. This is risky because the Docker build needs a stand-alone version of RMLStreamer.
-u <username>  Add an username name to the tag name as on Docker Hub, like <username>/rmlstreamer:<version>.
-p   Push to Docker Hub repo. You must be logged in for this to succeed.
-v <version>       Override the version in the tag name, like <username>/rmlstreamer:<version>. If not given, use the current version found in pom.xml.
```

### Moderately quick start (Docker - the recommended way)

If you want to get RMLStreamer up and running within 5 minutes using Docker, check out [docker/README.md](docker/README.md)






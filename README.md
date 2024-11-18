# EleceDb

- [Overview](#overview)
- [How to run EleceDb](#how-to-run-elecedb)
    - [Manual compilation](#manual-compilation)
    - [Docker](#docker)
- [Environment variables](#environment-variables)
- [Documentation](#documentation)

## Overview

EleceDb is a simple sql database, created with the purpose of gaining knowledge on how databases work internally, mostly
on how to create a parser, how indexes and rows are stored, how b+ trees work, how queries are processed, etc...

In its core, EleceDb can handle most queries, but in its current form, it does not support transactions. Since this was
my first attempt at creating a database there are a few shortcomings in my implementation, and supporting transactions
would require a deep refactoring of the whole project, therefore I've decided to treat this as a limitation, and maybe
in the future I can add support for it.

## How to run EleceDb

There are two ways to run an instance of EleceDb:

- manually, which requires the user to compile the project locally and run the jar;
- through docker, which just launches a docker container.

### Manual compilation

To compile the project locally, you need to install:

- [Java 23](https://www.oracle.com/java/technologies/downloads/)
- [Git](https://git-scm.com/downloads)
- [Maven](https://maven.apache.org/download.cgi)

Then you also need to clone this repository:

> git clone https://github.com/BernardoFMF/eleceDb.git

Afterward on the directory of the project, run:

> mvn clean install

This will compile the project, and the jar will be on

> /{projectDirectory}/target/eleceDb-{version}-jar-with-dependencies.jar

To run the jar, execute the following command:

> java -jar .\eleceDb-{version}-jar-with-dependencies.jar

This will launch an instance of EleceDb running on the default port (3000).
There are several configurations possible, and if defined as environment variables in the system, it's possible to
change the default values.

### Docker

To launch a docker container, you need to install:

- [Docker](https://docs.docker.com/desktop/)

Then run:

> docker pull bernardofmf/elecedb:latest

This will download the latest version of the database, but you can launch other versions if desired:

- [Docker hub](https://hub.docker.com/repository/docker/bernardofmf/elecedb/general)

You then need to start the docker daemon.
Finally, to start a container you can run:

> docker run -d -p 3000:3000 bernardofmf/elecedb:latest

This is just an example, where the port 3000 is exposed, obviously if the default port of the database is changed, then
the docker command needs to be adapted to follow the formula:

> docker run -d -p <mappedPort>:<elece.db.port> bernardofmf/elecedb:latest

## Environment variables

Below is a comprehensive list of all possible configurations that can be done. This isn't required, as all of these
configurations have default values, so only change these if you want to experiment.

| Env variable name                        | Description                                                                                                                                                                                                            | Default value | Possible values                                           |
|------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|-----------------------------------------------------------|
| elece.db.port                            | The port of the tcp server                                                                                                                                                                                             | 3000          | -                                                         |
| elece.db.pool.coreSize                   | The number of threads used to accept new clients that should be present even when idle                                                                                                                                 | 5             | -                                                         |
| elece.db.pool.maxSize                    | Represents the max number of threads/clients that can be connected at once                                                                                                                                             | 20            | -                                                         |
| elece.db.keepAliveTime                   | The time that idle client threads will wait for new tasks before terminating                                                                                                                                           | 100           | -                                                         |
| elece.db.closeTimeoutTime                | Defines the time the database will wait before giving a timeout when closing a file channel                                                                                                                            | 5             | -                                                         |
| elece.db.acquisitionTimeoutTime          | Defines the time the database will wait before giving a timeout when acquiring a file channel                                                                                                                          | 10            | -                                                         |
| elece.db.timeoutUnit                     | The time unit used when throwing timeouts                                                                                                                                                                              | SECONDS       | SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS, MINUTES |
| elece.db.btree.degree                    | The degree of the b+ tree, defines the min and max number of keys a node can hold, and also the min an max number of children a node can have                                                                          | 10            | -                                                         |
| elece.db.btree.growthNodeAllocationCount | Used to calculate offsets when storing indexes to disk                                                                                                                                                                 | 20            | -                                                         |
| elece.db.baseDbPath                      | Defines the base path directory, where all of the database files will reside                                                                                                                                           | /temp         | -                                                         |
| elece.db.btree.maxFileSize               | Defines the max size of the index chunk files, when the limit is reached, a new file is created for the next chunk                                                                                                     | -1            | if -1 then the file size is unlimited;                    |
| elece.db.indexStorageManagerStrategy     | If the configuration is set to COMPACT, then all of the indexes of the b+ tree are stored in the same file, otherwise if set to ORGANIZED the indexes are stored in different files according to the chunk they are in | COMPACT       | COMPACT, ORGANIZED                                        |
| elece.db.fileHandlerStrategy             | Defines if file channels can be opened with or without limit                                                                                                                                                           | UNLIMITED     | UNLIMITED, LIMITED                                        |
| elece.db.fileDescriptorAcquisitionSize   | This is used when the file handler strategy is set to LIMITED, it defines the number of concurrent open file channels possible                                                                                         | 20            | -                                                         |
| elece.db.fileHandlerPoolThreads          | Mainly used to create thread pools used for asynchronous file channels                                                                                                                                                 | 10            | -                                                         |
| elece.db.sessionStrategy                 | Decides whether or not the changes done to the b+ tree are immediately stored in disk, or if they stay in memory until a commit or rollback is done                                                                    | IMMEDIATE     | IMMEDIATE, COMMITTABLE                                    |
| elece.db.dbPageSize                      | Defines the page size of the database files                                                                                                                                                                            | 64000         | -                                                         |
| elece.db.dbPageMaxFileSize               | Defines the maximum page size of the database files, when the limit is reached, a new file is created for the next page                                                                                                | 100           | -                                                         |
| elece.db.dbPageBufferSize                | Defines the capacity for the page buffer, to hold loaded pages in memory                                                                                                                                               | -1            | if -1 then the page buffer size is unlimited;             |
| elece.db.dbQueryCacheSize                | Defines the capacity of the memory cache used in order by queries, when the limit is reached, the rows are stored in a temp file and the cache is drained                                                              | 50            | -                                                         |

## Documentation

Here you'll find links to the documentation regarding different topics and implementation details of this project.

**TODO**
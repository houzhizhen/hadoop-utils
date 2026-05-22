# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

hadoop-utils is a collection of Hadoop HDFS and YARN testing/benchmarking tools developed at Baidu. It is **not** a library — it is a toolbox of executable utilities designed to be run via `hadoop jar` on Hadoop clusters. The project targets Hadoop 3.3.6 and compiles to Java 8.

## Build Commands

```bash
# Full build (skip tests)
mvn -T 1C clean package -DskipTests -B

# macOS build (forces x86_64 arch for native dependencies)
mvn -T 1C clean package -DskipTests -Dos.arch=x86_64 -B

# Build a single module
mvn -T 1C clean package -DskipTests -pl fs-test -B
mvn -T 1C clean package -DskipTests -pl yarn -B
```

Output JARs: `fs-test/target/fs-test-1.8.10.jar`, `yarn/target/yarn-1.8.10.jar`

## Running Tools

All tools are invoked via `hadoop jar`:
```bash
hadoop jar ./fs-test-1.8.10.jar <main-class> [args...]
```

## Maven Modules

- **fs-test** — Main module. HDFS filesystem testing, benchmarking, and stress tools.
- **yarn** — YARN ResourceManager testing and Timeline Server LevelDB utilities.

## Architecture

### Key Packages (fs-test)

| Package | Role |
|---------|------|
| `com.baidu.fs.raw` | Single-operation HDFS commands using a **Command pattern**. `RawFs` dispatches to commands via `CommandFactory`. |
| `com.baidu.fs.parallel` | Multi-threaded stress/performance tests (read, write, list, mkdir, etc.) using `ExecutorService` + `CountDownLatch`. |
| `com.baidu.fs.distributed` | **MapReduce-based** distributed tests. Mapper-only jobs with time-based barrier synchronization across mappers. |
| `com.baidu.fs.test` | Standalone test scenarios (long-running dir tests, erasure coding, GSet benchmarks). |
| `com.baidu.fs.util` | Shared utilities — `Parameters` (CLI arg parser for `--key value`), `FileWriter`, `FileReader`, `CpuTimer`, etc. |
| `com.baidu.fs.router` | HDFS Router (RBF) mount table operations. |
| `org.apache.hadoop.hdfs.server.blockmanagement` | `LogBlocksPlugin` — NameNode plugin for monitoring block replication. |

### Key Packages (yarn)

| Package | Role |
|---------|------|
| `com.baidu.timeline.leveldb` | LevelDB utilities for YARN Timeline Server (count, dump, group, TTL). |
| `com.baidu.resourcemanager` | Multi-threaded YARN application report retrieval test. |

### CLI Argument Parsing

Tools use a custom `Parameters` class (`com.baidu.fs.util.Parameters`) that parses `--key value` pairs. This is used instead of Hadoop's `GenericOptionsParser`. When adding new tools, follow this pattern.

### Distributed Test Pattern

`DistributedReadTest` and `DistributedReadWriteByPercent` launch MR mapper-only jobs where each mapper runs parallel read/write operations. They use a time-based barrier (`--sleepTime`) to synchronize all mappers before starting the actual test.

## Testing

Most classes named `Test*` are **not JUnit tests** — they are standalone executable tools with `main()` methods. Actual JUnit 4 tests are minimal, located under `fs-test/src/test/java/`. Run with:

```bash
mvn test -pl fs-test
```

## Shell Scripts

- `bin/` — Monitoring scripts and distributed test orchestration (block generation at scale).
- `fs-test/src/bin/` — Convenience wrappers for running individual tools.
- `yarn/src/main/bin/` — LevelDB utility runners.

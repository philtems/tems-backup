# tems-backup - Advanced Backup Tool with Deduplication and Versioning

**Copyright (c) 2026 Philippe TEMESI**

## Overview

**tems-backup** is a powerful, feature-rich backup utility designed for efficiency and reliability. It implements content-defined deduplication, compression, and versioning to create space-efficient backups with full file history. The tool is cross-platform and works on Linux, BSD, macOS, and Windows. Unlike traditional backup tools that store complete file copies, tems-backup splits files into fixed-size chunks (default 1MB) and stores each unique chunk only once. This drastically reduces storage requirements for repeated or similar data.

## Key Features

| Feature | Description |
|---------|-------------|
| **🔁 Deduplication** | Split files into fixed-size chunks (configurable from 1KB to 16MB) and store duplicates only once. Achieves high space savings for repeated data. |
| **📦 Compression** | Support for Zstandard (zstd) and XZ compression with adjustable levels. Compression is applied per chunk. |
| **📚 Versioning** | Keep complete history of all file changes. Each backup creates new versions; previous versions remain accessible. |
| **💾 Multi-Volume Support** | Split large archives into multiple volume files (e.g., 4GB each) for easier storage on media with size limits. |
| **🔍 Integrity Checking** | Verify archive integrity by validating chunk hashes and database consistency. Optional repair capabilities. |
| **🧹 Garbage Collection** | Remove orphaned chunks (no longer referenced by any file version) to reclaim space. |
| **⏱️ Time-based Filtering** | Only backup files modified within a specified time window (e.g., `--max-age 1d` for last 24 hours). |
| **📊 Progress Reporting** | Beautiful progress bars with real-time speed, ETA, and file information. |
| **🖥️ Cross-Platform** | Full support for Unix-like systems (Linux, BSD, macOS) and Windows, including permission and metadata preservation where possible. |
| **🔐 Strong Hashing** | Choose between Blake3 (default), SHA256, or xxHash3 for chunk identification and integrity verification. |
| **📈 Statistics** | View deduplication ratios, storage efficiency, and archive composition. |

## Command Reference

### Global Options

| Option | Description |
|--------|-------------|
| `-v, -vv, -vvv` | Verbosity level (warn, info, debug, trace) |
| `-c, --config FILE` | Path to configuration file |

### create - Create a new backup archive

**Usage:** `tems-backup create <ARCHIVE> <PATHS>... [OPTIONS]`

**Options:**
| Option | Description |
|--------|-------------|
| `-c, --compression <ALGO>` | Compression algorithm: zstd, xz, none (default: zstd) |
| `-l, --compress-level <LEVEL>` | Compression level (1-19 for zstd, 1-9 for xz) (default: 3) |
| `--no-dedup` | Disable deduplication |
| `-s, --chunk-size <SIZE>` | Chunk size in bytes with suffix K, M, G (default: 1M) |
| `--hash <ALGO>` | Hash algorithm: xxhash3, blake3, sha256 (default: blake3) |
| `-V, --volume-size <SIZE>` | Volume size with suffix K, M, G (optional) |
| `-e, --exclude <PATTERN>` | Exclude patterns |
| `-i, --include <PATTERN>` | Include patterns |
| `--exclude-caches` | Exclude caches (CACHEDIR.TAG) |
| `--dry-run` | Dry run (don't write) |
| `-p, --progress` | Show progress |
| `-j, --threads <NUM>` | Number of threads |
| `--max-age <DURATION>` | Only include files modified within this duration (e.g., 1d, 12h, 30m) |

### add - Add files to an existing archive

**Usage:** `tems-backup add <ARCHIVE> <PATHS>... [OPTIONS]`

**Options:**
| Option | Description |
|--------|-------------|
| `-c, --compression <ALGO>` | Compression algorithm (override archive default) |
| `-l, --compress-level <LEVEL>` | Compression level |
| `--no-dedup` | Disable deduplication for this addition |
| `-V, --volume-size <SIZE>` | Volume size for new volumes (e.g., 1G, 500M) |
| `-e, --exclude <PATTERN>` | Exclude patterns |
| `-i, --include <PATTERN>` | Include patterns |
| `--exclude-caches` | Exclude caches |
| `--dry-run` | Dry run |
| `-p, --progress` | Show progress |
| `--max-age <DURATION>` | Only include files modified within this duration (e.g., 1d, 12h, 30m) |

### restore - Restore files from an archive

**Usage:** `tems-backup restore <ARCHIVE> [PATHS]... [OPTIONS]`

**Options:**
| Option | Description |
|--------|-------------|
| `-C, --target <DIR>` | Target directory (default: current directory) |
| `-v, --version <VERSION>` | Restore specific version |
| `--as-of <DATE>` | Restore state as of this date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS) |
| `--all-versions` | Restore all versions (with .v1, .v2 suffixes) |
| `-s, --snapshot <ID>` | Snapshot ID to restore |
| `--overwrite` | Overwrite existing files |
| `--skip-existing` | Skip existing files |
| `--backup-existing` | Backup existing files with .bak suffix |
| `-i, --interactive` | Interactive mode (ask for each conflict) |
| `--preserve-permissions` | Preserve permissions |
| `--preserve-ownership` | Preserve ownership (requires root) |
| `--preserve-times` | Preserve timestamps |
| `--strip-components <N>` | Strip N components from paths |
| `--flatten` | Flatten directory structure |
| `-p, --progress` | Show progress |
| `--dry-run` | Dry run (show what would be done) |

### list - List files in archive

**Usage:** `tems-backup list <ARCHIVE> [PATTERNS]... [OPTIONS]`

**Options:**
| Option | Description |
|--------|-------------|
| `-l, --long` | Long format (with details) |
| `--all-versions` | Show all versions (not just current) |
| `--deleted` | Show deleted files |
| `--sort-size` | Sort by size |
| `--sort-date` | Sort by date |
| `-r, --reverse` | Reverse sort |
| `--json` | Output as JSON |

### log - Show file history

**Usage:** `tems-backup log <ARCHIVE> <PATH> [OPTIONS]`

**Options:**
| Option | Description |
|--------|-------------|
| `--json` | Output as JSON |

### diff - Compare file versions

**Usage:** `tems-backup diff <ARCHIVE> <PATH> [OPTIONS]`

**Options:**
| Option | Description |
|--------|-------------|
| `--version1 <VERSION>` | First version |
| `--version2 <VERSION>` | Second version |
| `--with-local` | Compare with local file |
| `-c, --context <LINES>` | Number of context lines (default: 3) |
| `--format <FORMAT>` | Output format (text, html, json) (default: text) |

### gc - Garbage collection

**Usage:** `tems-backup gc <ARCHIVE> [OPTIONS]`

**Options:**
| Option | Description |
|--------|-------------|
| `--dry-run` | Dry run (only show what would be removed) |
| `-v, --verbose` | Verbose output |
| `-f, --force` | Force GC without confirmation |
| `-p, --progress` | Show progress |

### check - Check archive integrity

**Usage:** `tems-backup check <ARCHIVE> [OPTIONS]`

**Options:**
| Option | Description |
|--------|-------------|
| `-v, --verify` | Verify all chunks (read and hash verify) |
| `-r, --repair` | Repair if possible |
| `-v, --verbose` | Verbose output |
| `--volume <NUM>` | Check specific volume only |
| `-p, --progress` | Show progress |

### volume - Manage volumes

**Usage:** `tems-backup volume <ARCHIVE> <COMMAND> [OPTIONS]`

**Commands:**
| Command | Description |
|---------|-------------|
| `list` | List volumes |
| `add` | Add a new volume |
| `info <VOLUME>` | Show volume info |
| `verify <VOLUME>` | Verify volume |

**Options for `volume add`:**
| Option | Description |
|--------|-------------|
| `-s, --size <SIZE>` | Volume size (e.g., 4G, 2M) |

**Options for `volume verify`:**
| Option | Description |
|--------|-------------|
| `-q, --quick` | Quick check (header only) |

## Configuration

tems-backup uses a TOML configuration file. By default, it looks for `tems-backup.toml` in the current directory, `~/.config/tems-backup/config.toml`, or `~/.tems-backup.toml`.

### Example Configuration

default_compression = "zstd"
default_level = 3
default_chunk_size = "1M"
default_hash = "blake3"
temp_dir = "/tmp/tems-backup"
threads = 4

[volumes]
default_size = "4G"
min_free = "100M"
prefix = "vol"

[exclude]
patterns = [".tmp", ".log", ".git/", "node_modules/"]
caches = true
larger_than = "100M"

[advanced]
wal_autocheckpoint = 1000
cache_size_mb = 256
verify_writes = true


## Performance Tuning

### Chunk Size

- Smaller chunks (256KB-512KB): Better deduplication, more overhead
- Larger chunks (2MB-4MB): Less overhead, worse deduplication
- Default (1MB): Good balance for most workloads

### Compression Level

- zstd level 1-3: Fast compression, good for backup speed
- zstd level 10-19: Better compression, slower
- xz level 6-9: Maximum compression, very slow

### Thread Count

- Set to number of CPU cores for optimal performance
- For I/O-bound workloads, fewer threads may suffice
- Default: auto-detected CPU count

### Database Cache

- Increase `cache_size_mb` in config for large archives
- 256MB default, up to 1GB for archives with millions of files

## Platform Support

| Platform | Status | Notes |
|----------|--------|-------|
| Linux | ✅ Full | All features supported |
| macOS | ✅ Full | All features supported |
| BSD | ✅ Full | All features supported |
| Windows | ✅ Full | All features supported |

### Unix-specific Features

- File permissions preservation
- Owner/group preservation (requires root for restore)
- Symbolic link handling

### Windows-specific Features

- File attributes preservation
- Alternate data streams (future)
- ACL preservation (future)

## FAQ

### Q: How much space can I save with deduplication?

A: It depends on your data. For backups with many similar files or multiple versions, savings of 50-90% are common. For unique data, deduplication adds minimal overhead.

### Q: Can I backup across different operating systems?

A: Yes, archives are cross-platform. You can create a backup on Linux and restore on Windows, though some metadata (permissions, ownership) may not be preserved.

### Q: How do I handle very large files?

A: Files are split into chunks, so there's no practical size limit. Use appropriate chunk size (2-4MB for multi-GB files) and volume size for manageability.

### Q: Can I backup open files?

A: Currently, open files are skipped with a warning. Volume shadow copy / snapshot support is planned.

### Q: How do I migrate to a new machine?

A: Copy the entire archive directory (volumes + database) to the new machine. The paths are relative, so it will work anywhere.

### Q: What happens if a volume is lost?

A: Any chunks stored in that volume become unavailable. You can still restore files whose chunks are in remaining volumes.

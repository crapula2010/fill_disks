# fill_disks

Fill one or more removable storage targets with a random set of files from one or more source folders.

The script is designed for development on Windows and execution on Android (for example in Termux or another Python runtime).

## What it does

- Reads files from one or more source folders.
- Measures free space on each target folder.
- Excludes internal Android storage by default.
- Builds a random copy plan that fits available target space.
- Runs as dry-run by default so you can inspect before copying.

## Install

1. Create and activate your venv.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

If you use only local/mounted paths and not SMB/UNC, smbprotocol is not required at runtime.

## Use config.yaml (recommended)

Create your local config file from the sample first:

```bash
cp config.sample.yaml config.yaml
```

Then edit values in config.yaml:

- sources
- targets
- options
- smb credentials (or use SMB_USERNAME / SMB_PASSWORD / SMB_DOMAIN environment variables)

Run a dry-run plan:

```bash
python fill_disks.py
```

Execute copy:

```bash
python fill_disks.py --execute
```

Notes:

- The script auto-loads config.yaml (or config.yml) from the current folder.
- CLI flags override config values.

## Find correct Android target paths

UI labels like "/SD Card" are often not real filesystem paths. Use this command on Android to find actual mount paths:

```bash
python fill_disks.py --list-targets
```

Typical real paths look like:

- /storage/1234-5678
- /storage/ABCD-EF12

## Config File Example

The project includes a starter template at config.sample.yaml.

```yaml
sources:
  - "\\\\192.168.68.57\\pauli\\envryone\\music"
targets:
  - "/storage/1234-5678"
  - "/storage/ABCD-EF12"
options:
  reserve_mb: 256
  auto_detect_targets: true
  execute: false
  plan_output: "plan.jsonl"
```

Keep real credentials in your local config.yaml. The repository ignores config.yaml by default.

## Direct CLI Example (No Config)

```bash
python fill_disks.py \
  --source "\\\\192.168.68.57\\pauli\\envryone\\music" \
  --target "/storage/1234-5678" \
  --target "/storage/ABCD-EF12" \
  --smb-username "your_user" \
  --smb-password "your_password" \
  --plan-output plan.jsonl
```

## Useful options

- --reserve-mb 256
  - Keep this much free space on each target.
- --seed 12345
  - Repeatable random selection.
- --auto-detect-targets
  - Adds detected external Android mounts to your explicit target list.
- --allow-internal
  - Allows internal storage targets (disabled by default).
- --max-files 1000
  - Optional cap for planned file count.

## Notes

- Destination layout is target/source_alias/relative/path/to/file.
- If a destination file already exists, it is skipped when size matches.
- If a destination name conflicts and sizes differ, a numbered filename is used unless --overwrite is set.

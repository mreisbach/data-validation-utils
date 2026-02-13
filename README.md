# data-validation-utils

Batch validation tooling for encrypted off-site compute jobs.

## Supported Job Types

1. `backtest` (default): validates strategy/series chunks.
2. `log_metrics`: computes per-bot genetics metrics from pre-parsed log telemetry.

## Usage

```bash
python validate.py input.dat [chunk_id]
```

`input.dat` is an encrypted payload produced by `vigil.ci.farm.encrypt_payload`.

## GitHub Actions Batch

The `batch.yml` workflow fans out matrix workers, each running `validate.py` on one chunk and uploading encrypted `output.dat` artifacts.

## Requirements

See `requirements.txt`.

# GCS lifecycle policies

The checked-in policies expire only short-lived maintenance namespaces. Staging
deletes live `_temporary/` objects after seven days; production deletes live
`_rollback/` objects after seven days. Neither policy expires live canonical
dataset or metadata objects, and neither policy deletes noncurrent generations.

Bucket Object Versioning must be enabled (and is enabled by the IAC) so replacing
a live object creates a recoverable prior generation. The maintenance asset is
safe to re-run: it overwrites working objects while retaining prior generations
for rollback.

From the repository root, review the current bucket lifecycle configuration and
the JSON file before applying either policy. These commands **replace the entire
existing lifecycle configuration**; they do not merge rules.

```bash
gcloud storage buckets update "gs://${HIFLD_STAGING_BUCKET}" --lifecycle-file=ops/gcs-lifecycle/staging.json
gcloud storage buckets update "gs://${HIFLD_DATASETS_BUCKET}" --lifecycle-file=ops/gcs-lifecycle/production.json
```

Do not apply the production policy to staging or the staging policy to
production. Confirm the bucket environment variables resolve to the intended
buckets before running either command.

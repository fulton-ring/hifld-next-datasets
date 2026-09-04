# GCS lifecycle policies

The checked-in policies expire only short-lived maintenance namespaces. Staging
deletes `_temporary/` objects after seven days; production deletes `_rollback/`
objects after seven days. Neither policy expires canonical dataset or metadata
objects.

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

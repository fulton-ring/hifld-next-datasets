"""Manual two-state public-center roster acquisition and separate Census result candidates."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.childcare_roster_pilot import (
    DATASET, FILES, GATES, acquire_roster, geocode_roster,
)


class ChildcarePilotConfig(Config):
    capture_directory: str
    # Explicit bounded pilot limit; remaining eligible records stay deferred.
    max_geocoding_addresses: int = 10


def _run(context, config, storage, state):
    roster=acquire_roster(storage,state,config.capture_directory)
    result=geocode_roster(storage,roster,config.capture_directory,config.max_geocoding_addresses)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET,FILES[state],result['version'])])
    context.add_output_metadata(result)
    return result


@asset(key=AssetKey(['ingest',DATASET,FILES['ny']]),group_name='childcare_pilot',compute_kind='http')
def new_york_childcare_census_results(context,config: ChildcarePilotConfig,
                                     staging_storage: StagingStorageResource) -> dict:
    return _run(context,config,staging_storage,'ny')


@asset(key=AssetKey(['ingest',DATASET,FILES['wa']]),group_name='childcare_pilot',compute_kind='http')
def washington_childcare_census_results(context,config: ChildcarePilotConfig,
                                       staging_storage: StagingStorageResource) -> dict:
    return _run(context,config,staging_storage,'wa')


def pilot_complete(candidate: dict) -> bool:
    counts=candidate.get('outcomes',{})
    return (isinstance(counts,dict) and bool(counts)
            and all(type(v) is int and v>=0 for v in counts.values())
            and candidate.get('feature_count',0)>0
            and counts.get('Match')==candidate.get('feature_count')
            and candidate.get('roster_count',0)>0
            and sum(counts.values())==candidate.get('roster_count')==candidate.get('outcome_count'))


@asset_check(asset=new_york_childcare_census_results,blocking=True)
def new_york_childcare_census_accounting(candidate: dict) -> AssetCheckResult:
    return AssetCheckResult(passed=pilot_complete(candidate),metadata=GATES,
        description='Technical complete-row outcome accounting only; matched points are partial results, all acceptance gates closed.')


@asset_check(asset=washington_childcare_census_results,blocking=True)
def washington_childcare_census_accounting(candidate: dict) -> AssetCheckResult:
    return AssetCheckResult(passed=pilot_complete(candidate),metadata=GATES,
        description='Technical complete-row outcome accounting only; matched points are partial results, all acceptance gates closed.')


childcare_pilot_job=define_asset_job('childcare_two_state_geocoding_pilot',selection=AssetSelection.assets(
    new_york_childcare_census_results,washington_childcare_census_results))
INGEST_ASSETS=[new_york_childcare_census_results,washington_childcare_census_results]
ASSET_CHECKS=[new_york_childcare_census_accounting,washington_childcare_census_accounting]
JOBS=[childcare_pilot_job]
SCHEDULES=[]

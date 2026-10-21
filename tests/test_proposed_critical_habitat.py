from dagster import DefaultScheduleStatus
from shapely.geometry import MultiPolygon

from dagster_hifld.assets.proposed_critical_habitat import proposed_critical_habitat_draft_schedule
from dagster_hifld.sources.proposed_critical_habitat import (
    ITEM_ID,
    LAYER_ID,
    LAYER_NAME,
    _esri_polygon,
    _ring_area,
    _receipt_timestamp,
)


def test_proposed_layer_is_explicit_and_stopped():
    assert ITEM_ID == "794de45b9d774d21aed3bf9b5313ee24"
    assert LAYER_ID == 2
    assert LAYER_NAME == "Proposed Critical Habitat Features"
    assert proposed_critical_habitat_draft_schedule.default_status == DefaultScheduleStatus.STOPPED


def test_esri_polygon_preserves_shell_hole_and_nested_island():
    # Esri clockwise shells have negative signed area; counterclockwise rings
    # are holes. The inner island must remain a separate polygon component.
    outer = [[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]]
    hole = [[2, 2], [8, 2], [8, 8], [2, 8], [2, 2]]
    island = [[3, 3], [3, 4], [4, 4], [4, 3], [3, 3]]
    assert _ring_area(outer) < 0
    assert _ring_area(hole) > 0
    assert _ring_area(island) < 0

    geometry = _esri_polygon({"rings": [outer, hole, island]})

    assert isinstance(geometry, MultiPolygon)
    assert len(geometry.geoms) == 2
    assert geometry.is_valid
    assert sum(len(poly.interiors) for poly in geometry.geoms) == 1
    assert geometry.area == 65


def test_native_population_check_rejects_duplicate_ids_and_invalid_geometry():
    from dagster_hifld.assets.proposed_critical_habitat import proposed_critical_habitat_complete

    complete = {"feature_count": 70, "source_unique_id_count": 70,
                "source_null_geometry_count": 0, "source_invalid_geometry_count": 0}
    assert proposed_critical_habitat_complete(complete).passed
    assert not proposed_critical_habitat_complete({**complete, "source_unique_id_count": 69}).passed
    assert not proposed_critical_habitat_complete({**complete, "source_invalid_geometry_count": 1}).passed


def test_http_date_is_normalized_without_losing_original_observation():
    normalized, original = _receipt_timestamp("Wed, 21 Oct 2026 15:51:00 GMT")
    assert normalized == "2026-10-21T15:51:00+00:00"
    assert original == "Wed, 21 Oct 2026 15:51:00 GMT"

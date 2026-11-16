"""Declarative table of the FEMA NFHL MapServer layers built by the family.

Each entry names one layer (or table) of FEMA's ``public/NFHL`` MapServer and
the HIFLD Open dataset it rebuilds. The slug is both the dataset and file slug,
so asset keys stay ``ingest/<slug>/<slug>`` and publish partitions stay
``<slug>/<slug>/<version>``. Completeness, schema and identity are checked
against the producer's live documents at capture time; nothing here pins a
population count, roster or digest.
"""
from __future__ import annotations

from dataclasses import dataclass

GEOMETRY_KINDS = {"point": "esriGeometryPoint", "line": "esriGeometryPolyline",
                  "polygon": "esriGeometryPolygon", "none": None}
MIB = 1024 * 1024


@dataclass(frozen=True)
class NfhlLayer:
    slug: str
    layer_id: int
    name: str
    title: str
    geometry: str
    id_fields: tuple[str, ...]
    inventory_id: str
    datalumos_project: int
    subject: str = ""
    id_note: str = ""
    # Initial OBJECTID-window target; ``None`` uses the layer's live maxRecordCount.
    page_records: int | None = None
    max_page_bytes: int = 64 * MIB
    min_free_bytes: int = 1_500_000_000
    # Also capture and check FEMA's ArcGIS Online registration of the service.
    portal_identity: bool = False

    def __post_init__(self) -> None:
        if self.geometry not in GEOMETRY_KINDS:
            raise ValueError(f"unknown_nfhl_geometry_kind:{self.geometry}")

    @property
    def stem(self) -> str:
        return self.slug.replace("-", "_")

    @property
    def is_table(self) -> bool:
        return self.geometry == "none"

    @property
    def esri_geometry_type(self) -> str | None:
        return GEOMETRY_KINDS[self.geometry]


_STANDARD = ("GFID", "GlobalID")


def _ids(*fields: str) -> tuple[str, ...]:
    return ("OBJECTID", *fields, *_STANDARD)


LAYERS: tuple[NfhlLayer, ...] = (
    NfhlLayer("nfhl-availability", 0, "NFHL Availability", "NFHL Availability (FEMA NFHL)", "polygon",
              ("OBJECTID", "STUDY_ID", "GlobalID"), "nfhl/nfhl-availability", 239980,
              subject="study-availability polygons",
              id_note="STUDY_ID is not unique in the producer population and is preserved as-is.",
              page_records=500, min_free_bytes=4_000_000_000),
    NfhlLayer("nfhl-lomrs", 1, "LOMRs", "LOMRs (FEMA NFHL)", "polygon",
              _ids("LOMR_ID", "CASE_NO"), "nfhl/lomrs", 239979, subject="Letters of Map Revision"),
    NfhlLayer("nfhl-firm-panels", 3, "FIRM Panels", "FIRM Panels (FEMA NFHL)", "polygon",
              _ids("DFIRM_ID", "VERSION_ID", "FIRM_ID", "ST_FIPS", "PCOMM", "PANEL", "SUFFIX", "FIRM_PAN",
                   "SOURCE_CIT"), "nfhl/firm-panels-1", 239969, subject="FIRM panel polygons",
              min_free_bytes=4_000_000_000),
    NfhlLayer("nfhl-base-index", 4, "Base Index", "Base Index (FEMA NFHL)", "polygon",
              _ids("BASE_ID"), "nfhl/base-index", 239790,
              subject="base map index polygons (the imagery or source sheets behind a DFIRM study)",
              page_records=1000, min_free_bytes=6_000_000_000),
    NfhlLayer("nfhl-plss", 5, "PLSS", "PLSS (FEMA NFHL)", "polygon",
              _ids("DFIRM_ID", "VERSION_ID", "PLSS_AR_ID"), "nfhl/plss", 239982,
              subject="the Public Land Survey System base map", min_free_bytes=10_000_000_000),
    NfhlLayer("nfhl-topographic-low-confidence-areas", 6, "Topographic Low Confidence Areas",
              "Topographic Low Confidence Areas (FEMA NFHL)", "polygon",
              _ids("DFIRM_ID", "VERSION_ID", "LOWCONF_ID", "DATESTAMP", "CONF_TYPE", "SOURCE_CIT"),
              "nfhl/topographic-low-confidence-areas-1", 240885,
              subject="S_HP_HAZ polygons where terrain cannot support reliable flood elevations",
              max_page_bytes=256 * MIB, min_free_bytes=4_000_000_000),
    NfhlLayer("nfhl-river-mile-markers", 7, "River Mile Markers", "River Mile Markers (FEMA NFHL)", "point",
              _ids("RIV_MRK_ID", "START_ID", "SOURCE_CIT"), "nfhl/river-mile-markers", 239986,
              id_note="RIV_MRK_ID, START_ID and GFID are reused across producer rows; only OBJECTID and "
                      "GlobalID are unique."),
    NfhlLayer("nfhl-coastal-gages", 9, "Coastal Gages", "Coastal Gages (FEMA NFHL)", "point",
              _ids("CSTGAGE_ID"), "nfhl/coastal-gages-1", 239797),
    NfhlLayer("nfhl-gages", 10, "Gages", "Gages (FEMA NFHL)", "point",
              _ids("CSTGAGE_ID"), "nfhl/gages-2", 239972),
    NfhlLayer("nfhl-nodes", 11, "Nodes", "Nodes (FEMA NFHL)", "point",
              _ids("NODE_ID"), "nfhl/nodes", 239981),
    NfhlLayer("nfhl-high-water-marks", 12, "High Water Marks", "High Water Marks (FEMA NFHL)", "point",
              _ids("HWM_ID"), "nfhl/high-water-marks-1", 239974),
    NfhlLayer("nfhl-station-start-points", 13, "Station Start Points", "Station Start Points (FEMA NFHL)",
              "point", _ids("START_ID"), "nfhl/station-start-points", 239988),
    NfhlLayer("nfhl-cross-sections", 14, "Cross-Sections", "Cross-Sections (FEMA NFHL)", "line",
              _ids("DFIRM_ID", "VERSION_ID", "XS_LN_ID", "START_ID", "MODEL_ID"), "nfhl/cross-sections-1",
              239967, min_free_bytes=12_000_000_000),
    NfhlLayer("nfhl-coastal-transects", 15, "Coastal Transects", "Coastal Transects (FEMA NFHL)", "line",
              _ids("TRAN_LN_ID", "TBASELN_ID", "CST_MDL_ID"), "nfhl/coastal-transects", 239795),
    NfhlLayer("nfhl-base-flood-elevations", 16, "Base Flood Elevations", "Base Flood Elevations (FEMA NFHL)",
              "line", _ids("BFE_LN_ID"), "nfhl/base-flood-elevations-1", 239788,
              min_free_bytes=12_000_000_000),
    NfhlLayer("nfhl-transect-baselines", 18, "Transect Baselines", "Transect Baselines (FEMA NFHL)", "line",
              _ids("TBASELN_ID", "CST_MDL_ID"), "nfhl/transect-baselines", 239991,
              min_free_bytes=4_000_000_000),
    NfhlLayer("nfhl-limit-moderate-wave-action", 19, "Limit of Moderate Wave Action",
              "Limit of Moderate Wave Action (FEMA NFHL)", "line", _ids("LIMWA_ID"),
              "nfhl/limit-of-moderate-wave-action-1", 239977),
    NfhlLayer("nfhl-political-jurisdictions", 22, "Political Jurisdictions",
              "Political Jurisdictions (FEMA NFHL)", "polygon", _ids("POL_AR_ID"),
              "nfhl/political-jurisdictions-1", 239983, page_records=1000),
    NfhlLayer("nfhl-levees", 23, "Levees", "Levees (FEMA NFHL)", "line",
              _ids("LEVEE_ID", "FC_SYS_ID", "FC_SEG_ID"), "nfhl/levees-1", 239976),
    NfhlLayer("nfhl-general-structures", 24, "General Structures", "General Structures (FEMA NFHL)", "line",
              _ids("STRUCT_ID", "STRUCT_TYP", "CST_STRUCT", "STRUCT_NM"), "nfhl/general-structures", 239973),
    NfhlLayer("nfhl-primary-frontal-dunes", 25, "Primary Frontal Dunes", "Primary Frontal Dunes (FEMA NFHL)",
              "line", _ids("DFIRM_ID", "VERSION_ID", "PFD_ID", "VZONE_LIMT", "SOURCE_CIT"),
              "nfhl/primary-frontal-dunes", 239984),
    NfhlLayer("nfhl-hydrologic-reaches", 26, "Hydrologic Reaches", "Hydrologic Reaches (FEMA NFHL)", "line",
              _ids("REACH_ID", "UP_NODE", "DN_NODE"), "nfhl/hydrologic-reaches-1", 239975,
              min_free_bytes=12_000_000_000),
    NfhlLayer("nfhl-seclusion-boundaries", 29, "Seclusion Boundaries", "Seclusion Boundaries (FEMA NFHL)",
              "polygon", _ids("SUBINFO_ID", "CASE_NO"), "nfhl/seclusion-boundaries", 239987),
    NfhlLayer("nfhl-alluvial-fans", 30, "Alluvial Fans", "Alluvial Fans (FEMA NFHL)", "polygon",
              _ids("ALLUVL_ID"), "nfhl/alluvial-fans", 239786),
    NfhlLayer("nfhl-subbasins", 31, "Subbasins", "Subbasins (FEMA NFHL)", "polygon",
              _ids("SUBBAS_ID"), "nfhl/subbasins", 239990, page_records=100,
              max_page_bytes=512 * MIB, min_free_bytes=64_000_000_000),
    NfhlLayer("nfhl-water-areas", 32, "Water Areas", "Water Areas (FEMA NFHL)", "polygon",
              _ids("DFIRM_ID", "VERSION_ID", "WTR_AR_ID", "WTR_NM", "SHOWN_FIRM", "SHOWN_INDX", "SOURCE_CIT"),
              "nfhl/water-areas-1", 240887, subject="S_Wtr_Area hydrographic base-map polygons shown on FIRMs",
              max_page_bytes=256 * MIB, min_free_bytes=4_000_000_000),
    NfhlLayer("nfhl-study-info", 41, "Study_Info", "Study Info (FEMA NFHL)", "none",
              _ids(), "nfhl/study-info", 239989, subject="one FIRM Database study record per row",
              portal_identity=True),
)

LAYERS_BY_SLUG = {layer.slug: layer for layer in LAYERS}
if len(LAYERS_BY_SLUG) != len(LAYERS) or len({layer.layer_id for layer in LAYERS}) != len(LAYERS):
    raise ValueError("duplicate_nfhl_layer_entry")

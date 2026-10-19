"""Fail closed on raw/published schema drift before a candidate is promoted.

Schemas are dictionaries keyed by column name, with type/nullable/meaning/id_role
metadata. Producers may provide extra metadata, but comparisons use the entire
approved dictionary so changed semantics cannot disappear during normalization.
"""
from __future__ import annotations


def schema_diff(expected: dict, observed: dict) -> dict:
    """Report additions, removals and metadata changes; never mutate the baseline."""
    return {
        'added': sorted(observed.keys() - expected.keys()),
        'removed': sorted(expected.keys() - observed.keys()),
        'changed': {name: {'before': expected[name], 'after': observed[name]}
                    for name in sorted(expected.keys() & observed.keys())
                    if expected[name] != observed[name]},
    }


def require_compatible(contract: dict, raw_schema: dict, published_schema: dict,
                       id_checks: dict, geometry: dict, coverage: object) -> None:
    """Call before promotion. Unknown fields and ID failures quarantine the release.

    id_checks must report verified values_preserved, unique and non_null for each
    identifier column in the contract. Source-specific code computes these checks
    without coercing identifiers; a new producer ID also changes raw_schema.
    """
    for scope, observed in [('raw_columns', raw_schema), ('published_columns', published_schema)]:
        diff = schema_diff(contract[scope], observed)
        if any(diff.values()):
            raise ValueError(f'Potential breaking change in {scope}: {diff}')
    for scope in ('raw_columns', 'published_columns'):
        for name, spec in contract[scope].items():
            if spec.get('id_role'):
                checks = id_checks.get(name, {})
                required = ['values_preserved']
                if spec.get('unique'):
                    required.append('unique')
                if not spec.get('nullable', True):
                    required.append('non_null')
                if not all(checks.get(key) is True for key in required):
                    raise ValueError(f'Potential breaking change to identifier {name}')
    if geometry != contract['geometry'] or coverage != contract['coverage']:
        raise ValueError('Potential breaking change to geometry or coverage')

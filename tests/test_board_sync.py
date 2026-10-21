import importlib.util
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location('board_sync', Path(__file__).resolve().parents[1] / 'tools/board_sync.py')
sync = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sync)


class BoardSyncTests(unittest.TestCase):
    def test_only_supported_moves_are_consumed_with_decision_first(self):
        self.assertEqual(sync.requested_move(['bug', 'move:done']), (None, []))
        self.assertEqual(sync.requested_move(['move:ready', 'move:review', 'move:needs-decision', 'bug']),
                         ('Needs decision', ['move:needs-decision', 'move:review', 'move:ready']))
        for label, status in sync.MOVE_LABELS.items():
            self.assertEqual(sync.requested_move([label]), (status, [label]))

    def test_failed_board_update_preserves_request_labels(self):
        calls = []
        def graphql(query, variables):
            calls.append('move')
            raise RuntimeError('Project permission denied')
        with self.assertRaises(RuntimeError):
            sync.apply_move(graphql, lambda *args: calls.append('remove'), 'P', 'F', {'Review': 'R'},
                            {'id': 'I', 'issue_id': 'ISSUE', 'number': 84}, {'move:review': 'LR'}, 'owner/repo')
        self.assertEqual(calls, ['move'])

    def test_success_consumes_all_supported_requests_after_move(self):
        calls = []
        def graphql(query, variables):
            calls.append(('move', variables['value']))
        sync.apply_move(graphql, lambda issue, labels: calls.append(('remove', issue, labels)), 'P', 'F', {'Review': 'R'},
                        {'id': 'I', 'issue_id': 'ISSUE', 'number': 84}, {'move:ready': 'LY', 'move:review': 'LR', 'bug': 'LB'}, 'owner/repo')
        self.assertEqual(calls, [('move', 'R'), ('remove', 'ISSUE', ['LR', 'LY'])])

    def test_dry_run_and_missing_lane_do_not_consume_labels(self):
        def unexpected(*args):
            self.fail('No request should be made')
        sync.apply_move(unexpected, unexpected, 'P', 'F', {'Ready': 'R'},
                        {'id': 'I', 'issue_id': 'ISSUE', 'number': 84}, {'move:ready': 'LY'}, 'owner/repo', dry_run=True)
        with self.assertRaises(ValueError):
            sync.apply_move(unexpected, unexpected, 'P', 'F', {},
                            {'id': 'I', 'issue_id': 'ISSUE', 'number': 84}, {'move:ready': 'LY'}, 'owner/repo')

    def test_paginated_sync_ignores_other_repositories(self):
        project = {'id': 'P', 'status': {'id': 'F', 'options': [{'name': 'Review', 'id': 'R'}]}}
        def item(repo, number):
            return {'id': 'I', 'content': {'id': 'ISSUE', 'number': number,
                    'repository': {'nameWithOwner': repo},
                    'labels': {'totalCount': 1, 'nodes': [{'id': 'LR', 'name': 'move:review'}]}}}
        pages = iter([
            {'nodes': [item('other/repo', 84), {'id': 'DRAFT', 'content': {}}],
             'pageInfo': {'hasNextPage': True, 'endCursor': 'next'}},
            {'nodes': [item('owner/repo', 84)], 'pageInfo': {'hasNextPage': False, 'endCursor': None}},
        ])
        writes = []
        def graphql(query, variables):
            if query == sync.FIELDS:
                return {'organization': {'projectV2': project}}
            if query == sync.ITEMS:
                return {'node': {'items': next(pages)}}
            writes.append(variables)
        removed = []
        sync.synchronize(graphql, lambda *args: removed.append(args), 'owner', 2, 'owner/repo', issue=84)
        self.assertEqual(len(writes), 1)
        self.assertEqual(removed, [('ISSUE', ['LR'])])

    def test_audit_bound_and_missing_card_preserve_requests(self):
        project = {'id': 'P', 'status': {'id': 'F', 'options': []}}
        calls = []
        def graphql(query, variables):
            calls.append(query)
            if query == sync.FIELDS:
                return {'organization': {'projectV2': project}}
            return {'node': {'items': {'nodes': [], 'pageInfo': {'hasNextPage': True, 'endCursor': 'next'}}}}
        with self.assertRaisesRegex(ValueError, 'audit bound'):
            sync.synchronize(graphql, lambda *args: self.fail('No removal'), 'owner', 2, 'owner/repo')
        self.assertEqual(len(calls), 11)

    def test_one_bad_card_does_not_starve_other_moves(self):
        project = {'id': 'P', 'status': {'id': 'F', 'options': [{'name': 'Review', 'id': 'R'}]}}
        def item(number, count):
            return {'id': str(number), 'content': {'id': str(number), 'number': number,
                    'repository': {'nameWithOwner': 'owner/repo'},
                    'labels': {'totalCount': count, 'nodes': [{'id': 'L', 'name': 'move:review'}]}}}
        writes = []
        def graphql(query, variables):
            if query == sync.FIELDS:
                return {'organization': {'projectV2': project}}
            if query == sync.ITEMS:
                return {'node': {'items': {'nodes': [item(1, 101), item(2, 1)],
                        'pageInfo': {'hasNextPage': False, 'endCursor': None}}}}
            writes.append(variables['item'])
        removed = []
        with self.assertRaisesRegex(RuntimeError, '1 card'):
            sync.synchronize(graphql, lambda issue, labels: removed.append(issue), 'owner', 2, 'owner/repo')
        self.assertEqual(writes, ['2'])
        self.assertEqual(removed, ['2'])


if __name__ == '__main__':
    unittest.main()

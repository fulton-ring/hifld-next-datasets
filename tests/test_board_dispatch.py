import importlib.util
from datetime import datetime, timezone
from pathlib import Path
import sys
import re
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
SPEC = importlib.util.spec_from_file_location('board_dispatch', Path(__file__).resolve().parents[1] / 'tools/board_dispatch.py')
dispatch = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dispatch)

NOW = datetime(2026, 10, 21, 5, tzinfo=timezone.utc).timestamp()
SHA = 'a' * 40


def run(number=1, role='review', issue=282, sha=SHA, status='completed', conclusion='success'):
    return dict(id=number, display_title=f'{role} issue #{issue} @ {sha}', status=status,
                conclusion=conclusion, updated_at='2026-10-21T04:00:00Z', run_attempt=1)


def card(issue=282, status='Review'):
    return dict(id=f'ITEM{issue}', number=issue, status=status)


class FakeAPI:
    def __init__(self, runs=None, pr=True):
        self.runs = runs or []
        self.pr = pr
        self.posts = []
        self.current_status = None
        self.head = SHA

    def __call__(self, kind, path, data=None):
        if kind == 'board':
            return {'data': {'node': {'status': {'name': self.current_status or 'Review'}, 'content': {
                'number': int(data['variables']['item'].removeprefix('ITEM')), 'state': 'OPEN',
                'repository': {'nameWithOwner': dispatch.REPO}, 'labels': {'totalCount': 0, 'nodes': []}}}}}
        if kind == 'control' and '/runs?' in path:
            return {'workflow_runs': self.runs}
        if kind == 'control' and path.endswith('/dispatches'):
            self.posts.append(data)
            return {'workflow_run_id': 100 + len(self.posts), 'html_url': 'https://github.com/fulton-ring/hifld-next-iac/actions/runs/100'}
        if '/comments?' in path:
            number = int(re.search(r'/issues/(\d+)/comments', path)[1])
            return ([{'body': f'<!-- hifld-agent-pr:v1 issue={number} pr=548 -->', 'user': {'login': 'maintainer'}}] if self.pr else [])
        if '/collaborators/' in path:
            return {'permission': 'write'}
        if '/timeline?' in path:
            return ([{'event': 'cross-referenced', 'source': {'issue': {
                'pull_request': {'url': f'https://api.github.com/repos/{dispatch.REPO}/pulls/548'}}}}] if self.pr else [])
        if '/pulls/' in path:
            return dict(number=548, state='open', head={'sha': self.head, 'ref': 'codex/cloud-dataset-282', 'repo': {'full_name': dispatch.REPO}},
                        base={'ref': 'codex/cloud-dataset-271', 'repo': {'full_name': dispatch.REPO}})
        if '/commits/' in path:
            return {'sha': SHA}
        raise AssertionError((kind, path, data))


class BoardDispatchTests(unittest.TestCase):
    def test_review_slots_are_independent_of_six_busy_workers(self):
        busy = [run(n, role='implementation', issue=900+n, status='in_progress') for n in range(6)]
        api = FakeAPI(busy)
        self.assertEqual(len(dispatch.dispatch_cards([card()], api, now=NOW)), 1)
        busy += [run(10+n, issue=950+n, status='queued') for n in range(2)]
        api = FakeAPI(busy)
        self.assertEqual(dispatch.dispatch_cards([card()], api, now=NOW), [])

    def test_full_review_pool_does_not_block_ready_implementations(self):
        api = FakeAPI([run(n, issue=900+n, status='in_progress') for n in range(2)], pr=False)
        api.current_status = 'Ready'
        selected = dispatch.dispatch_cards([card(), card(363, 'Ready')], api, now=NOW)
        self.assertEqual([(x['role'], x['issue']) for x in selected], [('implementation', '363')])

    def test_review_uses_exact_head_and_existing_base(self):
        api = FakeAPI()
        result = dispatch.dispatch_cards([card()], api, now=NOW)
        self.assertEqual(len(result), 1)
        inputs = api.posts[0]['inputs']
        self.assertEqual(api.posts[0]['ref'], 'main')
        self.assertEqual(inputs['role'], 'review')
        self.assertEqual(inputs['datasets_ref'], SHA)
        self.assertEqual(inputs['dispatch_head_sha'], SHA)
        self.assertEqual(inputs['pr_base'], 'codex/cloud-dataset-271')
        self.assertEqual(inputs['pull_request_number'], '548')

    def test_terminal_failure_and_success_are_both_deduplicated(self):
        for conclusion in ('success', 'failure', 'cancelled'):
            api = FakeAPI([run(conclusion=conclusion)])
            self.assertEqual(dispatch.dispatch_cards([card()], api, now=NOW), [])
            self.assertEqual(api.posts, [])

    def test_new_head_is_eligible_but_active_issue_is_not(self):
        api = FakeAPI([run(sha='b' * 40)])
        self.assertEqual(len(dispatch.dispatch_cards([card()], api, now=NOW)), 1)
        api = FakeAPI([run(sha='b' * 40, status='in_progress')])
        self.assertEqual(dispatch.dispatch_cards([card()], api, now=NOW), [])

    def test_ready_uses_existing_branch_and_parked_lanes_never_dispatch(self):
        api = FakeAPI()
        api.current_status = 'Ready'
        self.assertEqual(len(dispatch.dispatch_cards([card(status='Ready')], api, now=NOW)), 1)
        self.assertEqual(api.posts[0]['inputs']['datasets_ref'], 'codex/cloud-dataset-282')
        api = FakeAPI()
        self.assertEqual(dispatch.dispatch_cards([card(status=x) for x in ('Backlog', 'Needs decision', 'Done', 'In progress')], api, now=NOW), [])

    def test_new_ready_work_uses_shared_candidate_and_review_requires_pr(self):
        api = FakeAPI(pr=False)
        api.current_status = 'Ready'
        self.assertEqual(len(dispatch.dispatch_cards([card(status='Ready')], api, now=NOW)), 1)
        self.assertEqual(api.posts[0]['inputs']['datasets_ref'], 'codex/cloud-verification-417')
        api = FakeAPI(pr=False)
        self.assertEqual(dispatch.dispatch_cards([card()], api, now=NOW), [])

    def test_review_reservations_bound_delayed_github_ledger(self):
        api = FakeAPI()
        self.assertEqual(len(dispatch.dispatch_cards([card(x) for x in range(1, 100)], api, now=NOW)), 2)
        self.assertEqual(len(api.posts), 2)

    def test_lane_change_before_dispatch_and_dry_run_never_post(self):
        api = FakeAPI()
        api.current_status = 'Needs decision'
        self.assertEqual(dispatch.dispatch_cards([card()], api, now=NOW), [])
        api = FakeAPI()
        self.assertEqual(len(dispatch.dispatch_cards([card()], api, now=NOW, dry_run=True)), 1)
        self.assertEqual(api.posts, [])

    def test_completed_attempts_do_not_consume_concurrency(self):
        api = FakeAPI([run(n, issue=900+n) for n in range(99)])
        self.assertEqual(len(dispatch.dispatch_cards([card()], api, now=NOW)), 1)

    def test_six_implementation_reservations_bound_delayed_ledger(self):
        api = FakeAPI(pr=False)
        api.current_status = 'Ready'
        selected = dispatch.dispatch_cards([card(n, 'Ready') for n in range(1, 20)], api, now=NOW)
        self.assertEqual(len(selected), 6)
        self.assertTrue(all(x['role'] == 'implementation' for x in selected))

    def test_worker_slots_remain_full_when_both_review_slots_are_free(self):
        api = FakeAPI([run(n, role='implementation', issue=900+n, status='queued') for n in range(6)], pr=False)
        api.current_status = 'Ready'
        self.assertEqual(dispatch.dispatch_cards([card(363, 'Ready')], api, now=NOW), [])

    def test_fork_pr_cannot_be_sent_to_review(self):
        class ForkAPI(FakeAPI):
            def __call__(self, kind, path, data=None):
                result = super().__call__(kind, path, data)
                if '/pulls/' in path:
                    result['head']['repo']['full_name'] = 'stranger/fork'
                return result
        api = ForkAPI()
        self.assertEqual(dispatch.dispatch_cards([card()], api, now=NOW), [])
        self.assertEqual(api.posts, [])

    def test_stale_head_and_pending_move_prevent_dispatch(self):
        class StaleAPI(FakeAPI):
            reads = 0
            def __call__(self, kind, path, data=None):
                result = super().__call__(kind, path, data)
                if '/pulls/' in path:
                    self.reads += 1
                    if self.reads > 1:
                        result['head']['sha'] = 'b' * 40
                return result
        api = StaleAPI()
        self.assertEqual(dispatch.dispatch_cards([card()], api, now=NOW), [])
        class PendingAPI(FakeAPI):
            def __call__(self, kind, path, data=None):
                result = super().__call__(kind, path, data)
                if kind == 'board':
                    result['data']['node']['content']['labels'] = {'totalCount': 1, 'nodes': [{'name': 'move:needs-decision'}]}
                return result
        api = PendingAPI()
        self.assertEqual(dispatch.dispatch_cards([card()], api, now=NOW), [])

    def test_uncertain_dispatch_response_stops_without_retry(self):
        class NoAckAPI(FakeAPI):
            def __call__(self, kind, path, data=None):
                result = super().__call__(kind, path, data)
                return {} if path.endswith('/dispatches') else result
        api = NoAckAPI()
        with self.assertRaises(KeyError):
            dispatch.dispatch_cards([card(1), card(2)], api, now=NOW)
        self.assertEqual(len(api.posts), 1)

    def test_unknown_active_legacy_run_fails_closed(self):
        legacy = run(status='in_progress')
        legacy['display_title'] = 'GKE agent pilot'
        self.assertEqual(dispatch.capacity([legacy], 'review'), 0)

    def test_board_query_admits_shared_gate_only_for_independent_review(self):
        def item(number, body='<!-- hifld-dataset:example -->', state='OPEN', repo=dispatch.REPO, lane='Review'):
            return {'id': str(number), 'status': {'name': lane}, 'content': {
                'number': number, 'body': body, 'state': state, 'repository': {'nameWithOwner': repo},
                'labels': {'totalCount': 0, 'nodes': []}}}
        def api(kind, path, data=None):
            if data['query'] == dispatch.FIELDS:
                return {'data': {'organization': {'projectV2': {'id': 'P'}}}}
            return {'data': {'node': {'items': {'pageInfo': {'hasNextPage': False}, 'nodes': [
                item(282), item(417, body='Shared acceptance gate'), item(417, body='Shared gate', lane='Ready'), item(500, body='Unrelated issue'), item(3, state='CLOSED'), item(4, repo='stranger/fork')]}}}}
        self.assertEqual([c['number'] for c in dispatch.cards(api)], [282, 417])

    def test_public_mention_is_not_a_trusted_pr_association(self):
        class MentionAPI(FakeAPI):
            def __call__(self, kind, path, data=None):
                result = super().__call__(kind, path, data)
                if '/collaborators/' in path:
                    return {'permission': 'read'}
                return result
        api = MentionAPI()
        self.assertEqual(dispatch.dispatch_cards([card()], api, now=NOW), [])
        api.current_status = 'Ready'
        self.assertEqual(dispatch.dispatch_cards([card(status='Ready')], api, now=NOW), [])
        self.assertEqual(api.posts, [])

    def test_completed_review_prefix_cannot_starve_ready_or_later_review(self):
        old = [run(n, issue=n) for n in range(1, 21)]
        for prior in old:
            prior['updated_at'] = '2026-10-19T00:00:00Z'
        api = FakeAPI(old)
        api.current_status = 'Ready'
        selected = dispatch.dispatch_cards([card(n) for n in range(1, 21)] + [card(21, 'Ready')], api, now=NOW)
        self.assertEqual([x['issue'] for x in selected], ['21'])
        outcomes = []
        for hop in range(3):
            api = FakeAPI(old)
            outcomes += dispatch.dispatch_cards([card(n) for n in range(1, 22)], api, now=NOW + hop * 300)
        self.assertIn('21', [x['issue'] for x in outcomes])

    def test_ready_from_a_review_is_bound_to_reviewed_head_until_reassociated(self):
        class RoutedAPI(FakeAPI):
            reassociate = False
            def __call__(self, kind, path, data=None):
                result = super().__call__(kind, path, data)
                if '/comments?' in path:
                    result.append({'body': f'<!-- hifld-agent-route:v1 issue=282 pr=548 head={SHA} label=move:ready -->', 'user': {'login': 'maintainer'}})
                    if self.reassociate:
                        result.append(result[0])
                return result
        api = RoutedAPI()
        api.current_status = 'Ready'
        api.head = 'b' * 40
        self.assertEqual(dispatch.dispatch_cards([card(status='Ready')], api, now=NOW), [])
        api.reassociate = True
        self.assertEqual(len(dispatch.dispatch_cards([card(status='Ready')], api, now=NOW)), 1)


if __name__ == '__main__':
    unittest.main()

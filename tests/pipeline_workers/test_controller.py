"""State/identity tests for the trusted controller; no live GitHub mutations."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('controller', ROOT / 'tools/pipeline_workers/controller.py')
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
spec = importlib.util.spec_from_file_location('schema_gate', ROOT / 'tools/pipeline_workers/schema_gate.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def card(status='Ready', agent='Codex', repo='fulton-ring/hifld-next-datasets'):
    return {'id':'item', 'values':{'Status':status, 'Agent':agent},
            'content':{'number':7, 'title':'Hospitals', 'body':'<!-- hifld-dataset:hospitals-3 -->',
                       'state':'OPEN', 'repository':{'nameWithOwner':repo}}}


class Board:
    def __init__(self, status='Review'):
        self.cards = [card(status)]
        self.moves = []

    def items(self):
        return self.cards

    def move(self, item, status):
        self.moves.append(status)


class GitHub:
    repo = 'fulton-ring/hifld-next-datasets'

    def __init__(self, review_head='abc'):
        self.review_head = review_head
        self.calls = []
        self.comments = []
        self.attempts = []

    def pages(self, path):
        return self.attempts if 'comments' in path else []

    def rest(self, method, path, data=None):
        self.calls.append((method,path,data))
        if path == 'pulls/8':
            return {'head':{'sha':self.review_head}, 'state':'open'}
        if path.endswith('/merge'):
            return {'merged':True}
        if 'check-runs' in path:
            return {'total_count':0,'check_runs':[]}
        if path.endswith('/status'):
            return {'state':'pending','total_count':0}
        return {}

    def comment(self, issue, body):
        self.comments.append(body)


class ControllerTests(unittest.TestCase):
    def test_assignment_and_human_lanes_fail_closed(self):
        for status in ['Potential breaking change','Blocked','Needs decision','Done','Review']:
            self.assertFalse(c.eligible(card(status), GitHub.repo))
        for agent in ['',None,'Claude worker']:
            self.assertFalse(c.eligible(card(agent=agent), GitHub.repo))
        self.assertFalse(c.eligible(card(repo='elsewhere/repo'), GitHub.repo))
        self.assertTrue(c.eligible(card(), GitHub.repo))

    def test_issue_marker_and_paths(self):
        self.assertEqual(c.dataset_id('<!-- hifld-dataset:abc-2 -->'), 'abc-2')
        self.assertEqual(c.dataset_id('<!-- hifld-dataset:nfhl/flood-zones -->'), 'nfhl/flood-zones')
        with self.assertRaises(ValueError):
            c.dataset_id('<!-- hifld-dataset:abc/../../x -->')
        with self.assertRaises(ValueError):
            c.dataset_id('title only')
        for path in ['.github/workflows/x.yml','tools/pipeline_workers/controller.py',
                     'AGENTS.md','tests/pipeline_workers/test_controller.py','tests/../../tools/x.py','/tests/x.py','docs/dataset-sources/other/source.json']:
            self.assertFalse(c.allowed_path(path, 'implement', 'abc'))
        self.assertFalse(c.allowed_path('src/dagster_hifld/assets/x.py','research','abc'))
        self.assertTrue(c.allowed_path('docs/dataset-sources/abc/source.json','research','abc'))

    def test_dry_run_does_not_move_or_claim(self):
        github, board = GitHub(), Board('Source research')
        with tempfile.NamedTemporaryFile() as f, patch.dict(os.environ, {'GITHUB_OUTPUT':f.name}):
            c.plan(github, board, True)
        self.assertEqual(board.moves, [])
        self.assertEqual(github.comments, [])

    def test_retry_limit_and_phase_preservation(self):
        github, board = GitHub(), Board('Changes requested')
        github.attempts = [{'body':c.MARKER+'\nAttempt 1; phase research; run x'}]
        with tempfile.NamedTemporaryFile() as f, patch.dict(os.environ, {'GITHUB_OUTPUT':f.name}):
            c.plan(github, board, True)
            f.seek(0)
            matrix = json.loads(f.read().decode().splitlines()[0].split('=',1)[1])
        self.assertEqual(matrix['include'][0]['phase'], 'research')
        github.attempts *= 3
        with tempfile.NamedTemporaryFile() as f, patch.dict(os.environ, {'GITHUB_OUTPUT':f.name}):
            c.plan(github, board, False)
        self.assertEqual(board.moves,['Needs decision'])

    def finish(self, verdict='approve', head='abc', status='Review', tests='success', phase='implement', **gates):
        github, board = GitHub(head), Board(status)
        task = {'item':'item','issue':7,'phase':phase}
        report = dict(verdict=verdict, summary='Evidence', source_authority_verified=True,
                      schema_compatible=True, ids_preserved=True)
        report.update(gates)
        with tempfile.TemporaryDirectory() as directory:
            previous = os.getcwd()
            try:
                os.chdir(directory)
                Path('review').mkdir()
                Path('review/review.json').write_text(json.dumps(report))
                with patch.dict(os.environ, {'TEST_RESULT':tests,'REVIEW_RESULT':'success'}):
                    c.finish(github,board,task,8,'abc')
            finally:
                os.chdir(previous)
        return github, board

    def test_stale_head_and_manual_move_reject_completion(self):
        with self.assertRaises(ValueError):
            self.finish(head='new')
        with self.assertRaises(ValueError):
            self.finish(status='Potential breaking change')

    def test_schema_or_mirror_finding_blocks_merge(self):
        for kwargs in [{'schema_compatible':False},{'ids_preserved':False},
                       {'source_authority_verified':False},{'verdict':'breaking_change'}]:
            github, board = self.finish(**kwargs)
            self.assertEqual(board.moves,['Potential breaking change'])
            self.assertFalse(any(p.endswith('/merge') for _,p,_ in github.calls))

    def test_tests_or_findings_request_changes(self):
        for kwargs in [{'tests':'failure'},{'verdict':'changes_requested'}]:
            github, board = self.finish(**kwargs)
            self.assertEqual(board.moves,['Changes requested'])
            self.assertFalse(any(p.endswith('/merge') for _,p,_ in github.calls))

    def test_success_merges_exact_head_and_research_stays_open(self):
        github, board = self.finish()
        self.assertIn(('PUT','pulls/8/merge',{'sha':'abc','merge_method':'squash'}),github.calls)
        self.assertEqual(board.moves,['Done'])
        github, board = self.finish(phase='research')
        self.assertEqual(board.moves,['Ready'])
        self.assertIn(('PATCH','issues/7',{'state':'open'}),github.calls)


class SchemaTests(unittest.TestCase):
    def setUp(self):
        self.columns={'ID':{'type':'string','nullable':False,'id_role':'source','unique':True},
                      'name':{'type':'string','nullable':True}}
        self.contract={'raw_columns':self.columns,'published_columns':self.columns,
                       'geometry':{'type':'Point','crs':'EPSG:4326'},'coverage':'US'}
        self.ids={'ID':{'values_preserved':True,'unique':True,'non_null':True}}

    def check(self, raw=None, published=None, ids=None):
        gate.require_compatible(self.contract,self.columns if raw is None else raw,
            self.columns if published is None else published,self.ids if ids is None else ids,
            self.contract['geometry'],'US')

    def test_accepts_only_unchanged_contract(self):
        self.check()

    def test_extra_columns_missing_ids_and_type_changes_quarantine(self):
        for cols in [{'name':self.columns['name']}, self.columns|{'random':{'type':'int'}},
                     self.columns|{'ID':self.columns['ID']|{'type':'int'}}]:
            with self.assertRaises(ValueError):
                self.check(raw=cols)
            with self.assertRaises(ValueError):
                self.check(published=cols)

    def test_identifier_value_loss_null_and_duplicate_fail(self):
        for key in ['values_preserved','unique','non_null']:
            with self.assertRaises(ValueError):
                self.check(ids={'ID':self.ids['ID']|{key:False}})
        with self.assertRaises(ValueError):
            self.check(ids={})


if __name__=='__main__':
    unittest.main()

class PublishTests(unittest.TestCase):
    def test_original_task_is_used_and_nul_path_list_accepts_valid_patch(self):
        """A worker cannot replace trusted task metadata in its artifact."""
        github, board = GitHub(), Board('In progress')
        payload={'task':{'item':'item','issue':7,'phase':'research','dataset':'abc','branch':'codex/dataset-7','pr':0},
                 'head':'base','base':'base','base_branch':'main','issue':{'title':'Example'}}
        commands=[]
        def fake_git(*args, **kwargs):
            commands.append(args)
            if args[:3]==('diff','--cached','--name-only'):
                return 'docs/dataset-sources/abc/source.json\0'
            if args[:3]==('diff','--cached','--raw'):
                return ':000000 100644 0000000 1234567 A docs/dataset-sources/abc/source.json'
            if args==('rev-parse','HEAD'):
                return 'new'
            return ''
        previous=os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            try:
                os.chdir(directory)
                for name in ['payload','original','candidate/docs/dataset-sources/abc']:
                    Path(name).mkdir(parents=True)
                Path('original/task.json').write_text(json.dumps(payload))
                Path('payload/task.json').write_text('{"task":"tampered"}')
                Path('payload/result.json').write_text(json.dumps({'outcome':'ready','summary':'Researched'}))
                Path('payload/changes.patch').write_text('patch data')
                Path('candidate/docs/dataset-sources/abc/source.json').write_text(json.dumps({'source_role':'producer_publication'}))
                def rest(method,path,data=None):
                    return {'number':8} if path=='pulls' else {}
                with patch.object(c,'git',side_effect=fake_git), patch.object(github,'rest',side_effect=rest), \
                     patch.dict(os.environ,{'GITHUB_OUTPUT':str(Path(directory)/'output')}):
                    c.publish(github,board)
            finally:
                os.chdir(previous)
        self.assertEqual(board.moves,['Review'])
        self.assertTrue(any(args[0]=='push' and '--force-with-lease=refs/heads/codex/dataset-7:' in args for args in commands))

class ReconcileTests(unittest.TestCase):
    def test_only_controller_authored_review_can_resume(self):
        github, board = GitHub(), Board('Review')
        github.graphql=lambda query: {'viewer':{'login':'controller-bot'}}
        record={'task':{'issue':7,'item':'item','phase':'implement'},'head':'abc','pr':8,
                'review':{'verdict':'approve','summary':'ok','source_authority_verified':True,
                          'schema_compatible':True,'ids_preserved':True}}
        github.attempts=[{'user':{'login':'stranger'},'body':c.REVIEW_MARKER+'\n'+json.dumps(record)}]
        with patch.object(c,'finish') as finish:
            c.reconcile_review(github,board,board.cards[0])
            finish.assert_not_called()

    def test_head_change_invalidates_saved_review(self):
        github, board = GitHub('new'), Board('Review')
        github.graphql=lambda query: {'viewer':{'login':'controller-bot'}}
        record={'task':{'issue':7,'item':'item','phase':'implement'},'head':'abc','pr':8,'review':{}}
        github.attempts=[{'user':{'login':'controller-bot'},'body':c.REVIEW_MARKER+'\n'+json.dumps(record)}]
        with patch.object(c,'finish') as finish:
            c.reconcile_review(github,board,board.cards[0])
            finish.assert_not_called()
        self.assertEqual(board.moves,['Changes requested'])

    def test_same_head_resumes_without_model_and_restores_environment(self):
        github, board = GitHub(), Board('Review')
        github.graphql=lambda query: {'viewer':{'login':'controller-bot'}}
        record={'task':{'issue':7,'item':'item','phase':'implement'},'head':'abc','pr':8,
                'review':{'verdict':'approve'}}
        github.attempts=[{'user':{'login':'controller-bot'},'body':c.REVIEW_MARKER+'\n'+json.dumps(record)}]
        previous=os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            try:
                os.chdir(directory)
                with patch.object(c,'finish') as finish:
                    c.reconcile_review(github,board,board.cards[0])
                    finish.assert_called_once_with(github,board,record['task'],8,'abc')
            finally:
                os.chdir(previous)
        self.assertNotIn('RESUMING_REVIEW',os.environ)

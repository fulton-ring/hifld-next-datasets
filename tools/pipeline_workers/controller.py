"""Trusted GitHub controller. No model execution or third-party dependencies here."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request

STATES = ('Source research', 'Ready', 'In progress', 'Review', 'Changes requested',
          'Potential breaking change', 'Blocked', 'Needs decision', 'Done')
MARKER = '<!-- pipeline-worker-attempt -->'
REVIEW_MARKER = '<!-- pipeline-worker-review -->'
MAX_ATTEMPTS = 3
MAX_PAGES = 20


class GitHub:
    def __init__(self) -> None:
        self.token = os.environ['GH_TOKEN']
        self.repo = os.environ['GITHUB_REPOSITORY']

    def request(self, method: str, path: str, data: dict | None = None) -> dict | list:
        url = path if path.startswith('https://api.github.com/') else 'https://api.github.com' + path
        request = urllib.request.Request(url, method=method,
            data=json.dumps(data).encode() if data is not None else None,
            headers={'Authorization': f'Bearer {self.token}', 'Accept': 'application/vnd.github+json',
                     'X-GitHub-Api-Version': '2022-11-28', 'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response) if response.status != 204 else {}

    def rest(self, method: str, path: str, data: dict | None = None) -> dict | list:
        return self.request(method, f'/repos/{self.repo}/{path}', data)

    def pages(self, path: str) -> list:
        result = []
        for page in range(1, MAX_PAGES + 1):
            values = self.rest('GET', f'{path}{"&" if "?" in path else "?"}per_page=100&page={page}')
            if not isinstance(values, list):
                raise ValueError('Expected paginated list')
            result.extend(values)
            if len(values) < 100:
                return result
        raise RuntimeError('Pagination limit reached; refusing a partial result')

    def graphql(self, query: str, **variables: object) -> dict:
        result = self.request('POST', '/graphql', {'query': query, 'variables': variables})
        if result.get('errors'):
            raise RuntimeError(json.dumps(result['errors']))
        return result['data']

    def comment(self, issue: int, body: str) -> None:
        self.rest('POST', f'issues/{issue}/comments', {'body': body})


class Board:
    def __init__(self, github: GitHub) -> None:
        self.github = github
        self.project = os.environ['PIPELINE_PROJECT_ID']
        fields = github.graphql('''query($id:ID!){node(id:$id){... on ProjectV2{
          fields(first:100){nodes{... on ProjectV2SingleSelectField{id name options{id name}}}}}}}''', id=self.project)
        node = fields['node']
        if node is None:
            raise ValueError('Project unavailable to controller token')
        self.fields = {f['name']: f for f in node['fields']['nodes'] if f}
        for name in ('Status', 'Agent'):
            if name not in self.fields:
                raise ValueError(f'Missing project single-select field: {name}')
        options = {o['name'] for o in self.fields['Status']['options']}
        if not set(STATES) <= options:
            raise ValueError('Project Status options do not match documented worker states')

    def items(self) -> list[dict]:
        items = []
        cursor = None
        for _ in range(MAX_PAGES):
            result = self.github.graphql('''query($id:ID!,$cursor:String){node(id:$id){... on ProjectV2{
              items(first:100,after:$cursor){pageInfo{hasNextPage endCursor} nodes{id
                fieldValues(first:30){nodes{... on ProjectV2ItemFieldSingleSelectValue{
                  name field{... on ProjectV2SingleSelectField{name}}}}}
                content{... on Issue{number state title body repository{nameWithOwner}}}
              }}}}}''', id=self.project, cursor=cursor)['node']['items']
            for item in result['nodes']:
                item['values'] = {v['field']['name']: v['name'] for v in item['fieldValues']['nodes'] if v}
                items.append(item)
            if not result['pageInfo']['hasNextPage']:
                return items
            cursor = result['pageInfo']['endCursor']
        raise RuntimeError('Project pagination limit reached')

    def move(self, item: str, status: str) -> None:
        field = self.fields['Status']
        option = next(o['id'] for o in field['options'] if o['name'] == status)
        self.github.graphql('''mutation($p:ID!,$i:ID!,$f:ID!,$o:String!){
          updateProjectV2ItemFieldValue(input:{projectId:$p,itemId:$i,fieldId:$f,
          value:{singleSelectOptionId:$o}}){projectV2Item{id}}}''', p=self.project, i=item, f=field['id'], o=option)


def git(*args: str, cwd: str = 'candidate') -> str:
    return subprocess.run(['git', '-C', cwd, *args], check=True, text=True, capture_output=True).stdout.strip()


def write_output(**values: object) -> None:
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        for key, value in values.items():
            output.write(f'{key}={json.dumps(value) if not isinstance(value, str) else value}\n')


def dataset_id(body: str) -> str:
    match = re.search(r'<!-- hifld-dataset:([A-Za-z0-9][A-Za-z0-9-]*(?:/[A-Za-z0-9][A-Za-z0-9-]*)*) -->', body)
    if match is None:
        raise ValueError('Issue has no stable HIFLD dataset marker')
    return match.group(1)


def eligible(item: dict, repo: str) -> bool:
    issue = item.get('content') or {}
    return (issue.get('repository', {}).get('nameWithOwner') == repo
        and issue.get('state') == 'OPEN' and item['values'].get('Agent') == 'Codex'
        and item['values'].get('Status') in {'Source research', 'Ready', 'Changes requested', 'In progress'})


def plan(github: GitHub, board: Board, dry_run: bool) -> None:
    selected = []
    limit = max(1, min(5, int(os.environ.get('PIPELINE_MAX_WORKERS', '2'))))
    for item in board.items():
        if item['values'].get('Status') == 'Review' and item['values'].get('Agent') == 'Codex':
            if not dry_run:
                reconcile_review(github, board, item)
            continue
        if not eligible(item, github.repo):
            continue
        issue = item['content']
        try:
            slug = dataset_id(issue['body'])
        except ValueError:
            continue
        comments = github.pages(f'issues/{issue["number"]}/comments')
        attempts = sum(c['body'].startswith(MARKER) for c in comments)
        if attempts >= MAX_ATTEMPTS:
            if not dry_run:
                board.move(item['id'], 'Needs decision')
            continue
        branch = f'codex/dataset-{issue["number"]}'
        prs = github.pages('pulls?state=open&head=' + urllib.parse.quote(github.repo.split('/')[0] + ':' + branch))
        if len(prs) > 1:
            raise ValueError('Multiple active PRs for one worker branch')
        status = item['values']['Status']
        previous_phase = next((re.search(r'phase (research|implement)', c['body']).group(1)
            for c in reversed(comments) if c['body'].startswith(MARKER)
            and re.search(r'phase (research|implement)', c['body'])), 'implement')
        # Recover failed attempts on the next serialized dispatcher tick. Review is
        # resumed only by moving to Changes requested, never blindly overwritten.
        selected.append({'issue': issue['number'], 'item': item['id'], 'dataset': slug,
                         'phase': ('research' if status == 'Source research' else
                                   previous_phase if status in ('Changes requested', 'In progress') else 'implement'),
                         'branch': branch, 'pr': prs[0]['number'] if prs else 0,
                         'attempt': attempts + 1})
        if len(selected) >= limit:
            break
    print(json.dumps({'dry_run': dry_run, 'selected': selected}))
    write_output(matrix={'include': selected}, has_work='true' if selected and not dry_run else 'false')


def prepare(github: GitHub, board: Board, task: dict) -> None:
    item = next(i for i in board.items() if i['id'] == task['item'])
    if not eligible(item, github.repo):
        raise ValueError('Card no longer eligible')
    issue = github.rest('GET', f'issues/{task["issue"]}')
    if dataset_id(issue['body']) != task['dataset'] or issue['state'] != 'open':
        raise ValueError('Issue identity changed')
    repo = github.request('GET', '/repos/' + github.repo)
    base_branch = repo['default_branch']
    base = github.rest('GET', f'git/ref/heads/{base_branch}')['object']['sha']
    ref = base
    if task['pr']:
        pr = github.rest('GET', f'pulls/{task["pr"]}')
        if pr['head']['ref'] != task['branch'] or pr['head']['repo']['full_name'] != github.repo:
            raise ValueError('Unexpected worker PR repository')
        ref = pr['head']['sha']
    comments = github.pages(f'issues/{task["issue"]}/comments')
    Path('task.json').write_text(json.dumps({'task': task, 'issue': issue,
        'comments': comments, 'base': base, 'base_branch': base_branch, 'head': ref}))
    board.move(task['item'], 'In progress')
    github.comment(task['issue'], f'{MARKER}\nAttempt {task["attempt"]}; phase {task["phase"]}; '
        f'run https://github.com/{github.repo}/actions/runs/{os.environ["GITHUB_RUN_ID"]}')
    write_output(head=ref)


def allowed_path(path: str, phase: str, slug: str) -> bool:
    if '..' in Path(path).parts or path.startswith('/') or path.startswith('tests/pipeline_workers/'):
        return False
    if path.startswith(f'docs/dataset-sources/{slug}/'):
        return True
    return phase == 'implement' and path.startswith(('src/dagster_hifld/', 'tests/'))


def publish(github: GitHub, board: Board) -> None:
    payload = json.loads(Path('original/task.json').read_text())
    task = payload['task']
    result = json.loads(Path('payload/result.json').read_text())
    item = next(i for i in board.items() if i['id'] == task['item'])
    if item['values'].get('Status') != 'In progress' or item['values'].get('Agent') != 'Codex':
        raise ValueError('Card was moved or reassigned while worker ran')
    if result.get('outcome') not in ('ready', 'breaking_change', 'blocked'):
        raise ValueError('Invalid worker outcome')
    if result['outcome'] in ('breaking_change', 'blocked'):
        status = 'Potential breaking change' if result['outcome'] == 'breaking_change' else 'Blocked'
        board.move(task['item'], status)
        github.comment(task['issue'], f'{status}: {result["summary"]}')
        write_output(has_pr='false')
        return
    # Artifact patch is data, not executable controller code. Apply only to the
    # expected base and allowlisted source/test/evidence paths in a fresh job.
    patch = Path('payload/changes.patch')
    if not patch.stat().st_size or patch.stat().st_size > 5_000_000:
        raise ValueError('Empty or oversized worker patch')
    git('checkout', '--detach', payload['head'])
    git('apply', '--index', '--', str(patch.resolve()))
    paths = [p for p in git('diff', '--cached', '--name-only', '-z').split('\0') if p]
    if not all(allowed_path(p, task['phase'], task['dataset']) for p in paths):
        raise ValueError('Patch changed a controller, workflow, contract or unrelated path')
    modes = git('diff', '--cached', '--raw')
    if re.search(r'\b(120000|160000)\b', modes):
        raise ValueError('Symlinks and submodules are not accepted')
    evidence_path = f'docs/dataset-sources/{task["dataset"]}/source.json'
    evidence = json.loads(Path('candidate', evidence_path).read_text())
    if evidence.get('source_role') not in ('producer_publication', 'official_distributor'):
        board.move(task['item'], 'Potential breaking change')
        github.comment(task['issue'], 'Source lineage unresolved or a mirror/archive was proposed. Maintainer decision required.')
        write_output(has_pr='false')
        return
    if task['phase'] == 'implement':
        baseline_path = f'tools/pipeline_workers/contracts/{task["dataset"]}.json'
        try:
            baseline = json.loads(git('show', f'{payload["base"]}:{baseline_path}'))
        except subprocess.CalledProcessError:
            baseline = None
        if baseline is None or baseline != evidence.get('published_contract'):
            board.move(task['item'], 'Potential breaking change')
            github.comment(task['issue'], 'Missing approved schema baseline or proposed published contract differs. IDs/columns may not be changed by a worker; maintainer compatibility decision required.')
            write_output(has_pr='false')
            return
    git('config', 'user.name', 'pipeline-workers[bot]')
    git('config', 'user.email', 'pipeline-workers@users.noreply.github.com')
    git('commit', '-m', f'Dataset {task["dataset"]}: {task["phase"]}')
    sha = git('rev-parse', 'HEAD')
    # Explicit lease prevents a late worker replacing a newer branch head.
    expected = payload['head'] if task['pr'] else ''
    git('push', f'--force-with-lease=refs/heads/{task["branch"]}:{expected}',
        'origin', f'HEAD:refs/heads/{task["branch"]}')
    pr_number = task['pr']
    if not pr_number:
        pr = github.rest('POST', 'pulls', {'head': task['branch'], 'base': payload['base_branch'],
            'title': f'{task["phase"].title()}: {payload["issue"]["title"]}',
            'body': f'{"Relates to" if task["phase"] == "research" else "Closes"} #{task["issue"]}\n\nDataset: `{task["dataset"]}`\n\n{result["summary"]}'})
        pr_number = pr['number']
    board.move(task['item'], 'Review')
    github.comment(task['issue'], f'Worker PR: https://github.com/{github.repo}/pull/{pr_number}; head `{sha}`')
    write_output(has_pr='true', pr=str(pr_number), head=sha)


def finish(github: GitHub, board: Board, task: dict, pr_number: int, head: str) -> None:
    pr = github.rest('GET', f'pulls/{pr_number}')
    if pr['head']['sha'] != head or pr['state'] != 'open':
        raise ValueError('PR head or state changed; refusing stale review')
    item = next(i for i in board.items() if i['id'] == task['item'])
    if item['values'].get('Status') != 'Review' or item['values'].get('Agent') != 'Codex':
        raise ValueError('Card moved or reassigned; refusing completion')
    checks_passed = os.environ['TEST_RESULT'] == 'success'
    review_passed = os.environ['REVIEW_RESULT'] == 'success'
    if not review_passed:
        board.move(task['item'], 'Changes requested')
        github.comment(task['issue'], 'Independent review job failed. See workflow logs; no merge.')
        return
    review = json.loads(Path('review/review.json').read_text())
    verdict = review['verdict']
    if verdict == 'approve' and not all(review.get(k) is True for k in
        ('source_authority_verified', 'schema_compatible', 'ids_preserved')):
        verdict = 'breaking_change'
    if verdict == 'approve' and checks_passed and os.environ.get('RESUMING_REVIEW') != '1':
        record = {'task': task, 'pr': pr_number, 'head': head, 'review': review}
        github.comment(task['issue'], REVIEW_MARKER + '\n' + json.dumps(record))
    if verdict == 'breaking_change':
        board.move(task['item'], 'Potential breaking change')
    elif verdict != 'approve' or not checks_passed:
        board.move(task['item'], 'Changes requested')
    else:
        # Check external required checks too; GitHub's merge API enforces branch
        # rules. Pending or failed external checks are never treated as passing.
        runs = github.rest('GET', f'commits/{head}/check-runs?per_page=100')
        if runs.get('total_count', 0) > 100:
            raise ValueError('Check-run pagination required; refusing partial results')
        failed = [r for r in runs['check_runs'] if r['name'] not in ('finish',)
                  and (r['status'] != 'completed' or r['conclusion'] != 'success')]
        status = github.rest('GET', f'commits/{head}/status')
        if failed or status['state'] not in ('success', 'pending') or (status['state'] == 'pending' and status['total_count']):
            github.comment(task['issue'], 'External checks pending or failing; left in Review. Dispatcher will recheck this reviewed head without starting another worker.')
            return
        merged = github.rest('PUT', f'pulls/{pr_number}/merge', {'sha': head, 'merge_method': 'squash'})
        if not merged.get('merged'):
            raise RuntimeError('GitHub declined merge')
        # Research PRs do not resolve pipeline implementation issues.
        board.move(task['item'], 'Ready' if task['phase'] == 'research' else 'Done')
        if task['phase'] == 'research':
            github.rest('PATCH', f'issues/{task["issue"]}', {'state': 'open'})
    github.comment(task['issue'], f'Independent review for `{head}`: {verdict}\n\n{review["summary"]}\n\nTests: {os.environ["TEST_RESULT"]}')



def reconcile_review(github: GitHub, board: Board, item: dict) -> None:
    """Recheck a controller-authored review after external checks settle, no model call."""
    issue = item.get('content') or {}
    if issue.get('repository', {}).get('nameWithOwner') != github.repo or issue.get('state') != 'OPEN':
        return
    viewer = github.graphql('query{viewer{login}}')['viewer']['login']
    comments = github.pages(f'issues/{issue["number"]}/comments')
    for comment in reversed(comments):
        if comment.get('user', {}).get('login') != viewer or not comment['body'].startswith(REVIEW_MARKER + '\n'):
            continue
        record = json.loads(comment['body'].split('\n', 1)[1])
        task = record['task']
        if task['issue'] != issue['number'] or task['item'] != item['id']:
            raise ValueError('Stored review identity mismatch')
        pr = github.rest('GET', f'pulls/{record["pr"]}')
        if pr['state'] != 'open':
            # A human may merge/close outside the controller; do not claim Done.
            board.move(item['id'], 'Needs decision')
            return
        if pr['head']['sha'] != record['head']:
            board.move(item['id'], 'Changes requested')
            return
        Path('review').mkdir(exist_ok=True)
        Path('review/review.json').write_text(json.dumps(record['review']))
        previous = {key: os.environ.get(key) for key in ('TEST_RESULT','REVIEW_RESULT','RESUMING_REVIEW')}
        try:
            os.environ.update(TEST_RESULT='success', REVIEW_RESULT='success', RESUMING_REVIEW='1')
            finish(github, board, task, record['pr'], record['head'])
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=('plan', 'prepare', 'publish', 'finish'))
    parser.add_argument('--task')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--pr', type=int)
    parser.add_argument('--head')
    args = parser.parse_args()
    github = GitHub()
    board = Board(github)
    if args.command == 'plan':
        plan(github, board, args.dry_run)
    elif args.command == 'prepare':
        prepare(github, board, json.loads(args.task))
    elif args.command == 'publish':
        publish(github, board)
    else:
        finish(github, board, json.loads(args.task), args.pr, args.head)


if __name__ == '__main__':
    main()

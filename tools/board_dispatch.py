"""Admit Ready/Review cards into the private worker workflow, without a run backlog."""

import argparse
from datetime import datetime, timezone
import json
import os
import re
import time
import urllib.parse

from board_sync import FIELDS, MOVE_LABELS, request
from agent_handoffs import pages, read_handoffs

REPO = 'fulton-ring/hifld-next-datasets'
CONTROL = 'fulton-ring/hifld-next-iac'
WORKFLOW = f'repos/{CONTROL}/actions/workflows/agent-worker.yml'
SHARED_BASE = 'codex/cloud-verification-417'
IDENTITY = re.compile(r'^(implementation|review) issue #([1-9][0-9]*) @ ([a-f0-9]{40})$')
ITEMS = '''query($project:ID!,$cursor:String){node(id:$project){... on ProjectV2{
 items(first:100,after:$cursor){pageInfo{hasNextPage endCursor} nodes{
 id status:fieldValueByName(name:"Status"){... on ProjectV2ItemFieldSingleSelectValue{name}}
 content{... on Issue{number state body repository{nameWithOwner} labels(first:100){totalCount nodes{name}}}}
 }}}}}'''
CURRENT = '''query($item:ID!){node(id:$item){... on ProjectV2Item{
 status:fieldValueByName(name:"Status"){... on ProjectV2ItemFieldSingleSelectValue{name}}
 content{... on Issue{number state repository{nameWithOwner} labels(first:100){totalCount nodes{name}}}}
 }}}'''


def stamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.utcoffset() is None:
        raise ValueError('Run timestamp lacks timezone')
    return parsed.timestamp()


def capacity(runs, now):
    active = [r for r in runs if r['status'] != 'completed']
    # Legacy manual runs lack an issue/head identity: wait rather than overlap them.
    if any(not IDENTITY.fullmatch(r.get('display_title', '')) for r in active):
        return 0
    attempts = 0
    for run in runs:
        attempt = run['run_attempt']
        if type(attempt) is not int or attempt < 1:
            raise ValueError('Invalid admission ledger')
        if stamp(run['updated_at']) >= now - 86400:
            attempts += attempt
    return max(0, min(2 - len(active), 6 - attempts, 2))


def ledger(api):
    runs = []
    for page in range(1, 11):
        batch = api('control', f'{WORKFLOW}/runs?per_page=100&page={page}')['workflow_runs']
        runs.extend(batch)
        if len(batch) < 100:
            return runs
    raise ValueError('Admission ledger exceeds audit bound')


def graphql(api, query, variables):
    result = api('board', 'graphql', dict(query=query, variables=variables))
    if result.get('errors'):
        raise ValueError('Project query failed')
    return result['data']


def actionable(item):
    content = item.get('content') or {}
    labels = content.get('labels', {})
    return (content.get('state') == 'OPEN'
            and content.get('repository', {}).get('nameWithOwner') == REPO
            and labels.get('totalCount', 101) <= 100
            and not any(x['name'] in MOVE_LABELS for x in labels['nodes']))


def cards(api):
    project = graphql(api, FIELDS, dict(owner='fulton-ring', number=2))['organization']['projectV2']
    if not project:
        raise ValueError('Project inaccessible')
    result, cursor = [], None
    for _ in range(10):
        page = graphql(api, ITEMS, dict(project=project['id'], cursor=cursor))['node']['items']
        for item in page['nodes']:
            if actionable(item) and '<!-- hifld-dataset:' in item['content']['body']:
                result.append(dict(id=item['id'], number=item['content']['number'],
                                   status=(item.get('status') or {}).get('name')))
        if not page['pageInfo']['hasNextPage']:
            return result
        cursor = page['pageInfo']['endCursor']
    raise ValueError('Project exceeds audit bound')


def pull_requests(issue, api):
    # A public mention is not ownership. Only a writer's explicit association
    # selects a PR; workers record it in a trusted post-step after saving work.
    handoff = read_handoffs(issue, api)
    selected = handoff['pr']
    if selected is not None:
        pr = api('repo', f'repos/{REPO}/pulls/{selected}')
        if (pr['state'] != 'open' or not pr['head'].get('repo') or pr['head']['repo']['full_name'] != REPO
                or pr['base']['repo']['full_name'] != REPO):
            raise ValueError('Selected PR is closed or outside this repository')
        pr['_review_route'] = handoff['route']
        return [pr]
    # Existing unassociated PRs need supervisor reconciliation; do not create a
    # duplicate pipeline merely because the older handoff lacks the new marker.
    numbers = set()
    for event in pages(api, f'repos/{REPO}/issues/{issue}/timeline'):
        url = event.get('source', {}).get('issue', {}).get('pull_request', {}).get('url', '')
        match = re.fullmatch(r'https://api.github.com/repos/' + re.escape(REPO) + r'/pulls/([1-9][0-9]*)', url)
        if event.get('event') == 'cross-referenced' and match:
            numbers.add(int(match[1]))
    for number in sorted(numbers):
        pr = api('repo', f'repos/{REPO}/pulls/{number}')
        if (pr['state'] == 'open' and pr['head'].get('repo')
                and pr['head']['repo']['full_name'] == REPO and pr['base']['repo']['full_name'] == REPO):
            raise ValueError('Existing PR needs an explicit supervisor association')
    return []


def task_for(card, api):
    prs = pull_requests(card['number'], api)
    if len(prs) > 1:
        raise ValueError('Multiple same-repository PRs need supervisor selection')
    if card['status'] == 'Review' and not prs:
        raise ValueError('Review requires an open same-repository PR')
    pr = prs[0] if prs else None
    sha = pr['head']['sha'] if pr else api('repo', f'repos/{REPO}/commits/{urllib.parse.quote(SHARED_BASE, safe="")}')['sha']
    if not re.fullmatch('[a-f0-9]{40}', sha):
        raise ValueError('An exact commit is required')
    role = 'review' if card['status'] == 'Review' else 'implementation'
    route = pr.get('_review_route') if pr else None
    if role == 'implementation' and route and route['label'] == 'move:ready':
        if route['pr'] != pr['number'] or route['head'] != sha:
            raise ValueError('Review requested changes to a different head; supervisor reconciliation required')
    return dict(issue=str(card['number']), role=role, dispatch_head_sha=sha,
                datasets_ref=sha if role == 'review' else (pr['head']['ref'] if pr else SHARED_BASE),
                pr_base=pr['base']['ref'] if pr else SHARED_BASE,
                pull_request_number=str(pr['number']) if pr else '0')


def dispatch_cards(board, api, dry_run=False, now=None):
    current_time = time.time if now is None else lambda: now
    selected, reservations = [], []
    # Rotate bounded windows independently: stale reviews cannot starve Ready,
    # and an attempted prefix cannot permanently hide later cards. No stored
    # cursor, issue churn or extra private polling workflow is needed.
    window = []
    for lane in ('Review', 'Ready'):
        group = sorted((c for c in board if c['status'] == lane), key=lambda c: c['number'])
        if group:
            start = (int(current_time()) // 300 * 10) % len(group)
            window += (group[start:] + group[:start])[:10]
    for card in window:
        runs = ledger(api)
        known = {r['id'] for r in runs}
        runs += [r for r in reservations if r['id'] not in known]
        if not capacity(runs, current_time()):
            print('Dispatch held: two-slot pool or six-attempt rolling allowance is full.')
            break
        try:
            task = task_for(card, api)
            identity = (task['role'], task['issue'], task['dispatch_head_sha'])
            seen = [IDENTITY.fullmatch(r.get('display_title', '')) for r in runs]
            if any(m and (m.groups() == identity or (m[2] == task['issue'] and r['status'] != 'completed'))
                   for m, r in zip(seen, runs)):
                continue
            # Recheck both the lane and exact PR head immediately before admission.
            item = graphql(api, CURRENT, dict(item=card['id']))['node']
            if (not actionable(item) or item['content']['number'] != card['number']
                    or (item.get('status') or {}).get('name') != card['status']):
                continue
            if task['pull_request_number'] != '0':
                pr = api('repo', f"repos/{REPO}/pulls/{task['pull_request_number']}")
                if pr['state'] != 'open' or pr['head']['sha'] != task['dispatch_head_sha']:
                    continue
        except Exception as error:
            print(f"Hold #{card['number']}: {type(error).__name__}; supervisor action required.")
            continue
        title = f"{task['role']} issue #{task['issue']} @ {task['dispatch_head_sha']}"
        if dry_run:
            run_id = -len(selected) - 1
            print('Would dispatch ' + title)
        else:
            # Do not retry an ambiguous/failed POST: later sweeps audit actual runs.
            response = api('control', WORKFLOW + '/dispatches', dict(ref='main', inputs=task))
            run_id = response['workflow_run_id']
            if type(run_id) is not int or run_id < 1:
                raise ValueError('Dispatch acknowledgment is missing its run ID')
            print(f'Dispatched {title}; run {run_id}')
        reservations.append(dict(id=run_id, display_title=title, status='queued', run_attempt=1,
                                 updated_at=datetime.fromtimestamp(current_time(), timezone.utc).isoformat()))
        selected.append(task)
        if len(selected) == 2:
            break
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true')
    options = parser.parse_args()
    tokens = dict(board=os.environ['GH_TOKEN'], repo=os.environ['GH_REPO_TOKEN'],
                  control=os.environ['GH_CONTROL_TOKEN'])
    if not all(tokens.values()):
        raise ValueError('Board, repository and private dispatch credentials are required')

    def api(kind, path, data=None):
        if kind == 'control' and not path.startswith(f'repos/{CONTROL}/actions/'):
            raise ValueError('Private credential is restricted to Actions')
        return request(tokens[kind], path, data)

    api_cards = cards(api)
    dispatch_cards(api_cards, api, options.dry_run)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'Dispatch stopped safely ({type(error).__name__}); no automatic request retry.')
        raise SystemExit(1)

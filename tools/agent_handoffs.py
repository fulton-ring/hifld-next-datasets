"""Read explicit, writer-authored issue/PR and head-bound review handoffs."""

import re
import urllib.parse

REPO = 'fulton-ring/hifld-next-datasets'


def pages(api, path):
    result = []
    for page in range(1, 11):
        batch = api('repo', f'{path}?per_page=100&page={page}')
        result.extend(batch)
        if len(batch) < 100:
            return result
    raise ValueError('Issue history exceeds audit bound')


def read_handoffs(issue, api):
    association = re.compile(r'<!-- hifld-agent-pr:v1 issue=' + str(issue) + r' pr=([1-9][0-9]*) -->')
    route = re.compile(r'<!-- hifld-agent-route:v1 issue=' + str(issue)
                       + r' pr=([1-9][0-9]*) head=([a-f0-9]{40}) label=(move:ready|move:needs-decision) -->')
    state, permissions = dict(pr=None, route=None), {}
    for comment in pages(api, f'repos/{REPO}/issues/{issue}/comments'):
        body = comment.get('body') or ''
        selected, requested = association.search(body), route.search(body)
        login = (comment.get('user') or {}).get('login')
        if not (selected or requested) or not login:
            continue
        if login not in permissions:
            permissions[login] = api('repo', f'repos/{REPO}/collaborators/{urllib.parse.quote(login, safe="")}/permission')['permission']
        if permissions[login] not in ('write', 'admin'):
            continue
        if selected:
            state = dict(pr=int(selected[1]), route=None)
        if requested:
            state['route'] = dict(pr=int(requested[1]), head=requested[2], label=requested[3])
    return state


def validate_route(issue, label, api):
    # Always honor parking/human-stop requests. Head binding is required before
    # Ready can authorize another implementation, not to prevent a safe stop.
    if label != 'move:ready':
        return True
    handoff = read_handoffs(issue, api)
    route = handoff['route']
    if route is None or route['label'] != label:
        return True
    if route['pr'] != handoff['pr']:
        return False
    pr = api('repo', f"repos/{REPO}/pulls/{route['pr']}")
    return (pr['state'] == 'open' and pr['head']['sha'] == route['head']
            and pr['head'].get('repo', {}).get('full_name') == REPO
            and pr['base']['repo']['full_name'] == REPO)

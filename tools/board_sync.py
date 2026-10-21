"""Consume move:* issue labels into Project Status, as in agent-content-demos.

This synchronizes cards only. It never dispatches workers or approves releases.
"""

import argparse
import json
import os
import re
import urllib.request

# Same conflict rule as agent-content-demos: decisions win, then review, then ready.
MOVE_LABELS = {
    'move:needs-decision': 'Needs decision',
    'move:review': 'Review',
    'move:in-progress': 'In progress',
    'move:ready': 'Ready',
    'move:backlog': 'Backlog',
}
FIELDS = '''query($owner:String!,$number:Int!){organization(login:$owner){projectV2(number:$number){
 id status:field(name:"Status"){... on ProjectV2SingleSelectField{id options{id name}}}
}}}'''
ITEMS = '''query($project:ID!,$cursor:String){node(id:$project){... on ProjectV2{
 items(first:100,after:$cursor){pageInfo{hasNextPage endCursor} nodes{id content{... on Issue{
 id number repository{nameWithOwner} labels(first:100){totalCount nodes{id name}}
 }}}}
}}}'''
SET_STATUS = '''mutation($project:ID!,$item:ID!,$field:ID!,$value:String!){
 updateProjectV2ItemFieldValue(input:{projectId:$project,itemId:$item,fieldId:$field,
 value:{singleSelectOptionId:$value}}){projectV2Item{id}}}'''

REMOVE_LABELS = 'mutation($issue:ID!,$labels:[ID!]!){removeLabelsFromLabelable(input:{labelableId:$issue,labelIds:$labels}){clientMutationId}}'


def requested_move(labels):
    selected = [label for label in MOVE_LABELS if label in labels]
    return (MOVE_LABELS[selected[0]] if selected else None), selected


def request(token, path, data=None, method=None):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(
        'https://api.github.com/' + path, data=body, method=method,
        headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json',
                 'Accept': 'application/vnd.github+json', 'User-Agent': 'hifld-board-sync'},
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        payload = response.read()
    return json.loads(payload) if payload else None


def apply_move(graphql, remove_labels, project, field, options, item, labels, repo, dry_run=False):
    status, consumed = requested_move(labels)
    if status is None:
        return
    if status not in options:
        raise ValueError('Project Status option is missing: ' + status)
    print(f"{'Would move' if dry_run else 'Move'} {repo}#{item['number']} -> {status}")
    if dry_run:
        return
    graphql(SET_STATUS, dict(project=project, item=item['id'], field=field, value=options[status]))
    # Keep the request if the board write failed; remove only labels observed above.
    remove_labels(item['issue_id'], [labels[name] for name in consumed])


def synchronize(graphql, remove_labels, owner, number, repo, issue=None, dry_run=False):
    project = graphql(FIELDS, dict(owner=owner, number=number))['organization']['projectV2']
    if not project or not project.get('status'):
        raise ValueError('Project or Status field is inaccessible')
    options = {x['name']: x['id'] for x in project['status']['options']}
    cursor = None
    found = issue is None
    for _ in range(10):
        page = graphql(ITEMS, dict(project=project['id'], cursor=cursor))['node']['items']
        for item in page['nodes']:
            content = item.get('content') or {}
            if content.get('repository', {}).get('nameWithOwner') != repo:
                continue
            if issue is not None and content['number'] != issue:
                continue
            found = True
            if content['labels']['totalCount'] > 100:
                raise ValueError('Issue labels exceed the bounded read; requests preserved')
            labels = {x['name']: x['id'] for x in content['labels']['nodes']}
            apply_move(graphql, remove_labels, project['id'], project['status']['id'], options,
                       dict(id=item['id'], issue_id=content['id'], number=content['number']), labels, repo, dry_run)
        if not page['pageInfo']['hasNextPage']:
            if not found:
                raise ValueError('Issue is not on the configured Project; request preserved')
            return
        cursor = page['pageInfo']['endCursor']
    raise ValueError('Project exceeds the 1000-item audit bound')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--issue', type=int)
    args = parser.parse_args()
    if args.issue is not None and args.issue < 1:
        parser.error('--issue must be positive')
    repo = os.environ.get('REPO', 'fulton-ring/hifld-next-datasets')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo):
        raise ValueError('Invalid repository')
    board_token = os.environ['GH_TOKEN']
    repo_token = os.environ['GH_REPO_TOKEN']
    if not board_token or not repo_token:
        raise ValueError('AGENT_BOARD_TOKEN and repository token are required')

    def graphql(query, variables):
        result = request(board_token, 'graphql', dict(query=query, variables=variables))
        if result.get('errors'):
            raise RuntimeError('GitHub Projects request failed; check token access')
        return result['data']

    def remove_labels(issue, labels):
        result = request(repo_token, 'graphql', dict(query=REMOVE_LABELS, variables=dict(issue=issue, labels=labels)))
        if result.get('errors'):
            raise RuntimeError('Issue label removal failed; remaining requests will be retried')

    synchronize(graphql, remove_labels, os.environ.get('PROJECT_OWNER', 'fulton-ring'),
                int(os.environ.get('PROJECT_NUMBER', '2')), repo, args.issue, args.dry_run)


if __name__ == '__main__':
    main()

"""Validate the immutable PR tuple before and after untrusted CI execution.

Only run from a trusted main checkout. Never import or execute PR code here.
"""

import json
import os
import re
from urllib.request import Request, urlopen


def validate(pr: dict, repository: str, number: str, head: str, base: str) -> dict:
    """Reject stale heads/bases, forks, closed PRs and noncanonical inputs."""
    if not re.fullmatch(r"[1-9][0-9]*", number):
        raise ValueError("invalid PR number")
    if not all(re.fullmatch(r"[a-f0-9]{40}", sha) for sha in (head, base)):
        raise ValueError("expected exact lowercase commit SHAs")
    if pr.get("number") != int(number) or pr.get("state") != "open":
        raise ValueError("PR identity/state changed")
    for side, sha in (("head", head), ("base", base)):
        data = pr.get(side) or {}
        if (
            data.get("sha") != sha
            or (data.get("repo") or {}).get("full_name") != repository
        ):
            raise ValueError(f"PR {side} SHA/repository changed")
    if pr["base"].get("ref") != "main":
        raise ValueError("only main-targeted PRs are eligible")
    return {"pr_number": number, "head_sha": head, "base_sha": base}


def main() -> None:
    repository = os.environ["GITHUB_REPOSITORY"]
    number, head, base = (
        os.environ[key] for key in ("PR_NUMBER", "HEAD_SHA", "BASE_SHA")
    )
    if os.environ["GITHUB_SHA"] != base:
        raise ValueError("trusted workflow snapshot differs from requested base")
    # Validate all path components before making an authenticated request.
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("invalid repository")
    if not re.fullmatch(r"[1-9][0-9]*", number):
        raise ValueError("invalid PR number")
    request = Request(
        f"https://api.github.com/repos/{repository}/pulls/{number}",
        headers={
            "Authorization": "Bearer " + os.environ["GH_TOKEN"],
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urlopen(request, timeout=30) as response:
        values = validate(json.load(response), repository, number, head, base)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        for key, value in values.items():
            print(f"{key}={value}", file=output)
    print(f"Validated PR #{number} head {head} base {base}")


if __name__ == "__main__":
    main()

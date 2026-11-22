"""Resumable acquisition over a producer's observed dense numeric population."""
from dagster_hifld.http_capture import capture_retention_budget


def capture_observed_count_population(*, root, byte_budget, receipts, opening,
                                      observed_count, before_requests, page_count,
                                      page_request, closing_requests, fetch,
                                      checkpoint):
    """Retain opening witnesses, numeric windows and fresh closing witnesses.

    The count callback reads the retained opening witness, including on resume.
    Successful opening, roster and page requests checkpoint the reusable prefix;
    only page failures annotate that prefix. Closing witnesses are always fresh
    and never replace the reusable checkpoint.
    """
    def acquire(name, url, *, page=False, resume=True):
        if resume and name in receipts:
            return
        try:
            _body, receipt = fetch(name, url)
        except Exception as exc:
            if page:
                checkpoint(receipts, type(exc).__name__)
            raise
        receipts[name] = receipt
        if resume:
            checkpoint(receipts, None)

    with capture_retention_budget(root, byte_budget):
        for name, url in opening:
            acquire(name, url)
        count = observed_count()
        for name, url in before_requests(count):
            acquire(name, url)
        for index in range(page_count(count)):
            name, url = page_request(count, index)
            acquire(name, url, page=True)
        for name, url in closing_requests(count):
            acquire(name, url, resume=False)
    return receipts

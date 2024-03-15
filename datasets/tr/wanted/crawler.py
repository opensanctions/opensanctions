from zavod import Context
from zavod import helpers as h

def crawl_row(context: Context, row):
    print(row)

def crawl(context):
    headers = {
        'Content-Length': '0',
        'Content-Type': 'application/json',
    }
    res = context.http.post(
        context.dataset.data.url,
        headers=headers
    )
    data = res.json()

    for row in data.get("content"):
        crawl_row(context, row)

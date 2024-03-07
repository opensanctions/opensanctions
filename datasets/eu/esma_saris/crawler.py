from zavod import Context


def crawl(context: Context) -> None:
    context.fetch_html(context.dataset.url)
    source_file = context.fetch_resource(
        "source.csv", context.dataset.data.url, cookies={"criteria": "[]"}
    )

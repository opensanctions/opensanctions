import logging
from pathlib import Path
from playwright.async_api import Page, CDPSession
import asyncio
from base64 import standard_b64decode

logger = logging.getLogger(__name__)


async def click_and_download(
    page: Page, client: CDPSession, selector: str, url_pattern: str, path: Path
):
    """
    Click an element to initiate a download, then download the matching file
    to the given path.

    This is useful for cases where the download is triggered by javascript and
    difficult to trigger without a full browser.

    A remote browser downloads the file locally, so we use Chrome DevTools Protocol
    Fetch.enable to intercept the download request, pause it, then initiate a stream
    to continue the request and download the response body to this client.

    Args:
        page: The Playwright page to interact with
        client: The Chrome DevTools Protocol client
        selector: The CSS selector of the element to click to trigger download
        url_pattern: https://chromedevtools.github.io/devtools-protocol/tot/Fetch/#type-RequestPattern
        path: The path to save the downloaded file to
    """

    if path.exists():
        logger.info(f"File already exists: {path}")
        return

    future = asyncio.get_running_loop().create_future()

    def on_request_paused(event):
        if not future.done():
            future.set_result(event["requestId"])

    def on_done(click: asyncio.Future):
        if not future.done() and click.exception():
            future.set_exception(click.exception())

    # Pause matching requests until we handle them
    await client.send(
        "Fetch.enable",
        {
            "patterns": [
                {
                    "requestStage": "Response",
                    "resourceType": "Document",
                    "urlPattern": url_pattern,
                }
            ],
        },
    )

    # Add handler for paused requests and completion
    client.on("Fetch.requestPaused", on_request_paused)
    asyncio.ensure_future(page.click(selector)).add_done_callback(on_done)
    request_id = await future

    stream = await client.send(
        "Fetch.takeResponseBodyAsStream",
        {"requestId": request_id},
    )
    with open(path, "wb") as outfile:
        while True:
            chunk = await client.send("IO.read", {"handle": stream["stream"]})
            if chunk["base64Encoded"]:
                data = standard_b64decode(chunk["data"])
            else:
                data = bytes(chunk["data"], "utf8")
            outfile.write(data)
            if chunk["eof"]:
                break

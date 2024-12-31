from playwright.async_api import Page, CDPSession
import asyncio
from base64 import standard_b64decode

async def click_and_download(page: Page, client: CDPSession, selector, url_pattern: str, path: str):
    """
    Initiate a download and return a future whose result is the request ID of the download
    to stream the response.

    A remote browser downloads the file locally, so we use Chrome DevTools Protocol
    Fetch.enable to intercept the download request, pause it, then initiate a stream
    to continue the request and download the response body to this client.
    """
    future = asyncio.get_running_loop().create_future()

    def on_request_paused(event):
        if not future.done():
            from pprint import pprint
            pprint(event)
            future.set_result(event['requestId'])

    def on_done(click: asyncio.Future):
        if not future.done() and click.exception():
            future.set_exception(click.exception())

    await client.send('Fetch.enable', {
        'patterns': [{
            'requestStage': 'Response',
            'resourceType': 'Document',
            'urlPattern': url_pattern,
        }],
    })
    client.on('Fetch.requestPaused', on_request_paused)
    asyncio.ensure_future(page.click(selector)).add_done_callback(on_done)
    request_id = await future

    stream = await client.send('Fetch.takeResponseBodyAsStream', {
        'requestId': request_id,
    })
    with open(path, 'wb') as outfile:
        while True:
            chunk = await client.send('IO.read', {
                'handle': stream['stream'],
            })
            if chunk['base64Encoded']:
                data = standard_b64decode(chunk['data'])
            else:
                data = bytes(chunk['data'], 'utf8')
            outfile.write(data)
            if chunk['eof']:
                break

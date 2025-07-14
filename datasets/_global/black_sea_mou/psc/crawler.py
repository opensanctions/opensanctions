from zavod import Context
from zavod.shed.gpt import run_image_prompt

prompt = """
You are an advanced OCR model specialized in reading small, low-resolution CAPTCHA images containing exactly 5 digits.

Image characteristics:
- Digits are blue or black on a white background.
- Digits may be distorted, overlapping, or have intersecting outer strokes.
- Each digit is formed by outer lines and may contain white spaces (holes) inside (e.g., 0, 4, 6, 8, 9).
- No letters or other symbols are present.

Your task:
- Extract the exact 5-digit sequence as accurately as possible.
- Use visual cues like digit shapes, outer contours, loops, and internal white spaces to resolve overlaps or distortions.
- If a digit is ambiguous, infer the most likely digit based on shape and continuity.
- Return **only** the 5-digit string in valid JSON format:

```json
{
  "digits": "12345"
}```

If unsure — return a blank string.
"""

HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": "https://bsis.bsmou.org/public/?button=Agree",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://bsis.bsmou.org",
}


def crawl(context: Context):
    # Submit login form
    login_page = context.fetch_html("https://bsis.bsmou.org/public/?button=Agree")
    login_page.make_links_absolute(context.data_url)
    # Solve the CAPTCHA
    captcha = login_page.xpath('//img[contains(@src, "captcha.php")]/@src')
    if not captcha:
        context.log.warn("No CAPTCHA image found on the login page")
    captcha_path = context.fetch_resource("captcha.png", captcha[0])

    result = run_image_prompt(context, prompt, captcha_path, cache_days=0)
    if result is None:
        context.log.warn("No result from CAPTCHA solver")
        return
    answer = result.get("digits")
    print(answer)

    login_data = {"captcha": answer}
    login_url = "https://bsis.bsmou.org/public/?action=login"
    login_resp = context.fetch_html(
        login_url, data=login_data, headers=HEADERS, method="POST"
    )
    assert login_resp is not None, "Login failed, response is None"

    # total_pages = None
    # page = 0
    # while total_pages is None or page < total_pages:
    # total_pages = crawl_list_page(context, page)
    # page += 1

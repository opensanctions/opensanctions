import re
import json

from zavod import Context


def crawl(context: Context):
    # ServiceNow table widget requires X-UserToken which is set as global variable
    # using a script tag in the page, and JSESSIONID cookie which is set in response
    # to page request.
    # BigIP cookie is also important since session seems to be origin-specific.

    # Set cookies and fetch token
    doc = context.fetch_html(
        "https://www.pmddtc.state.gov/ddtc_public/ddtc_public?id=ddtc_kb_article_page&sys_id=384b968adb3cd30044f9ff621f961941"
    )
    page_data = doc.find(
        ".//script[@data-description='NOW vars set by properties, custom events, g_* globals']"
    )
    javascript_vars = page_data.text_content()
    token = re.search("window.g_ck = '(\w+)';", javascript_vars).groups(1)[0]

    request_data = {
        "sys_id": "384b968adb3cd30044f9ff621f961941",
        "preventViewAll": True,
        "workflow_state": "published",
        "field_list": "year,company,description,charging_letter,consent_agreement,order",
        "table": "x_usd10_ddtc_publi_ddtc_public_penalty_and_oversight",
        "order": 1,
        "sys_view_count": "0",
        "d": "DESC",
        "active": True,
        "fields_array": [
            "year",
            "company",
            "description",
            "charging_letter",
            "consent_agreement",
            "order",
        ],
        "kb_knowledge_page": "ddtc_kb_article_page",
        "o": "year",
        "sp_widget_dv": "PWS DDTC Public Portal KB",
        "kb_knowledge_base": "34a3eed6db2f0300d0a370131f961983",
        "category": "ba6b12cadb3cd30044f9ff621f961981",
        "sp_column": "b8037ac41b0ec950055b9796bc4bcb86",
        "sys_class_name": "sp_instance",
        "sp_widget": "dbd0af311b58b450055b9796bc4bcb2d",
    }

    headers = {"X-UserToken": token}
    res = context.http.post(
        "https://www.pmddtc.state.gov/api/now/sp/widget/844682081bbbc550055b9796bc4bcb3d?id=ddtc_kb_article_page&sys_id=384b968adb3cd30044f9ff621f961941",
        data=json.dumps(request_data),
        headers=headers,
    )
    print(res.json())

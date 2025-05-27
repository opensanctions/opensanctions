---
title: Data review reminder for {{ date | date('MMMM YYYY') }}
assignees: nvmbrasserie
labels: daily_issues
---
Monthly maintenance cycle for {{ date | date('MMMM YYYY') }}

## Manual Maintenance — Data Review

Please manually check and update the **data** for the following curated crawlers:

{{ env.DATASETS }}
---

*Automated reminder to ensure data accuracy and freshness for manually maintained crawlers.*
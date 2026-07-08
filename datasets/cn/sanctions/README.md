# China sanctions data

`sanctions.csv` is a manually maintained record of sanctions and related trade
restrictions imposed by Chinese government authorities. Changes to the data should
be submitted as pull requests.

## Contributing designations

Each row represents a designation. The same person or entity may have multiple rows
when they were designated under different measures or on different dates.
Duplicate listings are not a problem: entity records are merged during processing,
while the individual rows preserve the source and details of each designation.

Every designation must cite an official source in `Source URL`. Suitable sources
include notices published by the Ministry of Foreign Affairs, Ministry of Commerce,
the Taiwan Affairs Office, or another Chinese government authority. Secondary
reporting can provide context, but it is not a substitute for the official notice.

When adding a designation:

- Preserve the existing CSV columns and add one row per designation.
- Use the name and designation date stated in the official notice.
- Identify the issuing authority in `Body` and the applicable measure in `List`.
- Use `DD.MM.YYYY` for dates.
- Include the official notice URL in `Source URL`.
- Keep the pull request focused and explain any interpretation or translation needed
  to turn the notice into structured data.

## Removed source pages are not delistings

An official notice becoming unavailable or being removed from a government website
is not evidence that the designation has ended. Do not remove a row or clear its
`Source URL` solely because the URL no longer resolves. Retain the original URL as
the citation unless the same notice has moved to a new official URL.

Only remove a designation or set an `End date` when an official source explicitly
revokes the measure or establishes its expiry. Cite that evidence in the pull
request.

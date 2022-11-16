# Delta generating-script for entity data

This is a prototype format for publishing deltas between OpenSanctions entity data from day to day. The idea is that there's four modification types:

* "ADDED" - the entity is in the new data but not the old data
* "REMOVED" - the entity is in the old data but not the new data
* "MODIFIED" - the content checksum for the new and old data is different
* "MERGED" - the entity was merged with a different one and will no longer appear (the new merged entity will show either as "ADDED" or "MODIFIED")

Each line in the delta file contains a JSON object with the modification op code and the latest available version of the entity body (i.e. today's for added/modified, and yesterday's for merged and removed).
# def _prefix(*parts):
#     return jointext(*parts, sep=".")


# def flatten_row(nested, prefix=None):
#     yield (_prefix(prefix, "id"), nested.get("id"))
#     yield (_prefix(prefix, "schema"), nested.get("schema"))
#     yield (_prefix(prefix, "target"), nested.get("target"))
#     for prop, values in nested.get("properties").items():
#         for idx, value in enumerate(values):
#             prop_prefix = _prefix(prefix, prop, idx)
#             if is_mapping(value):
#                 yield from flatten_row(value, prefix=prop_prefix)
#             else:
#                 yield (prop_prefix, value)


# wide_path = context.get_resource_path("wide.csv")
# wide_path.parent.mkdir(exist_ok=True, parents=True)
# context.log.info("Writing targets to wide-format CSV", path=wide_path)
# with open(wide_path, "w", encoding=settings.ENCODING) as fh:
#     writer = csv.writer(fh, dialect=csv.unix_dialect)
#     writer.writerow(list(columns))
#     for entity in entities.values():
#         if not entity.target:
#             continue
#         data = nested_entity(entity, entities, inverted, [])
#         data = dict(flatten_row(data))
#         row = [data.get(c) for c in columns]
#         writer.writerow(row)

# title = "List of targets with details"
# context.export_resource(wide_path, mime_type="text/csv", title=title)

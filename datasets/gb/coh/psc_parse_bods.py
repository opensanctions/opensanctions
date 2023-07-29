from zavod import init_context

from common.bods import parse_file

if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        fn = context.get_resource_path("source.json")
        context.export_metadata("export/index.json")
        parse_file(context, fn)

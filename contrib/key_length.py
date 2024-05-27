import orjson

from followthemoney import model
from followthemoney.types import registry


def check_id_lengths():
    with open("entities.ftm.json", "rb") as fh:
        for line in fh:
            entity = model.get_proxy(orjson.loads(line))
            if entity.id is None:
                continue
            if not entity.id.startswith("ua-nsdc-10048"):
                continue
            bin_id = entity.id.encode("utf-8")
            print(bin_id, len(bin_id))
            if len(bin_id) > 255:
                print(
                    f"ID too long: {entity.id} ({entity.schema.name}, {entity.caption})"
                )
            # for prop in entity.schema.properties.values():
            #     if prop.type == registry.entity:
            #         for value in entity.get(prop):
            #             if len(value) > 255:
            #                 print(f"ID too long: {value}")


if __name__ == "__main__":
    check_id_lengths()

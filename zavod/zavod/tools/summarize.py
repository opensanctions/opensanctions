from typing import List, Optional

from zavod.store import View


def summarize(
    view: View,
    schema: Optional[str],
    from_prop: Optional[str],
    link_props: List[str],
    to_prop: Optional[str],
    to_props: List[str],
) -> None:
    """Print a summary of the entities in a view.

    e.g.
    ```
    Martina Gießübel
      link: startDate: ['2023-10-26'] endDate: ['2028-10-31']
      Member of the Landtag Bayern
        entity: country: ['de']
    ```
    or
    ```
    Wayne Panton
      Minister for Finance & Economic Development
      Minister for Sustainability & Climate Resiliency
      Premier
    ```
    or
    ```
    Margarita Nikolaevna Kotova
      link: relationship: ['personal relationships']
      Vladimir Alekseevich Gordeev
      link: relationship: ['business relationships']
      Evgeniy Yur'yevich Vladimirov
    ```
    """
    for from_entity in view.entities():
        if schema is None or from_entity.schema.is_a(schema):
            print(from_entity.caption)
            for prop_from, link_entity in view.get_adjacent(from_entity):
                if prop_from.name == from_prop:
                    link_summary = "  link: "
                    for prop_name in link_props:
                        link_summary += f"{prop_name}: {link_entity.get(prop_name)} "
                    if link_props:
                        print(link_summary)
                    for prop_to, to_entity in view.get_adjacent(link_entity):
                        if prop_to.name == to_prop:
                            print(f"  {to_entity.caption}")
                            to_summary = "    "
                            for prop_name in to_props:
                                to_summary += (
                                    f"{prop_name}: {to_entity.get(prop_name)} "
                                )
                            if to_props:
                                print(to_summary)

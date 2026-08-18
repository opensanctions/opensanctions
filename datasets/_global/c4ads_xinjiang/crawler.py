import csv

from zavod import Context
from zavod import helpers as h
from zavod.shed.internal_data import fetch_internal_data


def crawl(context: Context) -> None:
    path = context.get_resource_path("source.csv")
    fetch_internal_data("c4ads_xinjiang/xpcc_public_dissemination.csv", path)

    with open(path) as fh:
        for row in csv.DictReader(fh):
            name_zho = row.pop("Company_Name_Chinese")
            addr_zho = row.pop("Registered_Address")
            # The owned side of each row is a registered business, so it needs to be a
            # Company: Organization is not an Asset and can't be an Ownership:asset.
            entity = context.make("Company")
            entity.id = context.make_id(name_zho)
            entity.add("name", name_zho, lang="zho")
            entity.add("name", row.pop("Company_Name_English"), lang="eng")
            entity.add("country", "cn")
            h.apply_date(
                entity,
                "incorporationDate",
                row.pop("Date_of_Establishment"),
                # The XPCC was founded in 1954, so no company is older.
                two_digit_year_base=1950,
            )
            entity.add("sector", row.pop("Industry"), lang="zho")
            entity.add("address", addr_zho, lang="zho")
            entity.add("topics", "export.risk")
            entity.add("topics", "forced.labor")
            context.emit(entity)

            # Shareholders include XPCC divisions and state asset supervision
            # commissions besides companies, so they stay Organizations. The ones which
            # are companies are emitted as such where they show up as an owned entity.
            owner = context.make("Organization")
            owner_name = row.pop("Shareholding_Company_Name")
            if owner_name == name_zho:
                context.log.warning("Same owner and owned", name_zho=name_zho)
                continue
            owner.id = context.make_id(owner_name)
            owner.add("name", owner_name, lang="zho")
            owner.add("country", "cn")
            context.emit(owner)

            own = context.make("Ownership")
            own.id = context.make_id("ownership", entity.id, owner.id)
            own.add("owner", owner)
            own.add("asset", entity)
            own.add("percentage", row.pop("Investment_Ratio"), lang="zho")
            own.add("sharesValue", row.pop("Investment_Amount"), lang="zho")
            context.emit(own)

            context.audit_data(row)

    # xpcc = context.make("Organization")
    # xpcc.id = context.make_id("XPCC")
    # xpcc.add("name", "Xinjiang Production and Construction Corps (XPCC)", lang="eng")
    # xpcc.add("name", "新疆生产建设兵团", lang="zho")
    # context.emit(xpcc)
    # for owner_name in owned_names:
    #     print(owner_name, owner_name in owned_names)
    #     if owner_name not in owned_names:
    #         apex_id = context.make_id(owner_name)
    #         print(owner_name)
    #         own = context.make("Ownership")
    #         own.id = context.make_id("ownership", apex_id, xpcc.id)
    #         own.add("owner", xpcc)
    #         own.add("asset", owner)
    #         context.emit(own)

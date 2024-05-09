import re

from zavod import Context, helpers as h

EMAIL_PATTERN = r"[\w.+-]+@[\w-]+\.[\w.-]+(?<!\.)"


def build_address(input_dict: dict, context: Context):
    address = context.make("Address")
    if "adresse" in input_dict:
        address.id = context.make_id(
            input_dict["adresse"],
            input_dict["ville"],
            input_dict["pays"],
            input_dict["codePostal"],
        )
        address.add("street", input_dict.pop("adresse"))
    else:
        address.id = context.make_id(
            input_dict["ville"], input_dict["pays"], input_dict["codePostal"]
        )
    address.add("city", input_dict.pop("ville"))
    address.add("country", input_dict.pop("pays"))
    address.add("postalCode", input_dict.pop("codePostal"))
    context.emit(address)

    return address


def crawl_item(input_dict: dict, context: Context):
    organization = context.make("Organization")
    organization.id = context.make_slug(input_dict["denomination"])
    organization.add("name", input_dict.pop("denomination"))
    organization.add(
        "classification", input_dict.pop("categorieOrganisation").pop("label")
    )

    organization.add("registrationNumber", input_dict.pop("identifiantNational"))
    organization.add("name", input_dict.pop("nomUsage", None))
    organization.add("name", input_dict.pop("nomUsageHatvp", None))
    organization.add("name", input_dict.pop("ancienNomHatvp", None))
    organization.add("phone", input_dict.pop("telephoneDeContact", None))
    organization.add("website", input_dict.pop("lienSiteWeb", None))
    organization.add("website", input_dict.pop("lienPageLinkedin", None))
    organization.add("website", input_dict.pop("lienPageTwitter", None))
    organization.add("website", input_dict.pop("lienPageFacebook", None))
    organization.add("website", input_dict.pop("lienListeTiers", None))

    if "emailDeContact" in input_dict:
        match = re.findall(EMAIL_PATTERN, input_dict.pop("emailDeContact"))
        organization.add("email", match)

    address = build_address(input_dict, context)
    organization.add("address", address)

    for director in input_dict.pop("dirigeants"):
        person = context.make("Person")
        person.id = context.make_id(director["prenom"], director["nom"])
        h.apply_name(
            person, first_name=director.pop("prenom"), last_name=director.pop("nom")
        )
        directorship = context.make("Directorship")
        directorship.id = context.make_id(person.id, organization.id)
        directorship.add("organization", organization)
        directorship.add("director", person)

        if "fonction" in director:
            directorship.add("description", director.pop("fonction"), lang="fr")

        context.emit(person)
        context.emit(directorship)

    for employer in input_dict.pop("collaborateurs"):
        person = context.make("Person")
        person.id = context.make_id(employer["prenom"], employer["nom"])
        h.apply_name(
            person, first_name=employer.pop("prenom"), last_name=employer.pop("nom")
        )

        employment = context.make("Employment")
        employment.id = context.make_slug(person.id, organization.id)
        employment.add("employer", organization)
        employment.add("employee", person)

        if "fonction" in employer:
            employment.add("description", employer.pop("fonction"), lang="fr")

        context.emit(person)
        context.emit(employment)

    for sector in input_dict.pop("activites")["listSecteursActivites"]:
        organization.add("sector", sector.pop("label"))

    context.emit(organization)
    context.audit_data(
        input_dict,
        ignore=[
            "publierMonAdressePhysique",
            "publierMonTelephoneDeContact",
            "publierMonAdresseEmail",
            "typeIdentifiantNational",
            "declarationTiers",
            "clients",
            "declarationOrgaAppartenance",
            "affiliations",
            "isActivitesPubliees",
            "datePremierePublication",
            "dateCreation",
            "exercices",
            "dateDernierePublicationActivite",
            "dateCessation",
            "motifDesinscription",
            "motivationDesinscription",
            "sigleHatvp",
        ],
    )


def crawl(context: Context):
    response = context.fetch_json(context.data_url)

    for item in response["publications"]:
        crawl_item(item, context)

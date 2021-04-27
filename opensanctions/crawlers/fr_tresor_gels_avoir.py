import json

SCHEMATA = {
    "Personne physique": "Person",
    "Personne morale": "Company",
    "Navire": "Vessel",
}


def format_date(value):
    date = value.pop("Annee")
    month = value.pop("Mois")
    if len(month):
        date = f"{date}-{month}"
        day = value.pop("Jour")
        if len(day):
            date = f"{date}-{day}"
    return date


def apply_prop(entity, sanction, field, value):
    if field == "ALIAS":
        entity.add("alias", value.pop("Alias"))
    elif field == "SEXE":
        entity.add("gender", value.pop("Sexe"))
    elif field == "PRENOM":
        entity.add("firstName", value.pop("Prenom"))
    elif field == "NATIONALITE":
        entity.add("nationality", value.pop("Pays"))
    elif field == "TITRE":
        entity.add("position", value.pop("Titre"))
    elif field == "SITE_INTERNET":
        entity.add("website", value.pop("SiteInternet"))
    elif field == "TELEPHONE":
        entity.add("phone", value.pop("Telephone"))
    elif field == "COURRIEL":
        entity.add("email", value.pop("Courriel"))
    elif field == "NUMERO_OMI":
        entity.add("imoNumber", value.pop("NumeroOMI"))
    elif field == "DATE_DE_NAISSANCE":
        entity.add("birthDate", format_date(value))
    elif field in ("ADRESSE_PM", "ADRESSE_PP"):
        entity.add("country", value.pop("Pays"))
        entity.add("address", value.pop("Adresse"))
    elif field == "LIEU_DE_NAISSANCE":
        entity.add("birthPlace", value.pop("Lieu"))
        entity.add("country", value.pop("Pays"))
    elif field == "PASSEPORT":
        entity.add("passportNumber", value.pop("NumeroPasseport"))
    elif field == "IDENTIFICATION":
        comment = value.pop("Commentaire").lower()
        content = value.pop("Identification")
        if "swift" in comment:
            entity.add("swiftBic", content)
        elif "inn" in comment:
            entity.add("innCode", content)
        elif "fiscal" in comment or "taxe" in comment:
            entity.add("taxNumber", content)
        elif "arvada/arfada" in comment:
            pass
        else:
            entity.add("registrationNumber", content)
            # print((comment, content))
    elif field == "AUTRE_IDENTITE":
        entity.add("idNumber", value.pop("NumeroCarte"))
    elif field == "REFERENCE_UE":
        sanction.add("program", value.pop("ReferenceUe"))
    elif field == "REFERENCE_ONU":
        sanction.add("program", value.pop("ReferenceOnu"))
    elif field == "FONDEMENT_JURIDIQUE":
        sanction.add("reason", value.pop("FondementJuridiqueLabel"))
    elif field == "MOTIFS":
        sanction.add("reason", value.pop("Motifs"))
    # else:
    #     print(field, value)


def crawl_entity(context, data):
    nature = data.pop("Nature")
    schema = SCHEMATA.get(nature)
    entity = context.make(schema)
    entity.make_slug(data.pop("IdRegistre"))
    entity.add("name", data.pop("Nom"))

    sanction = context.make("Sanction")
    sanction.make_id("Sanction", entity.id)
    sanction.add("entity", entity)
    for detail in data.pop("RegistreDetail"):
        field = detail.pop("TypeChamp")
        for value in detail.pop("Valeur"):
            apply_prop(entity, sanction, field, value)

    context.emit(entity, target=True)


def crawl(context):
    context.fetch_artifact("source.json", context.dataset.data.url)
    path = context.get_artifact_path("source.json")
    with open(path, "r") as fh:
        data = json.load(fh)

    publications = data.get("Publications")
    # date = publications.get("DatePublication")
    for detail in publications.get("PublicationDetail"):
        crawl_entity(context, detail)

import csv
import shutil
from pathlib import Path

from rigour.mime.types import CSV, PDF

from zavod import Context, settings, helpers as h
from zavod.extract import zyte_api

LOCAL_PATH = Path(__file__).parent
PDF_URL = (
    "https://www.dans.bg/upload/298/RESHENIE_265_na_MS_ot_23042003_g_za_priemane"
    "_Spisyk_na_fiziceskite_lica_uridiceskite_lica_grupite_i_.pdf"
)
# SHA1 of the consolidated list PDF as last reviewed, with amendments through
# State Gazette no. 47 of 30.05.2023. A copy of that version is committed next
# to this crawler as dans_list.pdf; see the runbook comment in the yml.
PDF_HASH = "c05bfa98aa2037403fe7d4eda60f8e784e8a9087"

# The introductory sentence of each list section, verbatim from the decision.
SECTION_REASON = {
    "III": (
        "Лица, срещу които е образувано наказателно производство за тероризъм, "
        "финансиране на тероризъм, набиране или обучаване на отделни лица или групи "
        "от хора с цел извършване на тероризъм, излизане или влизане през границата "
        "на страната, както и незаконно пребиваване в нея с цел участие в тероризъм, "
        "образуване, ръководене или членуване в организирана престъпна група, която "
        "си поставя за цел да извършва тероризъм или финансиране на тероризъм, "
        "приготовление към извършване на тероризъм, подправка на официален документ "
        "с цел улесняване извършване на тероризъм, явно подбуждане към извършване на "
        "тероризъм или закана за извършване на тероризъм по смисъла на Наказателния "
        "кодекс"
    ),
    "IV": (
        "Лица, за които има достатъчно данни, че осъществяват дейност, свързана с "
        "тероризъм или с финансиране на тероризъм"
    ),
}


def crawl_row(context: Context, row: dict[str, str]) -> None:
    section = row.pop("Section")
    number = row.pop("Number")
    name_bg = row.pop("Name BG")
    entity = context.make(row.pop("Type"))
    entity.id = context.make_id(section, number, name_bg)
    entity.add("name", row.pop("Name"), lang="eng")
    entity.add("name", name_bg, lang="bul")
    entity.add("alias", row.pop("Alias").split(";"), lang="eng")
    entity.add("alias", row.pop("Alias BG").split(";"), lang="bul")
    entity.add("previousName", row.pop("Previous name").split(";"), lang="eng")
    entity.add("previousName", row.pop("Previous name BG").split(";"), lang="bul")
    h.apply_date(entity, "birthDate", row.pop("DOB"))
    entity.add("birthPlace", row.pop("POB"), lang="bul")
    country_prop = "citizenship" if entity.schema.is_a("Person") else "country"
    entity.add(country_prop, row.pop("Country"))
    entity.add("passportNumber", row.pop("Passport"))
    entity.add("idNumber", row.pop("ID Number"))
    entity.add("address", row.pop("Address"), lang="bul")
    entity.add("notes", row.pop("Notes"), lang="bul")

    sanction = h.make_sanction(
        context,
        entity,
        program_key="BG-MFT",
        start_date=row.pop("Listed"),
        end_date=row.pop("Delisted"),
    )
    sanction.add("reason", SECTION_REASON[section], lang="bul")
    sanction.add("summary", row.pop("Gazette"), lang="bul")
    if h.is_active(sanction):
        entity.add("topics", "sanction")
    context.emit(sanction)
    context.emit(entity)
    context.audit_data(row)


def check_list_pdf(context: Context) -> None:
    """Warn when the consolidated list PDF on dans.bg differs from the reviewed version.

    The list only changes through Council of Ministers amendment decisions
    promulgated in the State Gazette, after which DANS replaces the
    consolidated PDF. dans.bg blocks plain requests behind a Cloudflare
    challenge, so the document is fetched through the Zyte API with a
    Bulgarian geolocation.
    """
    if settings.ZYTE_API_KEY is None:
        context.log.info("Skipping list change detection: no Zyte API key configured")
        return
    _, _, _, path = zyte_api.fetch_resource(
        context, "dans_list.pdf", PDF_URL, expected_media_type=PDF, geolocation="BG"
    )
    if not h.assert_file_hash(path, PDF_HASH):
        context.log.warning("The consolidated list PDF has been amended", url=PDF_URL)


def crawl(context: Context) -> None:
    source_file = LOCAL_PATH / "sanctions.csv"
    resource_path = context.get_resource_path("source.csv")
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, context.SOURCE_TITLE)

    with open(source_file, encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)

    try:
        check_list_pdf(context)
    except Exception as exc:
        context.log.warning("List PDF change check failed", error=str(exc))

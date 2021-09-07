import structlog
from normality import normalize, WS
from opensanctions.core.index import Index

log = structlog.get_logger(__name__)


if __name__ == "__main__":
    index = Index("us_ofac_sdn")
    index.build()

    import pickle

    with open("ofac.pkl", "wb") as fh:
        pickle.dump(index, fh)

    # entity = model.make_entity("Person")
    # entity.add("name", "Saddam Hussein")
    # index.match(entity)

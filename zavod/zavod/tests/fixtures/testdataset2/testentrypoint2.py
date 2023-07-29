from zavod.context import Context


def crawl(context: Context):
    entity = context.make("Person")
    entity.id = "id"
    entity.name = "Friederich Bloggs"
    context.emit(entity, target=True)

    context.log.warn("Message in a bottle")
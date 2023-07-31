from zavod.context import Context


def crawl(context: Context):
    entity = context.make("Person")
    entity.id = "friedrich"
    entity.name = "Friedrich Bloggs"
    context.emit(entity, target=True)

    context.log.warn("Message in a bottle")
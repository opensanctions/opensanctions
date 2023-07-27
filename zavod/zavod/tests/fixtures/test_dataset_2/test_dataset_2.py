from zavod.context import Context


def crawl(context: Context):
    entity = context.make(schema)
    entity.id = "id"
    context.emit(entity)

    context.log.warn("Message in a bottle")
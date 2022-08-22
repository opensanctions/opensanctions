
def crawl(context):
    # this file is ods (open document)
    source_path = context.fetch_resource('eng-terrorismelijst.ods', context.dataset.data.url)
    # with open(source_path, 'r') as fh:
    #     print(len(fh.read()))
    

    # columns
    # starts at row 3 with columntitles
    # Surname
    # First name(s)
    # Alias
    # Date of Birth (DD-MM-JJJJ)
    # Place of Birth
    # Date of ministerial decision (DD-MM-JJJJ)
    # Link offical notification
    # link to offical notification
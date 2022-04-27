import json
from re import L

from opensanctions import settings


def check_raw(ident):
    prefix = "au-dfat-"
    if not ident.startswith(prefix):
        return False
    suffix = ident[len(prefix) :]
    try:
        int(suffix)
        return True
    except:
        return False


def filter_resolver():
    out_file = settings.RESOLVER_PATH.as_posix() + ".filtered"
    with open(settings.RESOLVER_PATH, "r") as infh:
        with open(out_file, "w") as outfh:
            while line := infh.readline():
                data = json.loads(line)
                if check_raw(data[0]):
                    print(data[0])
                    continue
                if check_raw(data[1]):
                    print(data[1])
                    continue
                # ts = data[5]
                # if ts > "2022-04-19":
                #     print(ts)
                #     continue
                outfh.write(line)


if __name__ == "__main__":
    filter_resolver()

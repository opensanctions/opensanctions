import json

from opensanctions import settings


def filter_resolver():
    out_file = settings.RESOLVER_PATH.as_posix() + ".filtered"
    with open(settings.RESOLVER_PATH, "r") as infh:
        with open(out_file, "w") as outfh:
            while line := infh.readline():
                data = json.loads(line)
                ts = data[5]
                if ts > "2022-04-19":
                    print(ts)
                    continue
                outfh.write(line)


if __name__ == "__main__":
    filter_resolver()

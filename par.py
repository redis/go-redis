import json

x = open("foo.json").readlines()
for line in x:
    ldata = json.loads(line)
    if ldata.get("Action") == "fail":
        print(ldata)

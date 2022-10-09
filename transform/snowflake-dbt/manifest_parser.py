import json

f = open('target/manifest.json')

# returns JSON object as
# a dictionary
data = json.load(f)


print(data)

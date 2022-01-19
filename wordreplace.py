# A simple code to replace multiple short forms with the full word
import re

texts = 'pls send us a dm..'
# replacewords = ['dm', 'dirm']
replacedict = {'direct message': ['dm', 'dirm'], 'please': ['pls', 'plz', 'pwease']}
for key in replacedict:
    for values in replacedict[key]:
        reg = values
        texts = re.sub(reg,key,texts)

print(texts)
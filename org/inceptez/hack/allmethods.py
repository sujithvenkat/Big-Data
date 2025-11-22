import re
def remspecialchar(given_string):
    return_string = re.sub("[-?,/_()\[\]()\-0-9\\\]","",given_string)
    return return_string

#print(remspecialchar('Pathway - 2\X (with dental)'))
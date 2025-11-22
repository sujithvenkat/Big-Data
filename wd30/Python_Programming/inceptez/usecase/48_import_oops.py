#from oops import *
#import oops

from oops import mask,endecode

inp_lst = ['arun', 'ram kumar', 'yoga murthy']
objmask = mask()
print("The hashmask function" , objmask.hashMask("arun"))
print(list(map(objmask.hashMask,inp_lst)))

objdecode = endecode()
return_list = list(map(objdecode.revEncode,inp_lst))
print(*return_list, sep=',')

print(list(map(objdecode.revDecode,return_list)))

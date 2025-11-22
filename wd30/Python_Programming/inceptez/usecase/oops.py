class mask:
    __addhash=100
    def hashMask(self,str):
        print(f"hash value of string {str}",hash(str))
        return mask.__addhash+hash(str)

class endecode:
    __prefixstr ='aix'
    #rev_str=[]
    #rev_string = ""
    def revEncode(self,str):
        print("Entered string is ", str)
        rev_string = ""
        for i in reversed(str):
            #endecode.rev_str.append(i)
            rev_string += i
        #print(*endecode.rev_str)
        print(rev_string)
        #print(list(reversed(str)))
        return endecode.__prefixstr + rev_string

    def revDecode(self,str1):
        print("Given rev string for decode is", str1)
        org_string = ""
        for i in reversed(str1.replace('aix','')):
            org_string += i
        print(org_string)
        return org_string

'''
mask_obj_mem = mask()
print(mask_obj_mem.hashMask("abc" + str(100)))
print(mask_obj_mem.hashMask("abc"))
#print(mask_obj_mem.hashMask("abc" + 100)) -> will throw error

endecode_obj = endecode()
print(endecode_obj.revEncode("Ganesh"))
'''





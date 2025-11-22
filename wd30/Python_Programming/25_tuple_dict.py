tuples_list = [("Inceptez","Technologies"),("Apple","Incorporation")]
dict_list = {}
print(type(dict_list))
dict_list = dict(tuples_list)

print(f"The value for Apple is {dict_list['Apple']}" )
print(f"Another way to get, now the value of Apple is {dict_list.get('Apple')}")
print("so far no error, lets try with the key which is not present in the dict")
#print(f"The value for Apple is {dict_list['Apple1']}" )  # KeyError: 'Apple1' and the program got failed
print(f"Another way to get, now the value of Apple is {dict_list.get('Apple1')}") # no error, just print None
print("no error and the program got success even the key is not present in the dict")
'''for i in dict_list:
    print(i)'''


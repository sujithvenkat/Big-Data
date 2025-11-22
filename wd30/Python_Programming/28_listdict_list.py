lst_dict = [{"Inceptez" : "technologies"},{"Apple":"Incorporation"}]
#think about using for loop, list() function, keys function and list append functions to achieve this.
lst1 = []
#print(lst_dict[0]["Inceptez"])
#print(lst_dict[0])
for i in lst_dict:
    #print(i.keys())
    #print(i.values())
    lst1.append(i.keys())
    #i.keys(lst1)   -- TypeError: keys() takes no arguments (1 given)

print(lst1[0])
print(lst1[1])
for i in lst1:
    print(i)



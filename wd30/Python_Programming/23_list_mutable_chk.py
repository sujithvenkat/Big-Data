range_lst = range(2, 11)
#print(range_lst) -- not a right way to print the values in the list
list_out = [1]
j=0
'''list_out[2] = 4
print(*list_out)
#list_out[3] = 5'''

for i in range_lst:
    print(i)     # -- right way to print the values in the list
    print("before" , j)
    print(*list_out)
    #list_out.append(i)
    list_out[j] = i
    j += 1
    print("after",j)

str = 'venkat'
str.r


'''
#print(list_out)

list_out[2] = 100
#list_out.index(4, 100)  --  ValueError: 4 is not in list
list_out.insert(4, 100)
print("After updating/mutable ", list_out)
'''
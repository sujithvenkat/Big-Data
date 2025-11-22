str1 = "Inceptez Technologies Pvt Ltd"
print(str1)
lst_str1 = list()
lst_str1=str1.split(' ')
'''for i in str1:
    #print(i)
    lst_str1.append(i)
'''
for i in lst_str1:
    print(i)
# to print the list in a single line
print(*lst_str1)
# to print the list in a single line separated by comma
print(*lst_str1, sep=', ')
# to print the list in a new line 
print(*lst_str1, sep = "\n")
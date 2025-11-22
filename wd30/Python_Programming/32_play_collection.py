emplst_lst= [["1", ("Arun","Kumar"), "10000"],["2", ("Bala","Mohan"), "12000"]]
print(type(emplst_lst))
rverse_lst=[]
tuple_first_elem = tuple(emplst_lst[0])
print(*tuple_first_elem)

print("Print the second element's second element and reverse the first and last name as given below")
print(*emplst_lst)
rverse_lst=reversed(emplst_lst[1][1])
print(*rverse_lst, sep=', ')

emplst_tup = tuple(emplst_lst)
print(*emplst_tup)
print(type(emplst_tup))
'''sum_s = 0
for i in emplst_lst:
    for j in i:
        sum_s = sum_s + j

print(sum_s)'''
sum_salary = int(emplst_lst[0][2]) + int(emplst_lst[1][2])
print(sum_salary)

lst1=[[10,20],[30,40,50],[60,70,80]]
result=0
max_lst=[]
min_lst=[]
#print(lst1[0][0])
print(sum(lst1[0]) + sum(lst1[1]) + sum(lst1[2]))
#uing sum function
for i in lst1:
    print(i)
    result = result + sum(i)


print("sum of all numbers",result)

#using nested for loop
for i in lst1:
    print(i)
    print(f"Max value of {i} is {max(i)}")
    max_lst.append(max(i))
    print(f"Min value of {i} is {min(i)}")
    min_lst.append(min(i))

print("Max Values in the max list ", max(max_lst))
print("Min Values in the min list ", min(min_lst))

init_val = 0
result = 0
for i in lst1:
    for j in i:
        result = result + j
        if j > init_val:
            init_val = j

print("the sum of all the values n the list ", result)
print("Max Values in the entire list ", init_val)

#sort the list to find the min and max value
unsorted_list = [10,50,20,60,30]
unsorted_cars = ['Ford', 'BMW', 'Volvo']
sorted_list = []
#print("The sorted list ", unsorted_list.sorted())
#unsorted_list.sort()
print("The sorted list ",sorted(unsorted_list))
print("The sorted list in numbers ", unsorted_list )
sorted_list = sorted(unsorted_list)
print("The sorted list in cars", unsorted_cars.sort())
print(unsorted_cars)

print("Max Values in the list ",sorted_list[len(sorted_list) - 1])
print("Min Values in the list ",sorted_list[0])









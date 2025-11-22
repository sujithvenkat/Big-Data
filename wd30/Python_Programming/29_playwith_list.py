lst=[10,20,40,30,20]
lst.sort()
print("sorted list after lst.sort()")
print("ascending order")
for i in lst:
    print(i)
print("descending order")
lst.sort(reverse=True)
for i in lst:
    print(i)
print("Max num in the list", max(lst))
print("Min num in the list", min(lst))
print("sum of all num in the list", sum(lst))

print("Remove 30 and 20 from the list")

lst.remove(30) #TypeError: remove() takes exactly one argument (2 given)
lst.remove(20)

for i in lst:
    print(i)
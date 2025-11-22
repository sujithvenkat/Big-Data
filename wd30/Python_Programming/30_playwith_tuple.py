tup=(10,20,40,30,20)
tup2=(60,80)
print(tup.count(30)) #number of occurences

tup3=tup.__add__(tup2) # to concatenate tuple with another tuple
for i in tup:
    print(i)
for i in tup3:
    print(i)

lst_tup = list(tup)
lst_tup.sort(reverse=True)
print("max num",max(lst_tup))
print("min num", min(lst_tup))



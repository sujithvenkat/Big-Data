sal_lst=[10000,20000,30000,10000,15000]
bonus=1000
sal_bonus_lst=[]
high_sal_bonus_lst=[]

for i in sal_lst:
    sal_bonus_lst.append(i + bonus)
for j in sal_bonus_lst:
    if j > 11000:
        high_sal_bonus_lst.append(j)


print("Salary bonus list " , sal_bonus_lst)
print("Highest Salary bonus list " , high_sal_bonus_lst)
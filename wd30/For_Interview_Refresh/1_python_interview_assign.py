import os
os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
#
# '''Assignment 1: List Mutability & Reference Trap (Very Common)'''
#
# '''Assignment 2: Remove Duplicates (Preserve Order)'''
# nums = [4, 5, 4, 3, 5, 6, 3, 2]
# #[4, 5, 3, 6, 2]
# res=[]
# seen = set()
# for num in nums:
#     if num not in res:
#         res.append(num)
#         seen.add(num)
# # print(res)
# # print(set(nums))
# # print(seen)
#
# '''Assignment 3: Second Largest (No Sorting)'''
# nums = [10, 20, 4, 45, 99, 99]
#
#
# #print(sorted(nums))
# nums.sort()
# #print(nums)
#
# sortd=sorted(set(nums))
# #print(sortd)
# #print(sortd[-1])
#
# largest = second = float('-inf')
# for num in nums:
#     if num > largest:
#         second = largest
#         largest = num
#     elif num < largest and num >second:
#         second = num
#
# #print(second)
#
# '''Assignment 4: Tuple with Mutable Element'''
# t = (1,2,[3,4])
# t[2].append(5)
# #print(t)
#
# '''Assignment 5: Frequency Count (Sorted)'''
# words = ["spark", "hive", "spark", "python", "hive", "spark"]
# freq = {}
# for word in words:
#     freq[word] = freq.get(word,0)+1
#
# #print(freq.items())
# result = dict(sorted(freq.items(), key=lambda x: -x[1]))
# #print(result)
#
# '''Assignment 6: Merge Lists into Dictionary'''
#
# keys = ["emp1", "emp2", "emp3"]
# values = [10000, 20000, 30000]
#
# result = {}
#
# for i in range(len(keys)):
#     if i < len(values):
#         result[keys[i]]=values[i]
#     else:
#         result[i]=None
#
# #print(result)
#
# '''Assignment 7: Flatten Nested List (No Recursion)'''
#
# data = [1, [2, 3], [4, [5, 6]], 7]
#
#
# def flatten(lst):
#     res=[]
#     for i in lst:
#         if isinstance(i, list):
#             res.extend(flatten(i))
#         else:
#             res.append(i)
#     return res
#
# flat=flatten(data)
# print(flat)
# #print(flat)
#
# '''Assignment 8: Dictionary Comprehension'''
#
# scores = {"A": 80, "B": 45, "C": 90, "D": 30}
#
# result = {k: "PASS" for k,v in scores.items() if v > 50}
#
# print(result)
#
# '''Assignment 9: Most Frequent Element (Tie → Smallest)'''
#
# nums = [1, 3, 2, 1, 3, 1, 3, 3]
#
# freq={}
#
# for i in nums:
#     freq[i]=freq.get(i,0)+1
#
# max_freq=max(freq.values())
#
# cand=[k for k, v in freq.items() if v == max_freq]
# '''
nums = [4, 5, 4, 3, 5, 6, 3, 2]


print(list(set(nums)))
res=[]
for n in nums:
    if n not in res:
        res.append(n)
print(res)

large = second = float('-inf')

for n in nums:
    if n > large:
        second = large
        large = n
    elif n < large and n > second:
        second = n

print(second)

words = ["spark", "hive", "spark", "python", "hive", "spark"]
freq={}

for i in words:
    freq[i]=freq.get(i,0)+1

print(freq)

keys = ["emp1", "emp2", "emp3"]
values = [10000, 20000, 30000]

result = {}

for i in range(len(keys)):
    result[keys[i]]=values[i] if i < len(values) else None

print(result)

data = [1, [2, 3], [4, [5, 6]], 7]



def flatten(lst):
    result = []
    for i in lst:
        if isinstance(i,list):
            result.extend(flatten(i))
        else:
            result.append(i)
    return result

flat=flatten(data)
print(flat)

scores = {"A": 80, "B": 45, "C": 90, "D": 30}
dict={k:"PASS" for k,v in scores.items() if v>= 50}

print(dict)
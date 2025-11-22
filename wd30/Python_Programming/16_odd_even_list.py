given_range = []
even_num_lst = []
odd_num_lst = []
'''test_lst=[10,20,30]
op_lst = list([])
print(test_lst[2])
test_lst[0]=12
op_lst[0]=12
#op_lst[0]=test_lst[0]
print(op_lst[0])
print(test_lst)
print(type(even_num_lst))
print(type(odd_num_lst))
odd_num_lst[0]
e_idx = 0
o_idx = 0 '''
for i in range(5, 21):
    given_range.append(i)
    if i % 2 == 0:
        even_num_lst.append(i)
        #print("even", i)
        '''even_num_lst[e_idx] = i
        e_idx += 1
        print(even_num_lst[e_idx])'''
    else:
        #print("odd", i)
        odd_num_lst.append(i)
        #print(odd_num_lst)
        '''print("index", o_idx)
        print("value before insert", odd_num_lst[0])
        odd_num_lst[o_idx] = i
        o_idx += 1
        print(odd_num_lst[o_idx])'''
print(f"The given range is : {given_range}")
print(f"Even number list : {even_num_lst}")
print(f"Odd number list : {odd_num_lst}")
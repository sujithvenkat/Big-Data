num = 4
loop_times = 3
result = 1
#using while loop
'''while loop_times > 0:
    result = result * num
    loop_times -= 1
'''
#using for loop
for i in range(1, loop_times+1):
    result = result * num
print(f"cube value of {num} is {result}")
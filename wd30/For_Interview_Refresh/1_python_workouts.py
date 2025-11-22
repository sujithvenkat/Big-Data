print("************** 1. Basics of Python Programing*************")

# lst=[5,3,2,1,0]
#
# rev_lst = reversed(lst)
# for i in rev_lst:
#     print(i)
#
# lst.reverse()
# print(lst)

#
# x=100
# y=10
# z=x*y
# z1=x/y
# print("Multiplication is", z , "Division is ", z1)
# a=2000
# a = a/y
# print(a)
#
# x:int = 100
# y:str = 'text'
# print(type(x),type(y))
# x=y
# print(type(x),type(y))
#
# while(1):
#     a=int(input())
#     b=int(input())
#     c=int(input())
#     print(a,b,c)
#
#     if (a > b) and (a > c):
#         print("a is greator")
#     elif(b > c) and (b > a):
#         print("b is greator")
#     else:
#         print("c is greator")
#
#
# x="madam"
# y="madam"
#
# rev_f = reversed(y)
# #str_y = y.__reversed__()
# rev_y = ''
# for i in rev_f:
#     print(i)
#     rev_y = rev_y + i
#
# print(rev_y)
#
# if (x == rev_y):
#     print("Its a palindrome")
# else:
#     print("its not a palindrome")
#

#list(list(elements))
def greatest_of_three_number(inp_1, inp_2, inp_3):
    if(inp_1>inp_2) and  (inp_1>inp_3):
        print(f"The First input {inp_1} is greater")
    elif(inp_2>inp_1) and  (inp_2>inp_3):
        print(f"The Second input {inp_2} is greater")
    else:
        print(f"The Third input {inp_3} is greater")

def find_about_the_given_number(inp_1):
    if (inp_1%2 == 0) and (inp_1 > 0):
        print(f"The given number {inp_1} is even but not negative ")
    elif(inp_1%2 != 0) and (inp_1 < 0):
        print(f"The given number {inp_1} is not even but negative ")
    elif(inp_1%2 != 0) and (inp_1 > 0):
        print(f"The given number {inp_1} is neither negative nor even")

def check_given_string_palindrome(str_1):
    str_1_reversed="".join(reversed(str_1))
    print(f"The reversed string is {str_1_reversed}")
    str_1_reversed2=str_1[::-1]
    print(f"The other method reversed string is {str_1_reversed2}")
    if str_1 == str_1_reversed:
        print(f"The give string {str_1} is a palindrome")
    else:
        print(f"The give string {str_1} is not a palindrome")

def check_given_int(inp_1):
    if isinstance(inp_1, int):
        print(f"{inp_1} is an integer")
    elif isinstance(inp_1, str):
        print(f"{inp_1} is a string")
    else:
        print("nethir string nor integer")
        print(type(inp_1))

def calc(inp_1,inp_2,str_1):
    try:
        if str_1 == 'mul':
            res = inp_1 * inp_2
        elif str_1 == 'sub':
            res = inp_1 - inp_2
        elif str_1 == 'div':
            res = inp_1 / inp_2
        else:
            res = inp_1 + inp_2
        return res
    except ZeroDivisionError as te:
        print(f"ZeroDivisionError: {te}")
        inp_2=1
        return calc(inp_1, inp_2, str_1)





def tuple_calc(inp_1,inp_2,str_1='all'):
    res_mul = inp_1 * inp_2
    res_sub = inp_1 - inp_2
    res_div = inp_1 / inp_2
    res_add = inp_1 + inp_2
    return (res_mul, res_add, res_sub, res_div)

def multiple_results(str_1):
    print("Capitalize :" + str_1.capitalize())
    print("Upper case :" + str_1.upper())
    print(f"Length : ", len(str_1))
    print(f"Count of words :" , len(str_1.split()))
    print(f"Ends with 's' :" , str_1.endswith('s'))
    print("After replace 'e' with 'a' :" + str_1.replace('e','a'))

def promo(amount,offer_percent ,offer_cap_limit = 50):
    if amount * offer_percent < offer_cap_limit:
        print("offer_cap_limit", offer_cap_limit)
        return amount - (amount * offer_percent)
    else:
        print("offer_cap_limit", offer_cap_limit)
        return amount-offer_cap_limit

def promo_arb(*args):
    amount = args[0]
    offer_percent = args[1]
    offer_cap_limit = args[2]
    if amount * offer_percent < offer_cap_limit:
        print("offer_cap_limit", offer_cap_limit)
        return amount - (amount * offer_percent)
    else:
        print("offer_cap_limit", offer_cap_limit)
        return amount-offer_cap_limit

def promo_lam(amount,offer_percent ,offer_cap_limit,func):
    if amount * offer_percent < offer_cap_limit:
        return lam(amount, offer_percent)
    else:
        return amount-offer_cap_limit

def promo_excep(amount,offer_percent ,offer_cap_limit = 50):
    try:
        if offer_percent < 0:
            raise ValueError ("Offer percent cannot be negative")
        if amount * offer_percent < offer_cap_limit:
            print("offer_cap_limit", offer_cap_limit)
            return amount - (amount * offer_percent)
        else:
            print("offer_cap_limit", offer_cap_limit)
            return amount-offer_cap_limit
    except ValueError as ve:
        print(f"Exception : ", ve)
        print(f"Consider this as a Warning and Using absolute value instead.")
        offer_percent= abs(offer_percent)
        return promo_excep(amount,offer_percent ,offer_cap_limit = 50)


# print("Enter three numbers")
# inp_1=int(input())
# inp_2=int(input())
# #inp_3=int(input())
# print("Enter a string")
# str_1=str(input())
#greatest_of_three_number(inp_1, inp_2, inp_3)
#find_about_the_given_number(inp_1)
#check_given_string_palindrome(str_1)
#x=100
#check_given_int(x)
#res=calc(inp_1,inp_2,str_1)
#res_tuple=tuple_calc(inp_1,inp_2)
#multiple_results(str_1)
inp_1=1000
inp_2=-.10
inp_3=200
# print(promo(inp_1,inp_2,inp_3))
# print(promo(inp_1,inp_2))
# print(promo_arb(inp_1,inp_2,inp_3))
# lam = lambda amount,offer_percent: amount - (amount * offer_percent)
# print(promo_lam(inp_1,inp_2,inp_3,lam))
# res=calc(inp_1,inp_2,str_1)
# print("Final res", res)
print(promo_excep(inp_1, inp_2, inp_3))





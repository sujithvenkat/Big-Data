#Python is an Indent based programming
#Comments in Python
#Variables in Python
# """  """ and ''' '''

c='venkat'
d="Venkat Sujith"
print(f"Hello {c} / {d} welcome to python world")

teamnumber=30
#print("Good morning " + teamnumber)  # will work in scala but not in python
print("Good morning",str(teamnumber)) # default space between two words will be there
print("Good morning " + str(teamnumber))
print(f"Good morning {teamnumber}")
print("Good morning {0}" .format(teamnumber))

a=1000
b=10000
c=10000
print(a,b,c)

if a==b==c:
    print("All three are same")
elif (a > b) & (a > c):
    print("A is bigger")
elif (b > a) & (b > c):
    print("B is bigger")
elif (c > b) & (c > a):
    print("C is bigger")
elif a==b:
    if a>c:
        print("a and b are same but both are greater than c")

elif b==c:
    if b>a:
        print("b and c are same but both are greater than a")

elif a==c:
    if a>b:
        print("a and c are same but both are greater than b")










def greatest_num(a,b,c):
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
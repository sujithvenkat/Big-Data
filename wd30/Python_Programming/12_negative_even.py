get_inp='its a string'
if type(get_inp) == int:
    if get_inp % 2 == 0:
        if get_inp > 0:
            print(f"The given number {get_inp} is even but not negative")
        else:
            print(f"The given number {get_inp} is even but negative")
    else:
        if get_inp > 0:
            print(f"The given number {get_inp} is odd and positive")
        else:
            print(f"the given number {get_inp} is not even but negative")
else:
    print("the given number is neither negative nor even, its just a string")


while True:
    print("Enter any string")
    get_inp = str(input())
    #rev_get_inp = reversed(get_inp)
    #print(reversed(get_inp))
    #print(get_inp.__reversed__(self,get_inp))

    rev_get_inp=''
    for i in get_inp:
        rev_get_inp = i + rev_get_inp
    #   print(rev_get_inp)
    #print(rev_get_inp)
    if rev_get_inp == get_inp:
        print(f"The entered string {get_inp} is Palindrome")
    else:
        print(f"The entered string {get_inp} is  not a Palindrome")



str1 = 'inceptez technologies'
lst_str1 = []
def string_handling (str1: str):
    print(type(str1.split()))
    print(str1.capitalize())   ##    Will initcap only the first word in the entire value Inceptez technologies
    print(str1.title())        ##    will initcap each word in the line   Inceptez Technologies
    print(str1.__len__())
    print(len(str1))
    print(str1.endswith('s'))
    print(len(str1.split()))
    print(str1.replace('e','a'))
    lst_str1.append(str1.title())
    lst_str1.append(str1.upper())
    lst_str1.append(str1.__len__())
    lst_str1.append(len(str1.split()))
    lst_str1.append(str1.endswith('s'))
    lst_str1.append(str1.replace('e','a'))
    return lst_str1
print(string_handling(str1))
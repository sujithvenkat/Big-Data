tuple_input = ("Inceptez","Technologies","Pvt","Ltd")
print(type(tuple_input))
#print(tuple_input[2])
print("tuple input")
for i in tuple_input:
    print(i)

tuple_output = ()
#tuple_output.append -- non resizable and immutable

tmp_list = list(tuple_output)
print(type(tmp_list))

tmp_list.append(tuple_input[1])
tmp_list.append(tuple_input[3])
tuple_output=tuple(tmp_list)

print("tuple output")
for i in tuple_output:
    print(i)

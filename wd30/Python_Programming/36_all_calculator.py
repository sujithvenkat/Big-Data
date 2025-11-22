def all_calculator(inp1:int, inp2:int):
    lst_result = []
    lst_result.append(inp1 + inp2)
    lst_result.append(inp1 - inp2)
    lst_result.append(inp1 * inp2)
    lst_result.append(inp1 / inp2)
    return lst_result


print(all_calculator(10, 3))
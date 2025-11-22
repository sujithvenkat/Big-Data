#import operator
def calculator(inp1:int, inp2:int,oper:str ):
    dict_case = {'add': 'inp1.__add__(inp2)',
                 'mul': 'inp1.__mul__(inp2)',
                 'sub': 'inp1.__sub__(inp2)',
                 'div': 'inp1.__divmod__(inp2)',
                 ' ': 'inp1.__add__(inp2)'}
    result = dict_case[oper]

    return eval(result)

print(calculator(20,2,'mul'))

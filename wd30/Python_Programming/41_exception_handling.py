def calculator(inp1, inp2,oper ):
    dict_case = {'add': inp1 + inp2,
                 'mul': inp1 * inp2,
                 'sub': inp1 - inp2,
                 'div': inp1 / inp2,
                 ' ': inp1 + inp2 }
    result = dict_case[oper]

    return result
inp1=20
inp2=2
oper_str='div'
try:
    print(calculator(inp1,inp2,oper_str))
except TypeError as typ_error:
    print(f"Exception occured {typ_error}")
    print(calculator(int(inp1), int(inp2), oper_str))
except KeyError as err:
    print("Unable to retrieve variable. Exception: {} ".format(err))
    print(f"Exception occured {err}")
else:
    print("I will be executing if no exception occurs in the try block - closing connections created in the try block")
finally:
    print("I will be executing at any cost, whether exception occured or not - closing all connections created in this code")

'''except KeyError as k_err:
    print(f"Exception occured {k_err}")
except Exception as error_message:
    print("Other exception occured {}".format(error_message))
    print(f"Exception occured {error_message}")'''
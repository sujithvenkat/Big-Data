promo_calc_lambda = lambda x:x[0]-(x[0] * x[1])
'''test_params=[(1000,.10)]
test_result=[]
test_result = map(promo_calc_lambda,test_params)
print(list(test_result))

kms_calc_lambda=lambda x:x[2] * 5 #Anonymous Functions/Lambda Functionskms_calc_lambda=lambda x:x[2] * 5 #Anonymous Functions/Lambda Functions
def kms_calc_funct(x):
    return x[2]*5
kms_driven=[('a','petrol',20),('b','diesel',30)]
result_lam=map(kms_calc_lambda,kms_driven)#advisable
#result_lam1=map(lambda x:x[2] * 5,kms_driven)#advisable
#result_regular=map(kms_calc_funct,kms_driven)#not advisable
print(list(result_lam))'''
'''print(list(result_regular))
def kms_calc_funct(x):
    return x[2]*5'''



def promo_calc(amount, offer_percent, offer_cap_limit,promo_calc_lambda):
    print("with lambda parameters")
    if (amount * offer_percent) < offer_cap_limit:
        lambda_param=[(amount,offer_percent)]
        lambda_call=map(promo_calc_lambda,lambda_param)
        print(list(lambda_call))
        return list(lambda_call)
    else:
        return amount - offer_cap_limit


result=promo_calc(1000,.10,200,promo_calc_lambda)
print(type(result))
print(*result)
print(promo_calc(1000,.10,20,promo_calc_lambda))


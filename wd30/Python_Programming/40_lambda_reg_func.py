

print("complex logic in lambda function (not advisable)")
kms_driven=[('a','petrol',20),('b','diesel',30),('irfan','gasoline',40)]
surcharge_anonymous=lambda kmd_ft:(kmd_ft[0],kmd_ft[2]*5 if kmd_ft[1]=='petrol' else (kmd_ft[2]*3 if kmd_ft[1]=='diesel' else kmd_ft[2]*2))
print(list(map(surcharge_anonymous,kms_driven)))

promo_calc_lambda = lambda x:x[0]-(x[0] * x[1])
print(list(map(promo_calc_lambda,[(1000,.10)])))

#if amount*offer_percent < offer_cap_limit then return amount-(amount*offer_percent) else return the amount-offer_cap_limit

promo_calc_lambda_logic = lambda x:x[0]-(x[0] * x[1]) if x[0]*x[1] < x[2] else x[0] - x[2]
print(list(map(promo_calc_lambda_logic,[(1000,.10,20)])))
print(list(map(promo_calc_lambda_logic,[(1000,.10,200)])))



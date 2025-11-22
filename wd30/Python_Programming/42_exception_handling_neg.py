def promo_calc(amount, offer_percent, offer_cap_limit=200):
    print("with default parameter and with all 3 params")
    if (amount * offer_percent) < offer_cap_limit:
        return amount - (amount * offer_percent)
    else:
        return amount - offer_cap_limit

a=1000
b=-.10
c=20
try:
    if a < 0 or b < 0 or c < 0:
        raise ValueError
    else:
        print(promo_calc(a,b,c))  # with all 3 params
except ValueError as err:
    print("ValueError Exception occured Please Enter positive numbers",format(err))

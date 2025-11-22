def promo_calc(amount, offer_percent, offer_cap_limit=200):
    print("with default parameter and with all 3 params")
    if (amount * offer_percent) < offer_cap_limit:
        return amount - (amount * offer_percent)
    else:
        return amount - offer_cap_limit


print(promo_calc(1000, .10, 20))  # with all 3 params
print(promo_calc(1000, .10))  # with default parameter


def promo_calc_arbitrary(*args):
    print("with arbitrary arguments")
    if (args[0] * args[1]) < args[2]:
        return args[0] - (args[0] * args[1])
    else:
        return args[0] - args[2]


print(promo_calc_arbitrary(1000, .10, 200))  # with arbitrary arguments


def promo_calc_key_arbitrary(**args):
    print("with keyword arbitrary arguments")
    if (args['amount'] * args['offer_percent']) < args['offer_cap_limit']:
        return args['amount'] - (args['amount'] * args['offer_percent'])
    else:
        return args['amount'] - args['offer_cap_limit']


print(promo_calc_key_arbitrary(offer_percent=.10,offer_cap_limit=20,amount=1000))  #with keyword arbitrary arguments

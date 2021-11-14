import itertools

t1 = [(2,4,1),(7,-4,-9),(12,-3,97)]
t2 = [(94,23),(58,25)]
t3 = [(10,),(12,),(22,)]

FROM_CLAUSE = '     t1 ,t2,    t3 '
FROM_CLAUSE = [x.strip() for x in FROM_CLAUSE.split(',')]
## FROM_CLAUSE = ['t1', 't2', 't3']
if len(FROM_CLAUSE) == 1:
    FROM_TABLE = FROM_CLAUSE[0]
def cartesian_product(table1, table2):
    cartesian_temp = list(itertools.product(table1,table2))
    cartesian=[]
    for item in cartesian_temp:
        item = item[0] + item[1]
        cartesian.append(item)
    return cartesian

product = cartesian_product(t1,t2)
#print(product)
product = cartesian_product(product,t3)
#print(product)

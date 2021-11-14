import csv
import sqlparse
import os
import itertools
import sys

with open('metadata.txt', 'r') as file:
    metadata = file.read().splitlines()
    # read() returns a single str object, so splitlines() is used to split that single str into separate lines(str)

db_schema = {}
for i in range(len(metadata)):
    if metadata[i]=="<begin_table>":
        i=i+1
        table_name = metadata[i]
        i=i+1
        column_names = []
        while metadata[i] != "<end_table>":
            column_names.append(metadata[i].lower())
            i=i+1
    db_schema[table_name] = column_names
'''
db_schema contains names of tables along with their column names as a dictionary
'''
#print(db_schema)
## db_schema =  {'table1': ['a', 'b', 'c'], 'table2': ['d'. 'e']}  
all_col_names = []
for table_schema in db_schema.values():
    for colnames in table_schema:
        all_col_names.append(colnames)
## all_col_names = ['a', 'b', 'c', 'd', 'e']
#print(all_col_names)
 
table_names = list(db_schema.keys())    #['table1', 'table2']


csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]  #csv_files list contains all csv files given in this folder
## csv_files = ['table1.csv', 'table2.csv']
print(csv_files)
tables_with_data = {}
i=0
for csvFile in csv_files:
    
    with open(csvFile,'r') as file:
        rawdata = csv.reader(file)
        rawtable = []
        for record in rawdata:
            record = tuple(map(int,record))
            rawtable.append(record)
    tables_with_data[table_names[i]] = rawtable
    i=i+1
'''
The aforewritten code snippet opens all csv files(containing the data) present in 'files' folder, and iteratively stores each line of the csv file in 'record' tuple. 
As the csv file contents have 'str' datatype, they are converted to int using map().
A list named 'rawtable' holds all the "str-to-int" converted 'record' tuples, making it a 2-d array
'rawtable' is then stored with its table name in a dictionary named 'tables_with_data'
'''
## tables_with_data = {'table1': [(922, 158, 5727), (640, 773, 5058), (775, 85, 10164), (-551, 811, 1534), (-952, 311, 1318), (-354, 646, 7063), (-497, 335, 4549), (411, 803, 10519), (-900, 718, 9020), (858, 731, 3668)], 'table2': [(158, 11191), (773, 14421), (85, 5117), (811, 13393), (311, 16116), (646, 5403), (335, 6309), (803, 12262), (718, 10226), (731, 13021)]}
#print(tables_with_data)

all_cols_data = {}
for table in tables_with_data.values():
    for n_col in range(len(table[0])):
        col_data = []
        for n_row in range(len(table)):
            col_data.append(table[n_row][n_col])
        col_name = all_col_names.pop(0)
        all_cols_data[col_name] = col_data

## all_cols_data = {'a': [922, 640, 775, -551, -952, -354, -497, 411, -900, 858], 'b': [158, 773, 85, 811, 311, 646, 335, 803, 718, 731], 'c': [5727, 5058, 10164, 1534, 1318, 7063, 4549, 10519, 9020, 3668], 'd': [158, 773, 85, 811, 311, 646, 335, 803, 718, 731], 'e': [11191, 14421, 5117, 13393, 16116, 5403, 6309, 12262, 10226, 13021]}



def cartesian_product(table1, table2):
    cartesian_temp = list(itertools.product(table1,table2))
    cartesian=[]
    for item in cartesian_temp:
        item = item[0] + item[1]
        cartesian.append(item)
    return cartesian


def fromImplementation(clause):
    FROM_CLAUSE = [x.strip() for x in clause.split(',')]
    ## FROM_CLAUSE = ['table1', 'table2']
    FROM_columns = []
    for table_name in FROM_CLAUSE:
        FROM_columns = FROM_columns + db_schema[table_name]

    FROM_TABLE = []
    if len(FROM_CLAUSE) == 1:
        FROM_TABLE = tables_with_data[FROM_CLAUSE[0]]
    else:
        product = tables_with_data[FROM_CLAUSE[0]]
        for i in range(1,len(FROM_CLAUSE)):
            t2 = tables_with_data[FROM_CLAUSE[i]]
            product = cartesian_product(product,t2)

        FROM_TABLE = product

    return FROM_TABLE, FROM_columns
    #print(FROM_TABLE)    

def extract_where_clause(tokens_list):
    
    condition1 = []
    condition2 = []
    i=1
    logical_op=''
    for subtoken in token.flatten():
        if str(subtoken.ttype) == 'Token.Name' and i==1:
            condition1.append(str(subtoken.value))
        elif str(subtoken.ttype) == 'Token.Operator.Comparison' and i==1:
            condition1.append(str(subtoken.value))
        elif str(subtoken.ttype) == 'Token.Literal.Number.Integer' and i==1:
            condition1.append(int(subtoken.value))
        elif str(subtoken.value) in ['or', 'and']:  #if 'or'/'and' is there it means there are two conditions in where clause. So i=2 indicates second condition
            logical_op = str(subtoken.value)
            i=2
        elif str(subtoken.ttype) == 'Token.Name' and i==2:
            condition2.append(str(subtoken.value))
        elif str(subtoken.ttype) == 'Token.Operator.Comparison' and i==2:
            condition2.append(str(subtoken.value))
        elif str(subtoken.ttype) == 'Token.Literal.Number.Integer' and i==2:
            condition2.append(int(subtoken.value))
        else:
            pass
    
    return condition1,condition2,logical_op




def whereImplementation(cond1, cond2, logic_opr, fromResult, fromResultColnamesIndexed):
    whereResult = []
    #print(fromResult)
    if not isinstance(cond1[0], str):
        sys.exit("incorrect column datatype!")
    if cond1[0] not in fromResultColnamesIndexed.keys():
        sys.exit("No such column is present!")
    operand1_cond1 = cond1[0]
    opr1 = cond1[1]
    if isinstance(cond1[2], str) and cond1[2] not in fromResultColnamesIndexed.keys():
        sys.exit("No such column is present!")

    operand2_cond1 = cond1[2]

    condition1result = []
    condition2result = []
       
    j = fromResultColnamesIndexed[operand1_cond1]
    if isinstance(operand2_cond1,str):
        k = fromResultColnamesIndexed[operand2_cond1]
        
        if opr1 == '=':
            for i in range(len(fromResult)):
                if fromResult[i][j] == fromResult[i][k]:
                    condition1result.append(fromResult[i])
        elif opr1 == '>':
            for i in range(len(fromResult)):
                if fromResult[i][j] > fromResult[i][k]:
                    condition1result.append(fromResult[i])
        elif opr1 == '<':
            for i in range(len(fromResult)):
                if fromResult[i][j] < fromResult[i][k]:
                    condition1result.append(fromResult[i])
        elif opr1 == '>=':
            for i in range(len(fromResult)):
                if fromResult[i][j] >= fromResult[i][k]:
                    condition1result.append(fromResult[i])
        elif opr1 == '<=':
            for i in range(len(fromResult)):
                if fromResult[i][j] <= fromResult[i][k]:
                    condition1result.append(fromResult[i])
            
    elif isinstance(operand2_cond1,int):
        if opr1 == '=':
            for i in range(len(fromResult)):
                if fromResult[i][j] == operand2_cond1:
                    condition1result.append(fromResult[i])
        elif opr1 == '>':
            for i in range(len(fromResult)):
                if fromResult[i][j] > operand2_cond1:
                    condition1result.append(fromResult[i])
        elif opr1 == '<':
            for i in range(len(fromResult)):
                if fromResult[i][j] < operand2_cond1:
                    condition1result.append(fromResult[i])
        elif opr1 == '>=':
            for i in range(len(fromResult)):
                if fromResult[i][j] >= operand2_cond1:
                    condition1result.append(fromResult[i])
        elif opr1 == '<=':
            for i in range(len(fromResult)):
                if fromResult[i][j] <= operand2_cond1:
                    condition1result.append(fromResult[i])
    
    else:
        sys.exit("invalid query, error in where clause!")    

    if cond2 == []:
        return condition1result
    else:
        
        if not isinstance(cond2[0], str):
            sys.exit("incorrect column datatype!")
        if cond2[0] not in fromResultColnamesIndexed.keys():
            sys.exit("No such column is present!")
        operand1_cond2 = cond2[0]
        opr2 = cond2[1]
        if isinstance(cond2[2], str) and cond2[2] not in fromResultColnamesIndexed.keys():
            sys.exit("No such column is present!")

        operand2_cond2 = cond2[2]

        j = fromResultColnamesIndexed[operand1_cond2]
        if isinstance(operand2_cond2,str):
            k = fromResultColnamesIndexed[operand2_cond2]
            
            if opr2 == '=':
                for i in range(len(fromResult)):
                    if fromResult[i][j] == fromResult[i][k]:
                        condition2result.append(fromResult[i])
            elif opr2 == '>':
                for i in range(len(fromResult)):
                    if fromResult[i][j] > fromResult[i][k]:
                        condition2result.append(fromResult[i])
            elif opr2 == '<':
                for i in range(len(fromResult)):
                    if fromResult[i][j] < fromResult[i][k]:
                        condition2result.append(fromResult[i])
            elif opr2 == '>=':
                for i in range(len(fromResult)):
                    if fromResult[i][j] >= fromResult[i][k]:
                        condition2result.append(fromResult[i])
            elif opr2 == '<=':
                for i in range(len(fromResult)):
                    if fromResult[i][j] <= fromResult[i][k]:
                        condition2result.append(fromResult[i])
                
        elif isinstance(operand2_cond2,int):
            if opr2 == '=':
                for i in range(len(fromResult)):
                    if fromResult[i][j] == operand2_cond2:
                        condition2result.append(fromResult[i])
            elif opr2 == '>':
                for i in range(len(fromResult)):
                    if fromResult[i][j] > operand2_cond2:
                        condition2result.append(fromResult[i])
            elif opr2 == '<':
                for i in range(len(fromResult)):
                    if fromResult[i][j] < operand2_cond2:
                        condition2result.append(fromResult[i])
            elif opr2 == '>=':
                for i in range(len(fromResult)):
                    if fromResult[i][j] >= operand2_cond2:
                        condition2result.append(fromResult[i])
            elif opr2 == '<=':
                for i in range(len(fromResult)):
                    if fromResult[i][j] <= operand2_cond2:
                        condition2result.append(fromResult[i])
        else:
            sys.exit("invalid query, error in where clause!")


    condition1resultSet = set(condition1result)
    condition2resultSet = set(condition2result)    
    if logic_opr == 'or':
        whereResultSet = condition1resultSet | condition2resultSet  #set union
        whereResult = list(whereResultSet)

    elif logic_opr == 'and':
        whereResultSet = condition1resultSet & condition2resultSet  #set intersection
        whereResult = list(whereResultSet)
    else:
        sys.exit("invalid query, error in where clause!")


    return whereResult
        
        


def groupbyImplementation(groupbyClause, whereResult, fromResultColnamesIndexed):
    GROUPBY_COLNAME = groupbyClause.split(',')
    if len(GROUPBY_COLNAME) > 1:
        sys.exit("more than one column present with group by !")
    GROUPBY_COLNAME = GROUPBY_COL[0]
    GROUPBY_COL_INDEX = fromResultColnamesIndexed[GROUPBY_COLNAME]

    GROUPBY_COL_DATA = set()
    GROUPBY_RESULT = []
    for i in range(len(whereResult)):
        if whereResult[i][GROUPBY_COL_INDEX] not in GROUPBY_COL_DATA:
            GROUPBY_COL_DATA.add(whereResult[i][GROUPBY_COL_INDEX])
            GROUPBY_RESULT.append(whereResult[i])

    return GROUPBY_RESULT
    

def selectImplementation(selectClause, groupbyResult, fromResultColnamesIndexed, groupbyColname):
    SELECT_CLAUSE = [x.strip() for x in selectClause.split(',')]
    SELECT_RESULT = []
    if len(SELECT_CLAUSE) > 1 and '*' in SELECT_CLAUSE:
        sys,exit("Invalid query, error in select statement!")

    if len(SELECT_CLAUSE) == 1 and '*' in SELECT_CLAUSE:
        return groupbyResult, fromResultColnamesIndexed.keys()
    selected_cols_data = []
    selected_cols_name = []
    for selectVal in SELECT_CLAUSE:
        if selectVal in fromResultColnamesIndexed.keys():
            selected_cols_name.append(selectVal)
            col_index = fromResultColnamesIndexed[selectVal]
            col = [x[col_index] for x in groupbyResult]
            selected_cols_data.append(col)

        
    SELECT_RESULT = list(zip(*selected_cols_data))
    return SELECT_RESULT,selected_cols_name


    


'''PARSING.............................................................'''

#query = 'select max(b),c,count(*) from table1 where A>500 order by d desc'.lower()
query = sys.argv[1].lower()
stmtInstanceTuple = sqlparse.parse(query)
#parsed[0]._pprint_tree()
stmtObject = stmtInstanceTuple[0]
tokens_list = stmtObject.tokens

list_sql_query = list(map(str,tokens_list))
#print(list_sql)
 
list_sql_query = list(filter(' '.__ne__, list_sql_query))
## list_sql_query = ['select', 'distinct', 'max(C),name, age,course', 'from', 'student, hostel', 'where B=D ', 'group by', 'course', 'order by', 'desc']



''' Parsing FROM'''
for i in range(len(list_sql_query)):
    if list_sql_query[i] == 'from':
        FROM_CLAUSE = list_sql_query[i+1]
        break
FROM_RESULT, FROM_RESULT_colnames = fromImplementation(FROM_CLAUSE)
FROM_RESULT_colnames_indexed = {}
for i in range(len(FROM_RESULT_colnames)):
    FROM_RESULT_colnames_indexed[FROM_RESULT_colnames[i]] = i




''' Parsing WHERE '''
WHERE_RESULT = FROM_RESULT
for token in tokens_list:
    if isinstance(token, sqlparse.sql.Where):
        cond1,cond2,logic_opr = extract_where_clause(tokens_list)
        WHERE_RESULT = whereImplementation(cond1,cond2,logic_opr,FROM_RESULT, FROM_RESULT_colnames_indexed)
        break  

''' Parsing Group By '''
GROUPBY_RESULT = WHERE_RESULT
GROUPBY_COL = ""
for i in range(len(list_sql_query)):
    if list_sql_query[i] == 'group by':
        GROUPBY_COL = list_sql_query[i+1]
        GROUPBY_RESULT = groupbyImplementation(GROUPBY_COL, WHERE_RESULT, FROM_RESULT_colnames_indexed)
        break

''' Parsing Select '''
SELECT_RESULT = GROUPBY_RESULT
SELECT_COLNAMES = []
for i in range(len(list_sql_query)):
    if list_sql_query[i] == 'from':
        SELECT_CLAUSE = list_sql_query[i-1]
        SELECT_RESULT,SELECT_COLNAMES =  selectImplementation(SELECT_CLAUSE,GROUPBY_RESULT,FROM_RESULT_colnames_indexed, GROUPBY_COL)
        break

print(SELECT_RESULT)
select_clause_token = tokens_list[2]    #tokens_list[0] = select, tokens_list[1] = whitespace
select_tokens = []
for subtoken in select_clause_token.flatten():
    select_tokens.append(str(subtoken.value))

select_tokens = list(filter(','.__ne__, select_tokens))
select_tokens = list(filter(')'.__ne__, select_tokens))
select_tokens = list(filter('('.__ne__, select_tokens))
print(select_tokens)









def printToFile(output):
    with open('output.txt','w') as file:
        colnames_str = ','.join(SELECT_COLNAMES)
        file.write(colnames_str)
        file.write('\n')
        for row in FINAL_RESULT:
            row_str = list(map(str,row))    # as the tuples consist of int data , the data values are converted from int to string so as to use .join() 
            file.write(','.join(row_str))
            file.write('\n')




FINAL_RESULT = SELECT_RESULT
printToFile(FINAL_RESULT)

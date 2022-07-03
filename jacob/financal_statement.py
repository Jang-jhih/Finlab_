from jacob.crawlar import *
# from jacob.crawlar import add_to_sql


if not os.path.isdir(os.path.join("datasource")):
    os.mkdir(os.path.join("datasource"))
    
if not os.path.isdir(os.path.join("datasource","financal_statement")):
    os.mkdir(os.path.join("datasource","financal_statement"))
    


def crawl_finance_statement():

    url ="https://mops.twse.com.tw/server-java/FileDownLoad?step=9&functionName=show_file2&fileName=tifrs-2022Q1.zip&filePath=/ifrs/2022/"
    req = requests.get(url,verify = True ,stream = True)


    with open(os.path.join('datasource',"tmp.zip"),'wb') as f:
        for chunk in req.iter_content(chunk_size = 1024):
            if chunk:
                f.write(chunk)



    path=os.path.join('datasource','financal_statement')
    zipfile = zipfile.ZipFile(open(os.path.join('datasource',"tmp.zip"),'rb'))
    zipfile.extractall(path=path)
    fname = [f for f in  os.listdir(path) if f[-5:] == '.html' ]
    fname = sorted(fname)
    newfilename = [f.split('-')[5] + ".html" for f in fname]


    for fold, fnew in zip(fname ,newfilename):
        if not os.path.exists(os.path.join(path,fnew)):
            os.rename(os.path.join(path,fold) ,os.path.join(path,fnew))


def import_finance_statement(year,season):
    data_index = ['Stock_ID',"year",'season']


    listdir = os.listdir(os.path.join('datasource','financal_statement'))
    listdir = [os.path.join('datasource','financal_statement',i) for i in listdir]

    for _dir in listdir:
        Stock_ID = _dir.split('.')[0][-4:]
        dfs = pd.read_html(open(_dir,encoding="utf-8-sig"))
        balance_sheet = dfs[0]
        income_sheet  = dfs[1]
        cash_flow     = dfs[2]

        financal_statement =[balance_sheet,income_sheet,cash_flow]
        Statement_names = ['balance_sheet','income_sheet','cash_flow']

        for statement,Statement_name in zip(financal_statement,Statement_names):
            statement.columns = statement.columns.get_level_values(1)
            statement.rename(columns={statement.columns[2]:"Amount"},inplace = True)
            statement.rename(columns={statement.columns[1]:"Accounting_Title"},inplace=True)
            statement['Accounting_Title']=statement['Accounting_Title'].apply(remove_english) 
            statement['Stock_ID'] = Stock_ID
            statement.drop_duplicates(subset = ["Stock_ID",'Accounting_Title'],inplace=True ,keep='last')
            statement = statement.pivot(index= "Stock_ID" ,columns = 'Accounting_Title' ,values = "Amount")
            statement.dropna(axis=1,inplace=True)
            statement['year']=year
            statement['season']=season
            
            #把重要欄位移到前面
            statement.reset_index(inplace=True)
            statement.set_index(data_index,inplace=True)
            statement.reset_index(inplace=True)
            # add_to_sql(table_name=Statement_name,new_df=statement,data_index=data_index)
            add_to_sql(table_name=Statement_name,new_df=statement)
            add_to_pkl(table_name=Statement_name,new_df=statement)


def remove_english(s):
    result = re.sub(r'[a-zA-Z()]', "", s)
    result = result.replace(" ","").replace(",","").replace("-","")
    return result
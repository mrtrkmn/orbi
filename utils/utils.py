import pandas as pd

def compare_excel_files(file1: pd.DataFrame, file2: pd.DataFrame, output_file: str):
    df1 = pd.read_excel(file1, sheet_name='Results')
    df2 = pd.read_excel(file2, sheet_name='Results')
    
    # check number of rows in Company Name column
    if df1.shape[0] < df2.shape[0]:
        diff_df = df2[~df2.isin(df1)].dropna(subset=['Unnamed: 0'])
        print(f"File {file2} has {diff_df.shape[0]} more companies than {file1}")
    else:
        diff_df = df1[~df1.isin(df2)].dropna(subset=['Unnamed: 0'])
        print(f"File {file1} has {diff_df.shape[0]} more companies than {file2}")
    
    # save to excel file
    diff_df.to_excel(output_file, index=False)
    
# if __name__ == '__main__':
#     file1= 'only_company_name.xlsx'
#     file2 = 'received_from_sec.xlsx'
#     output_file = 'diff.xlsx'
#     compare_excel_files(file2, file1, output_file)


from datetime import datetime

json_list = [{'id' : 5461, 'date': '02/14/2009'},
             {'id' : 5217, 'date': '09/25/2002'},
             {'id' : 5913, 'date': '02/28/2014'},
             {'id' : 5132, 'date': '01/07/2005'}]

sorted_date = sorted(json_list, key=lambda x: datetime.strptime(x['date'], '%m/%d/%Y'))
print (sorted_date)
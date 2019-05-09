from .base import *

def view(df, max_rows = None, max_columns = None, max_colwidth = 500):
    with pd.option_context('display.max_rows', max_rows, 'display.max_columns', max_columns, 'display.max_colwidth', max_colwidth):
        print(df)
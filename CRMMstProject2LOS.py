from sqlalchemy import create_engine
import urllib
import sqlalchemy
import pandas as pd
import sys
from datetime import datetime


def main():
    params = 'Driver={ODBC Driver 17 for SQL Server};Server=192.168.2.58;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;'
    params = urllib.parse.quote_plus(params)

    db = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, fast_executemany=True)

    str_sql = """
    SELECT a.ProductID,
       a.Project,
	   a.Producttype,
	   a.ProjectType,
       ISNULL(a.SAPProductID, '-') AS SAPProductID,
       ISNULL(a.SAPCostCenter, '-') AS SAPCostCenter,
       b.CompanyNameThai,
	   FORMAT(a.CreateDate,'dd/MM/yyyy') AS CreateDate,
	   FORMAT(a.StartSale,'dd/MM/yyyy') AS StartSale,
       CASE ISNULL(RTPExcusive, 1)
           WHEN 1 THEN
               'Active'
           WHEN 2 THEN
               'Active'
           ELSE
               'Inactive'
       END AS StatusProject
    FROM dbo.ICON_EntForms_Products a LEFT JOIN dbo.ICON_EntForms_Company b ON a.CompanyID = b.CompanyID
    WHERE 1 = 1
    AND a.RTPExcusive = 1
    ORDER BY ProductID
    """

    # Setup Format File Name CSV
    date_fmt = datetime.now().strftime("%Y%m%d")
    file_type = ".csv"
    file_name = r"/home/ubuntu/myPython/crm2los/CRM2LOS_Project_"
    full_file_name = "{}{}{}".format(file_name, date_fmt, file_type)

    # Read by SQL Statement
    pd.read_sql(sql=str_sql, con=db).to_csv(full_file_name, index=False, encoding="utf-8")


if __name__ == '__main__':
    main()

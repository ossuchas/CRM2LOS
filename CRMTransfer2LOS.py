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
    SELECT [ProjectCode],[UnitNo],[UnitAddress],[UnitArea],TitleDeedNumber,[ElectricMeter],[WaterMeter],[ElectricMeterTransferDate],[WaterMeterTransferDate]
           ,[TransferDate],[ID],[Title],[FirstNameTH],[LastNameTH],[FirstNameEN],[LastNameEN],[Nationality],[Gender],[Race],[BirthDate]
           ,[CardID],[HomeTel],[Mobile],[Fax],[Email],[Address],[Moo],[SoiTH],[RoadTH],[SoiEN],[RoadEN],[SubDistrictRunningCode]
           ,[DistrictRunningCode],[ProvinceRunningCode],[PostCode],[Floor],[Building],[CreatedDate],[ModifiedDate] 
    FROM [dbo].[vw_UnitTransfered]  a
    WHERE a.ProjectCode IN ('20009','70027','40022','40039')
    """

    # Setup Format File Name CSV
    date_fmt = datetime.now().strftime("%Y%m%d")
    file_type = ".csv"
    file_name = r"/home/ubuntu/myPython/crm2los/CRM2LOS_Transfer_"
    full_file_name = "{}{}{}".format(file_name, date_fmt, file_type)

    # Read by SQL Statement
    pd.read_sql(sql=str_sql, con=db).to_csv(full_file_name, index=False, encoding="utf-8")


if __name__ == '__main__':
    main()

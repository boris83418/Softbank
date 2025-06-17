import pandas as pd
import pyodbc
import logging
from sendEmail import Email
import datetime

# 設定日誌紀錄（帶有時間戳）
current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
fn = f"logfile_{current_datetime}.log"

# 設定日誌紀錄
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(fn, encoding='utf-8'),  # 使用時間戳命名 log 檔案
        logging.StreamHandler()
    ]
)

# 資料庫連線函數
def connect_to_database(server, database):
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes"
        )
        logging.info(f"成功連線到資料庫: {server}/{database}")
        return conn
    except Exception as e:
        logging.error(f"資料庫連線失敗: {e}")
        raise

# 創建資料庫表格
def create_or_clear_table(cursor, table_name, column_mappings, sheet_name):
    try:
        cursor.execute(f"IF OBJECT_ID(N'{table_name}', N'U') IS NOT NULL SELECT 1 ELSE SELECT 0")
        table_exists = cursor.fetchone()[0]

        if table_exists == 0:
            sql = f"""
            CREATE TABLE {table_name} (
            """
            for excel_col, (db_col, db_type) in column_mappings[sheet_name].items():
                if sheet_name == 'Orderinfo' and excel_col == 'OrderinfoNumber':
                    sql += f"OrderinfoNumber INT IDENTITY(1,1) PRIMARY KEY,\n"  # 自動增量主鍵
                elif (sheet_name == 'Customer Code' and excel_col == 'ASP施工店') or \
                   (sheet_name == 'FactoryShipment' and excel_col == 'PartNo_ETA_FLTC') or \
                   (sheet_name == 'Productinfo' and excel_col == 'Delta_PartNO'):
                    sql += f"[{db_col}] {db_type} PRIMARY KEY,\n"
                else:
                    sql += f"[{db_col}] {db_type},\n"
            sql = sql.rstrip(',\n') + "\n);"
            cursor.execute(sql)
            logging.info(f"{sheet_name} 表格建立完成")
        else:
            sql = f"DELETE FROM {table_name};"
            cursor.execute(sql)
            logging.info(f"{sheet_name} 表格資料已清除")

    except Exception as e:
        logging.error(f"處理表格 {table_name} 失敗: {e}")
        raise


# 動態生成 INSERT 語句
def generate_insert_sql(table_name, df, column_mappings, sheet_name):
    columns = [f"[{column_mappings[sheet_name][col][0]}]" for col in df.columns if col in column_mappings[sheet_name]]
    placeholders = ", ".join(["?"] * len(columns))
    return f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

# 插入資料
def insert_data(cursor, table_name, df, insert_sql):
    for index, row in df.iterrows():
        row = row.apply(lambda x: None if pd.isna(x) else x)  # 將 NaN 轉換為 None
        try:
            if table_name == 'dbo.SoftBank_Data_Orderinfo':
                logging.info(f"成功插入資料: {row.to_dict()}")
            cursor.execute(insert_sql, tuple(row))

        except pyodbc.IntegrityError:
            logging.warning(f"跳過重複主鍵值: {row.iloc[0]}")
            continue
        except Exception as e:
            logging.error(f"插入資料時出錯 (行 {index}): {e} - 資料: {row.to_dict()}")
            continue

    
# 主處理函數
def process_excel_to_sql_with_selection(excel_file_path, table_mapping, column_mappings):
    try:
        # 提示使用者選擇更新目標
        print("請選擇要更新的資料表：")
        print("1. Orderinfo")
        print("2. Productinfo")
        print("3. FactoryShipment")
        print("4. CustomerCode")
        print("5. All")
        
        choice = input("輸入選項編號 (1-5): ").strip()
        if choice not in ["1", "2", "3", "4", "5"]:
            print("無效的選擇，請重新執行程式。")
            return
        
        selected_tables = []
        if choice == "1":
            selected_tables = ["Orderinfo"]
        elif choice == "2":
            selected_tables = ["Productinfo"]
        elif choice == "3":
            selected_tables = ["FactoryShipment"]
        elif choice == "4":
            selected_tables = ["Customer Code"]
        elif choice == "5":
            selected_tables = list(table_mapping.keys())  # 更新所有表

        conn = connect_to_database('jpdejitdev01', 'ITQAS2')
        cursor = conn.cursor()

        for sheet_name in selected_tables:
            table_name = table_mapping[sheet_name]
            logging.info(f"處理工作表: {sheet_name}")
            df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
            df.columns = df.columns.str.strip()

            if sheet_name == 'FactoryShipment':
                # 資料處理邏輯與之前一致
                date_columns = ['PO_Date', 'Actual_Ex_fac_date', 'ETD_SH', 'ETA_FLTC', 'Original_ETA']
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                df['Qty'] = pd.to_numeric(df['Qty'], errors='coerce')
                df['ETA_Year'] = df['ETA_Year'].fillna(df['ETA_FLTC'].dt.year.astype(str))
                df['Part_No'] = df['Part_No'].astype(str).str.strip()
                df['PartNo_ETA_FLTC'] = df['Part_No'].astype(str) + df['ETA_FLTC'].astype(str)
                df = df.groupby('PartNo_ETA_FLTC', as_index=False).agg({
                    'PO_Date': 'first', 'Item': 'first', 'Qty': 'sum', 'PO_NO': 'first',
                    'Part_No': 'first', 'Actual_Ex_fac_date': 'first', 'ETD_SH': 'first',
                    'ETA_FLTC': 'first', 'Original_ETA': 'first', 'ship_method': 'first',
                    'ETA_Year': 'first', 'Status': 'first'
                })

            create_or_clear_table(cursor, table_name, column_mappings, sheet_name)
            insert_sql = generate_insert_sql(table_name, df, column_mappings, sheet_name)
            insert_data(cursor, table_name, df, insert_sql)

        conn.commit()
        logging.info("選定的資料已成功寫入資料庫")

    except Exception as e:
        logging.error(f"處理過程中出現錯誤: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    excel_file_path = r'\\jpdejstcfs01\\STC_share\\JP IT\STC SBK 仕分けリスト\\IT system\\SoftBankData_DBusing_test.xlsx'
    table_mapping = {
        'Customer Code': 'dbo.SoftBank_Data_CustomerCode',
        'FactoryShipment': 'dbo.SoftBank_Data_FactoryShipment',
        'Orderinfo': 'dbo.SoftBank_Data_Orderinfo',
        'Productinfo': 'dbo.SoftBank_Data_Productinfo'
    }
    column_mappings = {
        'Customer Code': {
            'ASP施工店': ('ASP', 'NVARCHAR(255)'), 
            'Customer code': ('Customer_code', 'NVARCHAR(255)')
        },
        'FactoryShipment': {
            'PartNo_ETA_FLTC': ('PartNo_ETA_FLTC', 'NVARCHAR(255)'),
            'PO_Date': ('PO_Date', 'DATE'),
            'Item': ('Item', 'NVARCHAR(255)'),
            'PO_NO': ('PO_NO', 'NVARCHAR(255)'),
            'Part_No': ('Part_No', 'NVARCHAR(255)'),
            'Qty': ('Qty', 'INT'),
            'Actual_Ex_fac_date': ('Actual_Ex_fac_date', 'DATE'),
            'ETD_SH': ('ETD_SH', 'DATE'),
            'ETA_FLTC': ('ETA_FLTC', 'DATE'),
            'Original_ETA': ('Original_ETA', 'DATE'),
            'ship_method': ('ship_method', 'NVARCHAR(255)'),
            'ETA_Year': ('ETA_Year', 'NVARCHAR(255)'),
            'Status': ('Status', 'NVARCHAR(255)')
        },
        'Orderinfo': {
            'OrderinfoNumber': ('OrderinfoNumber', 'INT IDENTITY(1,1) PRIMARY KEY'),
            '見積書回答状況':('Quotation_reply_status','NVARCHAR(255)'),
            '注文日': ('Order_Date', 'DATE'),
            'DEJ見積り番号': ('DEJ_Estimate_Number', 'NVARCHAR(255)'),
            '注文書': ('Quotation_status', 'NVARCHAR(255)'),
            '實際出荷日': ('Actual_Shipment_Date', 'DATE'),
            '預計出荷日': ('Estimated_Shipment_Date', 'DATE'),
            '納品日': ('Delivery_Date', 'DATE'),
            '希望納期': ('Desired_Delivery_Date', 'DATE'),
            '工事名/局名': ('Station_Name', 'NVARCHAR(255)'),
            '品名・規格': ('Product_Name', 'NVARCHAR(255)'),
            '台数': ('Quantity', 'INT'),
            '発注先': ('OrdererLocation', 'NVARCHAR(255)'),
            '担当者': ('Person_in_Charge', 'NVARCHAR(255)'),
            '送り先': ('Recipient', 'NVARCHAR(255)'),
            '連絡人': ('Contact_Person', 'NVARCHAR(255)'),
            '住所': ('Contact_Address', 'NVARCHAR(255)'),
            '電話': ('ContactPhone', 'NVARCHAR(255)'),
            '註': ('ContactNotes', 'NVARCHAR(255)'),
            'SO＃': ('SO_NO', 'NVARCHAR(255)'),
            'DN＃': ('DN_NO', 'NVARCHAR(255)'),
            '送り状番号': ('Invoice_Number', 'NVARCHAR(255)')
        },
        'Productinfo': {
            'Delta_PartNO': ('Delta_PartNO', 'NVARCHAR(255)'),
            'Category': ('Category', 'NVARCHAR(255)'),
            'Customer_Model_Name': ('Customer_Model_Name', 'NVARCHAR(255)'),
            'Model': ('Model', 'NVARCHAR(255)'),
            '税抜単価': ('UnitPrice', 'INT'),
            '標準納期': ('Standard_Delivery_Time', 'INT'),
            '月末SAP庫存': ('Month-End_SAP_Inventory', 'INT')
        }
    }
try:
    process_excel_to_sql_with_selection(excel_file_path, table_mapping, column_mappings)
    sender_email = "SRV.ITREMIND.RBT@deltaww.com"
    password = "Dej1tasd"
    email = Email()
    subject = f"SoftBank_Update_dataBase"
    body=f"SoftBank_Update_dataBase"
    for u in ['boris.wang@deltaww.com']:
        email.send_email(sender_email, password, u, subject, body, fn)  # 發送含有附件的 Email

except Exception as e:
        logging.error(f"程式執行失敗: {e}")

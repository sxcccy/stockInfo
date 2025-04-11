import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def get_single_stock_data(stock, start_date, end_date, result):
    """获取单支股票数据"""
    # 兼容A股和H股的不同字段名称
    code = ''.join(c for c in stock.get('代码', stock.get('symbol', '')) if c.isdigit())
    name = stock.get('名称', stock.get('name', ''))
    current_price = stock.get('最新价', stock.get('price', 0))
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 根据股票类型选择不同的历史数据接口
            if code.startswith(('0', '3', '6')):  # A股代码
                try:
                    hist_data = ak.stock_zh_a_hist(symbol=code, period="daily", 
                    start_date=start_date, end_date=end_date)
                except:
                    hist_data = ak.stock_zh_a_hist(symbol=code, period="daily", 
                    start_date=start_date, end_date=end_date, adjust="hfq")
            else:  # H股代码
                hist_data = ak.stock_hk_hist(symbol=code, period="daily", 
                start_date=start_date, end_date=end_date)
                
            max_price = hist_data['收盘'].max()
            min_price = hist_data['收盘'].min()
            
            result.loc[len(result)] = [code, name, current_price, max_price, min_price]
            break
        except Exception as e:
            retry_count += 1
            print(f"获取股票{code}数据失败(尝试 {retry_count}/{max_retries}): {e}")
            if retry_count == max_retries:
                print(f"无法获取股票{code}数据，跳过该股票")

def get_stock_data():
    """获取A股和H股所有股票2年内最高价、最低价和当前价"""
    # 获取当前日期和2年前日期
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y%m%d')
    
    # 获取所有A股股票列表
    try:
        a_stock_list = ak.stock_zh_a_spot()
    except Exception as e:
        print(f"使用stock_zh_a_spot获取数据失败: {e}")
        print("尝试使用stock_zh_a_spot_em接口...")
        a_stock_list = ak.stock_zh_a_spot_em()
    
    # 获取所有H股股票列表
    try:
        h_stock_list = ak.stock_hk_spot()
    except Exception as e:
        print(f"使用stock_hk_spot获取H股数据失败: {e}")
        print("尝试使用stock_hk_spot_em接口...")
        try:
            h_stock_list = ak.stock_hk_spot_em()
        except Exception as e:
            print(f"获取H股数据失败: {e}")
            h_stock_list = pd.DataFrame()
    
    # 准备结果DataFrame
    a_result = pd.DataFrame(
        columns=pd.Index(['股票代码', '股票名称', '当前价', '2年最高价', '2年最低价']))
    h_result = pd.DataFrame(
        columns=pd.Index(['股票代码', '股票名称', '当前价', '2年最高价', '2年最低价']))
    
    # 处理A股数据
    if not a_stock_list.empty:
        a_total = len(a_stock_list)
        a_lock = threading.Lock()
        
        with ThreadPoolExecutor() as executor:
            futures = []
            for i, (_, stock) in enumerate(a_stock_list.iterrows(), 1):
                futures.append(executor.submit(get_single_stock_data, stock, start_date, end_date, a_result))
                
            for i, future in enumerate(as_completed(futures), 1):
                future.result()
                with a_lock:
                    print(f"A股进度: {i}/{a_total} ({(i/a_total)*100:.1f}%)")
    
    # 处理H股数据
    if not h_stock_list.empty:
        h_total = len(h_stock_list)
        h_lock = threading.Lock()
        
        with ThreadPoolExecutor() as executor:
            futures = []
            for i, (_, stock) in enumerate(h_stock_list.iterrows(), 1):
                futures.append(executor.submit(get_single_stock_data, stock, start_date, end_date, h_result))
                
            for i, future in enumerate(as_completed(futures), 1):
                future.result()
                with h_lock:
                    print(f"H股进度: {i}/{h_total} ({(i/h_total)*100:.1f}%)")
    
    return a_result, h_result

def save_to_excel(a_data, h_data):
    """将数据保存到Excel文件"""
    # 获取当前日期作为文件名后缀
    date_str = datetime.now().strftime('%Y%m%d')
    
    # 保存A股数据
    if not a_data.empty:
        a_filename = f'a_stock_info_{date_str}.xlsx'
        a_data.to_excel(a_filename, index=False)
        print(f"A股数据已保存到{a_filename}")
    
    # 保存H股数据
    if not h_data.empty:
        h_filename = f'h_stock_info_{date_str}.xlsx'
        h_data.to_excel(h_filename, index=False)
        print(f"H股数据已保存到{h_filename}")

if __name__ == '__main__':
    a_stock_data, h_stock_data = get_stock_data()
    save_to_excel(a_stock_data, h_stock_data)
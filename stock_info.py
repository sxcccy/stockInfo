import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def get_single_stock_data(stock, start_date, end_date, result):
    """获取单支股票数据"""
    code = ''.join(c for c in stock['代码'] if c.isdigit())
    name = stock['名称']
    current_price = stock['最新价']
    
    try:
        hist_data = ak.stock_zh_a_hist(symbol=code, period="daily", 
        start_date=start_date, end_date=end_date)
        max_price = hist_data['收盘'].max()
        min_price = hist_data['收盘'].min()
        
        result.loc[len(result)] = [code, name, current_price, max_price, min_price]
    except Exception as e:
        print(f"获取股票{code}数据失败: {e}")

def get_stock_data():
    """获取A股所有股票2年内最高价、最低价和当前价"""
    # 获取当前日期和2年前日期
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y%m%d')
    
    # 获取所有A股股票列表
    try:
        stock_list = ak.stock_zh_a_spot()
    except Exception as e:
        print(f"使用stock_zh_a_spot获取数据失败: {e}")
        print("尝试使用stock_zh_a_spot_em接口...")
        stock_list = ak.stock_zh_a_spot_em()
    
    # 准备结果DataFrame
    result = pd.DataFrame(
        columns=pd.Index(['股票代码', '股票名称', '当前价', '2年最高价', '2年最低价']))
    
    total_stocks = len(stock_list)
    lock = threading.Lock()
    
    with ThreadPoolExecutor() as executor:
        futures = []
        for i, (_, stock) in enumerate(stock_list.iterrows(), 1):
            futures.append(executor.submit(get_single_stock_data, stock, start_date, end_date, result))
            
        for i, future in enumerate(as_completed(futures), 1):
            future.result()
            with lock:
                print(f"进度: {i}/{total_stocks} ({(i/total_stocks)*100:.1f}%)")
    
    return result

def save_to_excel(data, filename='stock_info.xlsx'):
    """将数据保存到Excel文件"""
    data.to_excel(filename, index=False)
    print(f"数据已保存到{filename}")

if __name__ == '__main__':
    stock_data = get_stock_data()
    save_to_excel(stock_data)
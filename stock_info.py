import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def get_single_stock_data(stock, start_date, end_date, result):
    """获取单支股票数据"""
    # 兼容A股的不同字段名称
    code = ''.join(c for c in stock.get('代码', stock.get('symbol', '')) if c.isdigit())
    name = stock.get('名称', stock.get('name', ''))
    current_price = stock.get('最新价', stock.get('price', 0))
    
    try:
        # 根据股票类型选择不同的历史数据接口
        if code.startswith(('0', '3', '6')):  # A股代码
            try:
                hist_data = ak.stock_zh_a_hist(symbol=code, period="daily", 
                start_date=start_date, end_date=end_date, adjust="hfq")
            except Exception as e:
                print(f"尝试获取股票{code}的复权数据失败: {e}")
                print(f"尝试获取股票{code}的原始数据...")
                hist_data = ak.stock_zh_a_hist(symbol=code, period="daily", 
                start_date=start_date, end_date=end_date)

            
        max_price = hist_data['收盘'].max()
        min_price = hist_data['收盘'].min()
        
        result.loc[len(result)] = [code, name, current_price, max_price, min_price]
    except Exception as e:
        print(f"获取股票{code}数据失败: {e}")
        print(f"跳过股票{code}")

def get_stock_data():
    """获取A股所有股票2年内最高价、最低价和当前价"""
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
    
    # 准备结果DataFrame
    result = pd.DataFrame(
        columns=pd.Index(['股票代码', '股票名称', '当前价', '2年最高价', '2年最低价']))
    
    # 处理A股数据
    if not a_stock_list.empty:
        total = len(a_stock_list)
        lock = threading.Lock()
        
        with ThreadPoolExecutor() as executor:
            futures = []
            for i, (_, stock) in enumerate(a_stock_list.iterrows(), 1):
                futures.append(executor.submit(get_single_stock_data, stock, start_date, end_date, result))
                
            for i, future in enumerate(as_completed(futures), 1):
                future.result()
                #with lock:
                    #print(f"进度: {i}/{total} ({(i/total)*100:.1f}%)")
    
    return result

def save_to_excel(data):
    """将数据保存到Excel文件"""
    # 获取当前日期作为文件名后缀
    date_str = datetime.now().strftime('%Y%m%d')
    
    # 计算当前价与最高价的百分比
    if not data.empty:
        data['当前价格/最高价'] = (data['当前价'] / data['2年最高价'] * 100).round(2)
    
    # 保存数据
    if not data.empty:
        filename = f'stock_info_{date_str}.xlsx'
        data.to_excel(filename, index=False)
        print(f"数据已保存到{filename}")

if __name__ == '__main__':
    stock_data = get_stock_data()
    save_to_excel(stock_data)
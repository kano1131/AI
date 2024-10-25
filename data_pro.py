import csv
from decimal import Decimal, getcontext
from typing import List, Tuple

import numpy as np
from numpy import ndarray

getcontext().prec = 10  # 设置精度为10位

def read_csv(file_path: str) -> np.ndarray:
    """
    读取CSV文件并转换为Decimal类型
    :param file_path: CSV文件路径
    :return: data - 数据数组
    """
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        headers: List[str] = next(reader)  # 读取表头
        data: np.ndarray = np.array([row for row in reader], dtype=object)
    return data

def save_csv(file_path: str, data: np.ndarray, headers: List[str]) -> None:
    """
    保存数据到CSV文件
    :param file_path: CSV文件路径
    :param data: 数据数组
    :param headers: 表头
    """
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(data)

def process_data(data: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    处理数据
    :param data: 数据数组，类型为 np.ndarray
    :return: Tuple (time, main_data, price_data) - 处理后的数据数组
    """
     
    data = np.delete(data, [0, 3], axis=1)
    data = np.vectorize(Decimal)(data)
    
    time: np.ndarray = data[:, 0]
    buy_price1: np.ndarray = data[:, 5]
    buy_volumes1: np.ndarray = data[:, 6]
    sell_prices1: np.ndarray = data[:, 25]
    sell_volumes1: np.ndarray = data[:, 26]

    buy_1m_price: np.ndarray = data[:, 45]
    sell_1m_price: np.ndarray = data[:, 46]
    buy_1m_volumes: np.ndarray = data[:, 47]
    sell_1m_volumes: np.ndarray = data[:, 48]

    buy_prices: np.ndarray = data[:, 5:25:2]
    buy_volumes: np.ndarray = data[:, 6:26:2]
    sell_prices: np.ndarray = data[:, 25:45:2]
    sell_volumes: np.ndarray = data[:, 26:46:2]

    total_buy_amount: np.ndarray = np.sum(buy_prices * buy_volumes, axis=1)
    total_sell_amount: np.ndarray = np.sum(sell_prices * sell_volumes, axis=1)
    weighted_avg_buy_price: np.ndarray = total_buy_amount / np.sum(buy_volumes, axis=1)
    weighted_avg_sell_price: np.ndarray = total_buy_amount / np.sum(sell_volumes, axis=1)

    weighted_avg_buy_price = np.vectorize(lambda x: x.quantize(Decimal('0.00000001')))(weighted_avg_buy_price)
    weighted_avg_sell_price = np.vectorize(lambda x: x.quantize(Decimal('0.00000001')))(weighted_avg_sell_price)

    main_data: np.ndarray = np.column_stack((buy_volumes1, sell_volumes1, buy_1m_volumes, sell_1m_volumes, total_buy_amount, total_sell_amount))
    price_data: np.ndarray = np.column_stack((buy_price1, sell_prices1, buy_1m_price, sell_1m_price, weighted_avg_buy_price, weighted_avg_sell_price))

    return time, main_data, price_data

def normalize_data(data: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    对数据进行归一化处理
    :param data: 数据数组，类型为 np.ndarray
    :return: Tuple (normalized_data, mean_values) - 归一化后的数据和用于归一化处理的平均值，类型为 np.ndarray
    """

    # 计算前100行的平均值
    mean_values: np.ndarray = np.mean(data[:100], axis=0)

    # 用前100行的平均值对所有数据进行归一化处理
    normalized_data: np.ndarray = data / mean_values

    return normalized_data, mean_values

def standardize_price_data(price_data: np.ndarray) -> Tuple[np.ndarray, Decimal, Decimal]:
    """
    对价格数据进行标准化处理

    :param price_data: 价格数据数组，类型为 np.ndarray
    :return: Tuple[normalized_price_data: np.ndarray, mean_price: Decimal, custom_std_price: Decimal] -
             标准化后的价格数据和用于标准化处理的均值和自定义标准差
    """
    # 计算前100行的均值
    mean_price: Decimal = np.mean(price_data[:100], axis=0)

    # 自定义标准差，假设涨跌幅度在50%内
    custom_std_price: Decimal = mean_price * Decimal('0.5')

    # 将价格数据归一化到0-1之间，以0.5为中心
    normalized_price_data: np.ndarray = (price_data - mean_price) / custom_std_price * Decimal('0.5') + Decimal('0.5')

    return normalized_price_data, mean_price, custom_std_price

def reverse_normalize(normalized_data: np.ndarray, mean_values: np.ndarray) -> np.ndarray:
    """
    反推归一化处理的数据回原始数据
    :param normalized_data: 归一化后的数据，类型为 np.ndarray
    :param mean_values: 用于归一化处理的平均值，类型为 np.ndarray
    :return: 原始数据，类型为 np.ndarray
    """
    # 将 normalized_data 和 mean_values 转换为 Decimal 类型
    normalized_data = np.vectorize(Decimal)(normalized_data)
    mean_values = np.vectorize(Decimal)(mean_values)
    return normalized_data * mean_values

def reverse_standardize(
    normalized_price_data: np.ndarray,
    mean_price: Decimal,
    custom_std_price: Decimal,
) -> np.ndarray:
    """
    反推标准化处理的价格数据回原始数据

    :param normalized_price_data: 标准化后的价格数据，类型为 np.ndarray
    :param mean_price: 用于标准化处理的均值，类型为 Decimal
    :param custom_std_price: 用于标准化处理的自定义标准差，类型为 Decimal
    :return: 原始价格数据，类型为 np.ndarray
    """
    # 将 normalized_price_data 转换为 Decimal 类型
    normalized_price_data = np.vectorize(Decimal)(normalized_price_data)
    return (normalized_price_data - Decimal('0.5')) * custom_std_price / Decimal('0.5') + mean_price

def normalize_and_save(
    input_file_path: str,
    output_file_path: str,
    a: bool = True,
) -> Tuple[Decimal, np.ndarray]:
    """
    处理CSV数据并保存处理后的结果

    :param input_file_path: 输入文件的路径，类型为 str
    :param output_file_path: 输出文件的路径，类型为 str
    :param a: 用于决定是否进行数据归一化和标准化的标志，类型为 bool
    :return: Tuple (mean_price, mean_values) - 归一化和标准化处理的均值，类型为 Tuple[Decimal, np.ndarray]
    """
    headers: List[str] = ['T','BV1', 'SV1', 'BV1m', 'SV1m', 'TBA', 'TSA', 'B1', 'S1', 'B1m', 'S1m', 'WABP', 'WASP']
    
    data: np.ndarray = read_csv(input_file_path)
    
    time, main_data, price_data = process_data(data)
    
    if a:
        normalized_data, mean_values = normalize_data(main_data)
        normalized_price_data, mean_price, custom_std_price = standardize_price_data(price_data)
        
        final_data: np.ndarray = np.column_stack((time, normalized_data, normalized_price_data))
        
        print("Mean values for normalization:", mean_values)
        print("Mean price for standardization:", mean_price)
        
        save_csv(output_file_path, final_data, headers)
        
        return mean_price, mean_values
    else:
        final_data: np.ndarray = np.column_stack((time, main_data, price_data))
        save_csv(output_file_path, final_data, headers)
        return Decimal('0'), np.array([])

def restore_and_save(
    input_file_path: str,
    output_file_path: str,
    mean_values: np.ndarray,
    mean_price: Decimal,
) -> None:
    """
    反推归一化和标准化处理的数据回原始数据

    :param input_file_path: 输入文件的路径，类型为 str
    :param output_file_path: 输出文件的路径，类型为 str
    :param mean_values: 归一化处理的均值，类型为 np.ndarray
    :param mean_price: 标准化处理的均值，类型为 Decimal
    :return: None
    """
    headers: List[str] = ['T','BV1', 'SV1', 'BV1m', 'SV1m', 'TBA', 'TSA', 'B1', 'S1', 'B1m', 'S1m', 'WABP', 'WASP']
    # 读取CSV文件
    data: np.ndarray = read_csv(input_file_path)
    # 反推归一化和标准化处理的数据回原始数据
    time: np.ndarray =data[:, 0]
    normalized_data: np.ndarray = data[:, 1:7]
    normalized_price_data: np.ndarray = data[:, 7:]
    original_data: np.ndarray = reverse_normalize(normalized_data, mean_values)
    custom_std_price: Decimal = mean_price * Decimal('0.5')
    original_price_data: np.ndarray = reverse_standardize(normalized_price_data, mean_price, custom_std_price)
    # 合并两个数据
    final_data: np.ndarray = np.column_stack((time, original_data, original_price_data))
    # 保存数据到CSV文件
    save_csv(output_file_path, final_data, headers)

def test():
    mean_price, mean_values = normalize_and_save('test/1.csv','test/results1.csv',True)
    
    restore_and_save('test/results1.csv','test/back_results1.csv',mean_values, mean_price)

if __name__ == "__main__":
    mean_price, mean_values = normalize_and_save('D:\\data\\AI110\\data\\1\\data1.csv','D:\\data\\AI110\\data2\\1\\results1.csv',True)
    #mean_values = np.array([Decimal('1876164.64'), Decimal('805175.27'), Decimal('84029105.94'), Decimal('214278092.8'), Decimal('22441.52301'), Decimal('40452.84447')])
    #mean_price = np.array([Decimal('0.001671617'), Decimal('0.001671116'), Decimal('0.00167093432'), Decimal('0.001670284972'), Decimal('0.0016716898'), Decimal('0.0009713214')])
    restore_and_save('D:\\data\\AI110\\data2\\1\\results1.csv','D:\\data\\AI110\\data2\\1\\back_results1.csv',mean_values, mean_price)
    
    

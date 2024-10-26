import csv
import json
import os
import threading
import time


class DataSaver:
    def __init__(self, file_path, columns, interval=60):
        self.file_path = file_path
        self.interval = interval
        self.data_buffer = []
        self.lock = threading.Lock()
        self.running = True
        self.columns = columns
        self.recent_data = []

        # 启动后台线程
        self.thread = threading.Thread(target=self._save_data_periodically)
        self.thread.start()

    def add_data(self, data):
        with self.lock:
            self.data_buffer.append(data)

    def _save_data_periodically(self):
        while self.running:
            time.sleep(self.interval)
            self._save_data_to_file()

    def _save_data_to_file(self):
        with self.lock:
            if not self.data_buffer:
                return

            with open(self.file_path, 'a', newline='', encoding='utf8') as f:
                writer = csv.writer(f)
                if os.path.getsize(self.file_path) == 0:
                    writer.writerow(self.columns)
                for data in self.data_buffer:
                    if data not in self.recent_data:
                        writer.writerow(data)
                        self.recent_data.append(data)
            self.data_buffer = []

    def stop(self):
        self.running = False
        self.thread.join()
        self._save_data_to_file()

def process_json_to_list(json_data):
    data_dict = json.loads(json_data)
    event_type = data_dict.get('e')

    combined_list = [value for key, value in data_dict.items() if key not in ('b', 'a')]

    if event_type == 'depthUpdate':
        bids = data_dict['b']
        asks = data_dict['a']
        for bid, ask in zip(bids, asks):
            combined_list.extend([bid[0], bid[1], ask[0], ask[1]])
        time_re = combined_list[2]
    elif event_type == 'aggTrade':
        combined_list.extend([data_dict['a']])
        combined_list=[combined_list[i] for i in [1, 3, 4, 8]]
        combined_list[3] = 1 if combined_list[3] == True else 0
        time_re = combined_list[0]
        pass 
    else:
        raise ValueError(f"Unknown event type: {event_type}")

    return combined_list,event_type,time_re

if __name__ == "__main__":
    # 示例使用
    columns1 = ["e", "E", "T", "s", "U", "u", "pu", 
                "b1", "b1v", "b2", "b2v", "b3", "b3v", "b4", "b4v", "b5", "b5v",
                "b6", "b6v", "b7", "b7v", "b8", "b8v", "b9", "b9v", "b10", "b10v",
                "a1", "a1v", "a2", "a2v", "a3", "a3v", "a4", "a4v", "a5", "a5v", 
                "a6", "a6v", "a7", "a7v", "a8", "a8v", "a9", "a9v", "a10", "a10v"]
    columns2 = ["e", "E", "a", "s", "p", "q", "f", "l", "T", "m"]

    data_saver1 = DataSaver("data1.csv", columns1, interval=10)
    data_saver2 = DataSaver("data2.csv", columns2, interval=10)


    # 示例JSON数据
    data1 = '''{
    "e":"depthUpdate",
    "E":1729270072134,
    "T":1729270072113,
    "s":"BNBUSDT",
    "U":5555693039345,
    "u":5555693045490,
    "pu":5555693030957,
    "b":[["601.100","56.37"],["601.090","27.26"],["601.080","26.44"],["601.070","20.13"],["601.060","5.76"],["601.050","27.25"],["601.040","28.77"],["601.030","0.03"],["601.020","26.95"],["601.010","41.22"]],
    "a":[["601.110","4.74"],["601.120","0.17"],["601.130","0.05"],["601.140","4.99"],["601.150","6.20"],["601.160","32.33"],["601.170","6.51"],["601.180","5.83"],["601.190","0.72"],["601.200","12.01"]]
    }'''

    data2='''{
    "e":"aggTrade",
    "E":1729280821838,
    "a":612250785,
    "s":"BNBUSDT",
    "p":"599.600",
    "q":"0.01",
    "f":1425511795,
    "l":1425511795,
    "T":1729280821684,
    "m":false
    }'''

    # 添加数据到DataSaver
    data_list1,_ = process_json_to_list(data1)
    data_list2,_ = process_json_to_list(data2)

    #print(data_list2)
    data_saver1.add_data(data_list1)
    data_saver2.add_data(data_list2)

    # 停止DataSaver
    time.sleep(15)  # 等待一段时间以确保数据保存
    data_saver1.stop()
    data_saver2.stop()

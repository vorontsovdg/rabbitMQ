import pika
import json
import pandas as pd
import numpy as np

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='y_true')
channel.queue_declare(queue='y_pred')


def rmse(y_true, y_pred):
    return (np.sum((y_true - y_pred) ** 2) / len(y_true)) ** 0.5


def callback(ch, method, properties, body):
    data = json.loads(body)
    uid = data[0]
    y = data[1]
    print(f'Из очереди {method.routing_key} получено значение {y}')
    if method.routing_key == 'y_pred':
        with open('./y_pred.csv', 'a') as f:
            f.write(f'{uid},{y}\n')
    else:
        with open('./y_true.csv', 'a') as f:
            f.write(f'{uid},{y}\n')


def callback_rmse(ch, method, properties, body):
    df1 = pd.read_csv('./y_true.csv', sep=',', header=None, names=['uid', 'y_true'])
    df2 = pd.read_csv('./y_pred.csv', sep=',', header=None, names=['uid', 'y_pred'])
    df1 = df1.merge(df2, on='uid', how='inner')
    metric = rmse(df1['y_true'], df1['y_pred'])
    print(f'Текущая ошибка: {metric}')


channel.basic_consume(
    queue='y_pred',
    on_message_callback=callback,
    auto_ack=True)

channel.basic_consume(
    queue='y_true',
    on_message_callback=callback,
    auto_ack=True)

channel.basic_consume(
    queue='rmse',
    on_message_callback=callback_rmse,
    auto_ack=True)

print('...Ожидание сообщений, для выхода нажмите CTRL+C')
channel.start_consuming()

import pika
import pickle
import numpy as np
import json


with open('./myfile.pkl', 'rb') as pkl_file:
    regressor = pickle.load(pkl_file)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='Features')


def callback(ch, method, properties, body):
    print(f'Получен вектор признаков {json.loads(body)}')
    data = json.loads(body)
    input_data = np.array(data[1:]).reshape(1, -1)
    uid = data[0]
    result=regressor.predict(input_data)
    output = [uid]
    output.append(result[0])
    channel.queue_declare(queue='y_pred')
    channel.basic_publish(exchange='',
                          routing_key='y_pred',
                          body=json.dumps(output))
    print(f'Предсказанный результат {result[0]} отправлен в очередь')






#on message callback
channel.basic_consume(
    queue='Features',
    on_message_callback=callback,
    auto_ack=True)

print('...Ожидайте сообщения, для выхода нажмите CTRL+C')
channel.start_consuming()

import numpy as np
import pika
import json
from sklearn.datasets import load_diabetes
import time
import uuid

X, y = load_diabetes(return_X_y=True)


def main():
    counter = 0
    while True:
        counter += 1
        random_row = np.random.randint(0, X.shape[0] - 1)
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        uid = str(uuid.uuid4())
        channel = connection.channel()

        channel.queue_declare(queue='Features')
        body=[uid]
        body.extend(list(X[random_row]))
        channel.basic_publish(exchange='',
                              routing_key='Features',
                              body=json.dumps(body))
        print(json.dumps(body))
        print('Вектор признаков Х отправлен в очередь')

        channel.queue_declare(queue='y_true')
        body=[uid]
        body.append(y[random_row])
        channel.basic_publish(exchange='',
                              routing_key='y_true',
                              body=json.dumps(body))
        print(json.dumps(body))
        print('Сообщение успешно отправлено')

        if counter % 10 == 0:
            channel.queue_declare(queue='rmse')
            channel.basic_publish(exchange='',
                                  routing_key='rmse',
                                  body=json.dumps(['rmse']))
        connection.close()
        time.sleep(3)


if __name__ == '__main__':
    main()

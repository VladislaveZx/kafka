-  Создай структуру папок и файлов:

```
/project-root /producer - producer.py - requirements.txt - Dockerfile docker-compose.yml
```

-  В консоли в корне проекта выполни сборку и запуск с нужной скоростью (например 10 сообщений в секунду на каждый топик):

```
bash
RPS=10 docker-compose up --build
```

-  Если не задавать `RPS`, по умолчанию будет 1 сообщение в секунду на каждый топик:

```bash
bash
docker-compose up --build
```

-  Чтобы изменить скорость -- просто переопредели переменную `RPS` при запуске.



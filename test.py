import socket

def check_kafka_connection(host='localhost', port=29092, timeout=3):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            print(f"✅ Подключение к {host}:{port} успешно")
        return True
    except Exception as e:
        print(f"❌ Не удалось подключиться к {host}:{port}: {e}")
        return False

if __name__ == "__main__":
    check_kafka_connection()
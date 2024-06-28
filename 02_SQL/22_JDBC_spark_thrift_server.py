from pyhive import hive
if __name__ == '__main__':
    # 获取到spark ThriftServer连接
    conn = hive.Connection(
        host="10.3.15.36",
        port=10000,
        username="root",
    )
    cursor = conn.cursor()
    cursor.execute("show databases")
    result = cursor.fetchall()
    print(result)
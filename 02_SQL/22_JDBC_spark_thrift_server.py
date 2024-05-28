from pyhive import hive
if __name__ == '__main__':
    # 获取到spark ThriftServer连接
    conn = hive.Connection(
        host="node5",
        port=10000,
        username="root"
    )
    cursor = conn.cursor()
    cursor.execute("show databases")
    result = cursor.fetchall()
    print(result)
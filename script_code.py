def connect_and_etract():
    import psycopg2
    import pandas as pd
    # Thông tin kết nối cơ sở dữ liệu
    HOST = "localhost"
    DATABASE = "database-name"
    USER = "postgres"
    PASSWORD = ""
    PORT = 5432  # Cổng mặc định của PostgreSQL
    try:
    # Kết nối đến PostgreSQL
        connection = psycopg2.connect(
            host=HOST,
            database=DATABASE,
            user=USER,
            password= 'fill_password',
            port=PORT
    )
        print("Kết nối thành công đến PostgreSQL!")
    # Tạo con trỏ để thực hiện truy vấn
        cursor = connection.cursor()
    # Câu truy vấn SQL
        query = "SELECT * FROM cust;"  # Thay 'your_table_name' bằng tên bảng của bạn
        # Sử dụng pandas để đọc dữ liệu trực tiếp vào DataFrame
        df = pd.read_sql_query(query, connection)
    # Hiển thị DataFrame
        print(df)
    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")
    finally:
    # Đóng kết nối
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()
            print("Đã đóng kết nối đến PostgreSQL.")
    return(df)
# Save the PostpreSQL Data to df
df = connect_and_etract()
# Change data from Pandas version to Pyspark version
spark = SparkSession.builder.appName("Pandas to PySpark").getOrCreate()
spark_df = spark.createDataFrame(df)
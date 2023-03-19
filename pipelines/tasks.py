class BaseTask:
    """Base Pipeline Task"""

    def run(self):
        raise RuntimeError('Do not run BaseTask!')

    def short_description(self):
        pass

    def __str__(self):
        task_type = self.__class__.__name__
        return f'{task_type}: {self.short_description()}'

import csv
class CopyToFile(BaseTask):
    """Copy table data to CSV file"""

    def __init__(self, table, output_file):
        self.table = table
        self.output_file = output_file

    def short_description(self):
        return f'{self.table} -> {self.output_file}'

    def run(self):
        # Параметры подключения к базе данных MySQL
        config = {
            'user': 'root',
            'password': 'admin',
            'host': 'db',
            'database': 'pipelinedb'
        }

        # Подключение к базе данных
        con = mysql.connector.connect(**config)
        cur = con.cursor()

        # Получение данных из таблицы
        data = [('id', 'name', 'url', 'domain_of_url')]
        cur.execute(f"SELECT * FROM {self.table}")
        for row in cur.fetchall():
            data.append(row)

        # Запись данных в CSV-файл
        with open(self.output_file + '.csv', 'w', newline='') as myFile:
            writer = csv.writer(myFile)
            writer.writerows(data)

        # Закрытие соединения с базой данных
        cur.close()
        con.close()

        print(f"Copy table `{self.table}` to file `{self.output_file}`")


class LoadFile(BaseTask):
    """Load file to table"""

    def __init__(self, table, input_file):
        self.table = table
        self.input_file = input_file

    def short_description(self):
        return f'{self.input_file} -> {self.table}'

    def run(self):
        # Параметры подключения к базе данных MySQL
        config = {
            'user': 'root',
            'password': 'admin',
            'host': 'db',
            'port': '3306',
            'database': 'pipelinedb'
        }

        con = mysql.connector.connect(**config)
        cur = con.cursor()

        data = []
        with open(self.input_file, newline='') as File:
            reader = csv.reader(File)
            for row in reader:
                if (row[0] != 'id'):
                    temp = (row[0], row[1], row[2])
                    data.append(temp)

        cur.execute(f"CREATE TABLE {self.table} (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(45) NULL, url VARCHAR(45) NULL, PRIMARY KEY (id), UNIQUE INDEX id_UNIQUE (id ASC) VISIBLE);")
        cur.executemany(f"INSERT INTO {self.table} VALUES (%s, %s, %s)", data)
        con.commit()

        # Закрытие соединения с базой данных
        cur.close()
        con.close()

        print(f"Load file `{self.input_file}` to table `{self.table}`")

import mysql.connector
class RunSQL(BaseTask):
    """Run custom SQL query"""

    def __init__(self, sql_query, title=None):
        self.title = title
        self.sql_query = sql_query

    def short_description(self):
        return f'{self.title}'

    def run(self):
        # Подключение к базе данных MySQL
        db = mysql.connector.connect(
            host="db",
            user="root",
            password="admin",
            database="pipelinedb"
        )

        # Создание курсора для выполнения запросов
        cursor = db.cursor()

        # Выполнение SQL-запроса
        cursor.execute(self.sql_query)

        # Применение изменений к базе данных
        db.commit()

        # Вывод информации о выполненном запросе
        print(f"Run SQL ({self.title}):\n{self.sql_query}")

        # Закрытие соединения с базой данных
        db.close()



class CTAS(BaseTask):
    """SQL Create Table As Task"""

    def __init__(self, table, sql_query, title=None):
        self.table = table
        self.sql_query = sql_query
        self.title = title or table

    def short_description(self):
        return f'{self.title}'

    def run(self):
        con = mysql.connector.connect(
            host="db",
            user="root",
            password="admin",
            database="pipelinedb"
        )
        cur = con.cursor()

        # Define the SQL query to check if the function exists
        sql_check_function = "SHOW FUNCTION STATUS WHERE name = 'domain_of_url'"

        # Execute the SQL query to check if the function exists
        cur.execute(sql_check_function)
        function_result = cur.fetchone()

        # Check if the function exists
        if function_result is None:
            # Define the SQL query to create the function as a string
            sql_create_function = """
                    CREATE FUNCTION domain_of_url(url TEXT) RETURNS TEXT DETERMINISTIC
                    BEGIN
                        DECLARE res TEXT;
                        SET res = SUBSTRING_INDEX(SUBSTRING_INDEX(url, '://', -1), '/', 1);
                        RETURN res;
                    END
                """

            # Execute the SQL query to create the function
            cur.execute(sql_create_function)

        # Create the temporary table and add the domain column using the function
        cur.execute("CREATE TEMPORARY TABLE tmp_table AS " + self.sql_query)
        cur.execute(f"CREATE TABLE {self.table} SELECT * FROM tmp_table")

        con.commit()

        print(f"Create table `{self.table}` as SELECT:\n{self.sql_query}")

        cur.close()
        con.close()
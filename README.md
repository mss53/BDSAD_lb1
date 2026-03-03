# Практическая работа 1 — Вариант 19 (HR‑аналитика)

## Описание задачи и выбранного варианта

В рамках практической работы 1 по дисциплине «Обработка и анализ данных с использованием Apache Spark (PySpark)» выполняется **вариант 19 — HR‑аналитика**.

Предметная область — анализ эффективности сотрудников по данным о выработке и отработанном времени. Требуется:

- загрузить данные о сотрудниках в HDFS и считать их в PySpark;
- очистить выборку от уволенных сотрудников;
- рассчитать показатели производительности и стажа по отделам;
- визуализировать полученные результаты и сделать выводы для HR‑аналитики.

В качестве исходных данных используется файл: `Extended_Employee_Performance_and_Productivity_Data.csv`

Выбранные ключевые поля:

- `Department` — отдел;
- `Monthly_Salary` — месячная заработная плата;
- `Work_Hours_Per_Week` — количество отработанных часов в неделю;
- `Years_At_Company` — стаж работы в компании (в годах);
- `Resigned` — булевый признак увольнения сотрудника.

## Инструкция по запуску

### 1. Подготовка окружения (Ubuntu VM)

1. Запустить виртуальную машину с образом `ds_mgpu_Hadoop3+spark_3_4`.
2. Перейти в терминале под пользователем `hadoop`:

   ```bash
   sudo su - hadoop
   ```

3. Запустить службы Hadoop:

   ```bash
   start-dfs.sh
   start-yarn.sh
   jps
   ```

   Убедиться, что запущены процессы NameNode, DataNode, ResourceManager и NodeManager.

### 2. Загрузка данных в HDFS

1. Скопировать CSV‑файл на виртуальную машину (например, в каталог `/home/hadoop/data`).
2. Создать директорию для лабораторной и загрузить данные в HDFS:

   ```bash
   hdfs dfs -mkdir -p /user/hadoop/lab_01/input
   hdfs dfs -chmod 775 /user/hadoop/lab_01

   mkdir -p /home/hadoop/data
   hdfs dfs -put /home/hadoop/data/Extended_Employee_Performance_and_Productivity_Data.csv \
     /user/hadoop/lab_01/input/

   hdfs dfs -ls /user/hadoop/lab_01/input/
   ```

### 3. Запуск Jupyter Notebook и выполнение анализа

1. Запустить Jupyter Notebook от пользователя `hadoop` (команда зависит от настройки окружения, например):

   ```bash
   jupyter notebook
   ```

2. В веб‑интерфейсе Jupyter открыть файл:

- `lab_01.ipynb`

3. Последовательно выполнить все ячейки ноутбука.

4. Сохранить ноутбук с выполненными ячейками.

5. По окончании работы остановить службы Hadoop:

   ```bash
   stop-yarn.sh
   stop-dfs.sh
   ```

## Ссылка на источник данных

Для выполнения лабораторной используется датасет `Extended_Employee_Performance_and_Productivity_Data.csv`.

Ссылка на датасет: 'https://www.kaggle.com/datasets/mexwell/employee-performance-and-productivity-data'
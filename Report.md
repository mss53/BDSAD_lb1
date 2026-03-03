# Практическая работа 1 — Вариант 19 (HR‑аналитика)

## 1. Введение

### Цель работы

Целью практической работы является освоение базовых приёмов работы с распределённой файловой системой HDFS и фреймворком Apache Spark (PySpark) на примере задачи HR‑аналитики: загрузка и очистка данных о сотрудниках, расчёт показателей производительности и стажа по отделам, а также визуализация результатов для поддержки управленческих решений.

### Постановка бизнес‑задачи (вариант 19)

Бизнес‑задача варианта 19 формулируется следующим образом:

- загрузить данные о сотрудниках (выработка, часы);
- очистить выборку от уволенных сотрудников;
- рассчитать производительность и средний стаж по отделам;
- подготовить визуализацию, позволяющую сравнивать отделы по эффективности.

Результаты анализа могут быть использованы HR‑подразделением и руководством компании для:

- выявления наиболее эффективных отделов;
- оценки связи между опытом сотрудников и их производительностью;
- принятия решений по обучению, ротации и мотивации персонала.

### Описание данных

В работе используется датасет `Extended_Employee_Performance_and_Productivity_Data.csv` объёмом 100 000 записей, содержащий информацию о сотрудниках компании.

Ключевые поля:

- `Employee_ID` — идентификатор сотрудника;
- `Department` — отдел (IT, Finance, HR, Engineering, Marketing и др.);
- `Gender`, `Age`, `Job_Title` — демографические и должностные характеристики;
- `Hire_Date`, `Years_At_Company` — дата найма и стаж работы в компании;
- `Monthly_Salary` — месячная заработная плата;
- `Work_Hours_Per_Week` — количество отработанных часов в неделю;
- `Performance_Score`, `Employee_Satisfaction_Score` — показатели эффективности и удовлетворённости;
- `Resigned` — булевый признак увольнения (`True` / `False`).

На основе этих полей в ходе анализа дополнительно рассчитывается показатель:

- `productivity_per_hour` — условная производительность сотрудника, определяемая как отношение месячной зарплаты к числу отработанных часов в неделю.

---

## 2. Ход работы

### 2.1. Загрузка данных в HDFS

Работа выполнялась на виртуальной машине с образом `ds_mgpu_Hadoop3+spark_3_4`. Кластер Hadoop был развёрнут под пользователем `hadoop`. Основные команды:

```bash
sudo su - hadoop
start-dfs.sh
start-yarn.sh
jps

hdfs dfs -mkdir -p /user/hadoop/lab_01/input
hdfs dfs -chmod 775 /user/hadoop/lab_01

mkdir -p /home/hadoop/data
hdfs dfs -put /home/hadoop/data/Extended_Employee_Performance_and_Productivity_Data.csv \
  /user/hadoop/lab_01/input/

hdfs dfs -ls /user/hadoop/lab_01/input/
```
<img width="1847" height="766" alt="image" src="https://github.com/user-attachments/assets/5c96b48b-e501-4b93-b930-44de1bd8b46b" />
 Рис. 1. Вывод команды `jps` с запущенными компонентами NameNode, DataNode, ResourceManager, NodeManager; Вывод команды `hdfs dfs -ls /user/hadoop/lab_01/input/` с загруженным CSV‑файлом.

### 2.2. Считывание данных в Spark и предварительная обработка

Данные загружаются из HDFS в объект `DataFrame` PySpark:

```python
hdfs_path = "hdfs://localhost:9000/user/hadoop/lab_01/input/Extended_Employee_Performance_and_Productivity_Data.csv"

hr_df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(hdfs_path)
)

hr_df_raw.printSchema()
hr_df_raw.show(10, truncate=False)
```

Для проверки качества данных выполняется подсчёт количества пропусков по каждому столбцу:

```python
from pyspark.sql.functions import col, when, count

null_counts = hr_df_raw.select([
    count(when(col(c).isNull(), c)).alias(c) for c in hr_df_raw.columns
])

null_counts.show(truncate=False)
```

Результат показывает отсутствие пропусков (`0` для всех колонок).


### 2.3. Очистка выборки и расчёт производительности

В соответствии с вариантом 19 из анализа исключаются уволенные сотрудники (`Resigned = True`), а для оставшихся рассчитывается показатель производительности на час:

```python
STATUS_COL = "Resigned"
DEPT_COL = "Department"
REVENUE_COL = "Monthly_Salary"
HOURS_COL = "Work_Hours_Per_Week"
TENURE_COL = "Years_At_Company"

hr_df_active = hr_df_raw.where(col(STATUS_COL) == False)

hr_df = hr_df_active.withColumn(
    "productivity_per_hour",
    when(col(HOURS_COL) > 0, col(REVENUE_COL) / col(HOURS_COL)).otherwise(None)
)

hr_df.show(10, truncate=False)
```

После фильтрации остается 89 990 записей о действующих сотрудниках. Новая колонка `productivity_per_hour` используется для дальнейшей агрегированной аналитики по отделам.


---

## 3. Анализ данных (Spark SQL)

### 3.1. SQL‑запросы

Для выполнения аналитических запросов датафрейм регистрируется как временное представление `hr_data`:

```python
hr_df.createOrReplaceTempView("hr_data")
```

Основной запрос по варианту 19:

```sql
SELECT
  Department AS department,
  AVG(productivity_per_hour) AS avg_productivity_per_hour,
  AVG(CAST(Years_At_Company AS DOUBLE)) AS avg_tenure_years,
  COUNT(*) AS employees_count
FROM hr_data
GROUP BY Department
ORDER BY avg_productivity_per_hour DESC;
```

### 3.2. Табличные результаты

Результат запроса (пример агрегированных значений по отделам):

| department        | avg_productivity_per_hour | avg_tenure_years | employees_count |
|-------------------|---------------------------|------------------|-----------------|
| Customer Support  | 148.90                    | 4.47             | 10018           |
| Operations        | 148.90                    | 4.47             | 10060           |
| IT                | 148.80                    | 4.44             | 10067           |
| Engineering       | 148.65                    | 4.49             | 9899            |
| HR                | 148.54                    | 4.48             | 9835            |
| Sales             | 148.41                    | 4.47             | 10018           |
| Finance           | 148.30                    | 4.48             | 10020           |
| Marketing         | 148.12                    | 4.50             | 10091           |
| Legal             | 147.99                    | 4.48             | 9982            |


**Интерпретация табличных результатов:**

- показатели средней производительности по отделам отличаются незначительно (около 148 единиц на час);
- при этом наблюдаются небольшие различия в среднем стаже, что может указывать на специфику карьерных траекторий по функциям (например, в Marketing средний стаж чуть выше, чем в IT);
- полученные метрики подтверждают сопоставимый уровень нагрузок и компенсации между отделами в рамках синтетической модели.

---

## 4. Визуализация и бизнес‑интерпретация

### 4.1. Построение графиков

Для визуализации результатов агрегированного анализа используются библиотеки Matplotlib и Seaborn. Агрегированные данные переводятся в формат Pandas DataFrame:

```python
dept_metrics_pd = dept_metrics_df.toPandas()
```

Далее строится комбинированная диаграмма:

```python
plt.figure(figsize=(12, 6))
sns.set(style="whitegrid")

dept_metrics_pd_sorted = dept_metrics_pd.sort_values("avg_productivity_per_hour", ascending=False)

ax = sns.barplot(
    data=dept_metrics_pd_sorted,
    x="department",
    y="avg_productivity_per_hour",
    palette="Blues_d"
)
ax.set_title("Эффективность отделов (производительность на сотрудника/час)")
ax.set_xlabel("Отдел")
ax.set_ylabel("Средняя производительность на час")
plt.xticks(rotation=45, ha="right")

ax2 = ax.twinx()
sns.lineplot(
    data=dept_metrics_pd_sorted,
    x="department",
    y="avg_tenure_years",
    color="red",
    marker="o",
    ax=ax2
)
ax2.set_ylabel("Средний стаж, лет", color="red")
ax2.tick_params(axis="y", labelcolor="red")

plt.tight_layout()
plt.show()
```

---

## 5. Выводы

Анализ итогового графика показывает следующее:

- все отделы демонстрируют **сопоставимый уровень средней производительности** (около 148 условных единиц на час), при этом небольшое преимущество по метрике `avg_productivity_per_hour` имеют подразделения *Customer Support*, *Operations* и *IT*;
- различия в среднем стаже по отделам являются умеренными (в районе 4,4–4,5 лет) и не приводят к резким скачкам производительности: отделы с чуть более высоким стажем (например, *Marketing*, *Engineering*) не показывают драматически лучшую или худшую эффективность;
- график не выявляет ярко выраженных «аутсайдеров» или «лидеров» с аномальными значениями, что в контексте синтетического набора данных свидетельствует о сбалансированной кадровой политике и равномерном распределении нагрузки между отделами.

С практической точки зрения для бизнеса это означает, что:

- текущая структура отделов и политика компенсаций обеспечивают примерно одинаковую отдачу от сотрудников в различных функциях;
- целесообразно дополнительно исследовать внутренние процессы в отделах *Customer Support* и *Operations* как умеренных лидерах по производительности, чтобы понять, какие управленческие практики могут быть распространены на другие подразделения;
- наличие схожего уровня стажа и производительности указывает на то, что точкой дифференциации в реальных условиях могут стать не столько базовые кадровые характеристики, сколько качество управления, корпоративная культура и системы мотивации.

**Применимость Hadoop/Spark для данной задачи**:

- связка Hadoop + Spark позволяет эффективно обрабатывать большие объёмы HR‑данных за счёт распределённого хранения (HDFS) и параллельных вычислений;
- использование PySpark и Spark SQL упрощает выражение сложных агрегирующих запросов и вычисление метрик на уровне отделов;
- интеграция с экосистемой Python (Pandas, Matplotlib, Seaborn) обеспечивает удобство дальнейшей аналитики и визуализации результатов.


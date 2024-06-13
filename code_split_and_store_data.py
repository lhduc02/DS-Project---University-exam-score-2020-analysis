import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, split
from pyspark.sql.types import StringType, FloatType
import pandas as pd
import html
import math


# Đường dẫn tới tệp dữ liệu lớn
input_file = "D:\\.Repo\\Incomplete Project\\Thesis Project --- Spark\\data_test_600M.txt"
output_dir = "D:\\.Repo\\Incomplete Project\\Thesis Project --- Spark\\split_data"

# Tạo thư mục đầu ra nếu chưa tồn tại
os.makedirs(output_dir, exist_ok=True)

# Đọc tệp dữ liệu lớn và chia nhỏ thành các tệp nhỏ hơn
chunk_size = 80000  # Số dòng mỗi tệp nhỏ
with open(input_file, 'r', encoding='utf8') as file:
    lines = file.readlines()
    for i in range(0, len(lines), chunk_size):
        chunk = lines[i:i+chunk_size]
        with open(os.path.join(output_dir, f"chunk_{i//chunk_size}.txt"), 'w', encoding='utf8') as chunk_file:
            chunk_file.writelines(chunk)



# Khởi tạo SparkSession
spark = SparkSession.builder.appName("Process Data").getOrCreate()

# Định nghĩa hàm để xử lý các ký tự Unicode
def replace_unicode(text):
    chars = []
    codes = []
    with open("D:\\.Repo\\Incomplete Project\\Thesis Project --- Spark\\unicode.txt", mode="r", encoding="utf8") as file:
        unicode_table = file.read().split("\n")
        for i in unicode_table:
            i = i.split()
            chars.append(i[0])
            codes.append(i[1])
    for i in range(len(chars)):
        text = text.replace(codes[i], chars[i])
    return text[:-2]

# Định nghĩa hàm để chuyển đổi HTML entities thành ký tự Unicode
def unescape_html(text):
    return html.unescape(text)

# Định nghĩa hàm để chuyển đổi định dạng ngày sinh
def process_dob(dob):
    return dob[:-2]

# Định nghĩa hàm để xử lý điểm thi và chuyển đổi về lower case
def process_score(score):
    score = score[:-2]
    score = replace_unicode(score)
    for i in range(len(score)):
        if score[i:i+2] == "&#":
            score = score[:i] + html.unescape(score[i:i+5]) + score[i+6:]
    score = score.replace(":", "")
    score = score.replace("KHXH ", "KHXH   ")
    score = score.replace("KHTN ", "KHTN   ")
    score_list = score.split("   ")
    data = []
    for subject in ["Toán", "Ngữ văn", "KHXH", "KHTN", "Lịch sử", "Địa lí", "GDCD", "Sinh học", "Vật lí", "Hóa học", "Tiếng Anh"]:
        if subject in score_list:
            data.append(str(float(score_list[score_list.index(subject) + 1])))  # Chuyen 7.00 -> 7.0
        else:
            data.append("")
    return " ".join(data)

# Định nghĩa hàm để chuyển đổi các trường dữ liệu về định dạng số
def to_float(value):
    if value.strip() == "":
        return None
    return float(value)

# Normal
def normal(txt):
    return txt



# Tạo DataFrame rỗng
columns = [
    "sbd", "name", "dob", "toan", "ngu_van", "tieng_anh", "lich_su", "dia_ly",
    "gdcd", "khxh", "sinh_hoc", "vat_li", "hoa_hoc", "khtn"
]
full_df = pd.DataFrame(columns=columns)

# Xử lý for loop
n = math.ceil(len(lines)/chunk_size)
for i in range(n):
    # Đọc dữ liệu từ file text
    link_data = "D:\\.Repo\\Incomplete Project\\Thesis Project --- Spark\\split_data\\chunk_{}.txt".format(i)
    data_rdd = spark.sparkContext.textFile(link_data)

    
    # Lọc và xử lý dữ liệu
    filtered_rdd = data_rdd.filter(lambda line: len(line.strip()) > 0) \
        .map(lambda line: line.strip()) \
        .filter(lambda line: line.startswith("b'<!DOCTYPE html>")) \
        .map(lambda line: line.split("\\n")) \
        .filter(lambda arr: len(arr) == 90) \
        .map(lambda arr: (arr[61].strip(), arr[64].strip(), arr[67].strip()))

    # Chuyển RDD thành DataFrame
    def rdd_to_df(rdd):
        df = rdd.toDF(["name", "dob", "score"])
        return df

    while True:
        try:
            data_df = rdd_to_df(filtered_rdd)
            break
        except Exception as e:
            continue
    
    # Áp dụng các hàm xử lý dữ liệu vào DataFrame
    replace_unicode_udf = udf(replace_unicode, StringType())
    unescape_html_udf = udf(unescape_html, StringType())
    process_dob_udf = udf(process_dob, StringType())
    process_score_udf = udf(process_score, StringType())
    to_float_udf = udf(to_float, FloatType())
    normal_udf = udf(normal, StringType())

    processed_df = data_df.withColumn("name", replace_unicode_udf("name")) \
        .withColumn("name", unescape_html_udf("name")) \
        .withColumn("score", process_score_udf("score")) \
        .withColumn("dob", process_dob_udf("dob")) \
        .withColumn("toan", split(normal_udf("score"), " ")[0]) \
        .withColumn("ngu_van", split(normal_udf("score"), " ")[1]) \
        .withColumn("tieng_anh", split(normal_udf("score"), " ")[10]) \
        .withColumn("lich_su", split(normal_udf("score"), " ")[4]) \
        .withColumn("dia_ly", split(normal_udf("score"), " ")[5]) \
        .withColumn("gdcd", split(normal_udf("score"), " ")[6]) \
        .withColumn("khxh", split(normal_udf("score"), " ")[2]) \
        .withColumn("sinh_hoc", split(normal_udf("score"), " ")[7]) \
        .withColumn("vat_li", split(normal_udf("score"), " ")[8]) \
        .withColumn("hoc_hoc", split(normal_udf("score"), " ")[9]) \
        .withColumn("khtn", split(normal_udf("score"), " ")[3])

    processed_df = processed_df.drop('score')
    pd_df = processed_df.toPandas()
    print("Chunk {} done".format(i), end="\r")
    full_df = pd.concat([full_df, pd_df], ignore_index=True)

full_df.to_csv("data.csv", index=False)

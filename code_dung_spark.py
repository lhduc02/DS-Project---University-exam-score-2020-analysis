from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, split
from pyspark.sql.types import StringType, IntegerType, FloatType
import pandas as pd
import html

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
#     return "".join(score)
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



# Đọc dữ liệu từ file text và tạo DataFrame từ dữ liệu đó
data_rdd = spark.sparkContext.textFile("D:\\.Repo\\Incomplete Project\\Thesis Project --- Spark\\raw_data.txt")
data_df = data_rdd.filter(lambda line: len(line.strip()) > 0) \
    .map(lambda line: line.strip()) \
    .filter(lambda line: line.startswith("b'<!DOCTYPE html>")) \
    .map(lambda line: line.split("\\n")) \
    .filter(lambda arr: len(arr) == 90) \
    .map(lambda arr: (arr[61].strip(), arr[64].strip(), arr[67].strip())) \
    .toDF(["name", "dob", "score"])



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
so_bao_danh = arr = ['0'+str(2000000+i) for i in range(1, len(pd_df)+1)]
pd_df.insert(loc=0, column='sbd', value=so_bao_danh)
pd_df.to_csv("D:\\.Repo\\Incomplete Project\\Thesis Project --- Spark\\diem_thi.csv", index=False)

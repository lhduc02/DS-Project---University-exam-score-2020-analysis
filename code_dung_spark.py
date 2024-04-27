from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, split
from pyspark.sql.types import StringType, IntegerType, FloatType
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
    return dob[:-2].split("/")

# Định nghĩa hàm để xử lý điểm thi và chuyển đổi về lower case
def process_score(score):
    score = score[:-2]
    score = replace_unicode(score)
    for i in range(len(score)):
        if score[i:i+2] == "&#":
            score = score[:i] + html.unescape(score[i:i+5]) + score[i+6:]
    score = score.replace(":", "")
    score = score.replace("khxh ", "khxh   ")
    score = score.replace("khtn ", "khtn   ")
    score_list = score.split("   ")
    data = []
    for subject in ["Toán", "Ngữ văn", "KHXH", "KHTN", "Lịch sử", "Địa lí", "GDCD", "sinh học", "vật lí", "hoa", "N1"]:
        if subject in score_list:
            data.append(str(float(score_list[score_list.index(subject) + 1])))  # Chuyen 7.00 -> 7.0
        else:
            data.append("NULL")
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



# Lưu dữ liệu dưới dạng PySpark DataFrame
processed_df = data_df.withColumn("name", replace_unicode_udf("name")) \
    .withColumn("name", unescape_html_udf("name")) \
    .withColumn("score", process_score_udf("score")) \
    .withColumn("toan", split(normal_udf("score"), " ")[0]) \
    .withColumn("ngu_van", split(normal_udf("score"), " ")[1]) \
    .withColumn("khxh", split(normal_udf("score"), " ")[2]) \
    .withColumn("khtn", split(normal_udf("score"), " ")[3]) \
    .withColumn("lich_su", split(normal_udf("score"), " ")[4]) \
    .withColumn("dia_ly", split(normal_udf("score"), " ")[5]) \
    .withColumn("gdcd", split(normal_udf("score"), " ")[6]) \
    .withColumn("sinh_hoc", split(normal_udf("score"), " ")[7]) \
    .withColumn("vat_li", split(normal_udf("score"), " ")[8]) \
    .withColumn("hoc_hoc", split(normal_udf("score"), " ")[9]) \
    .withColumn("N1", split(normal_udf("score"), " ")[10])
processed_df = processed_df.drop('score')



# Lưu dữ liệu dưới dạng file CSV
# Lưu pyspark dataframe dưới dạng .csv
processed_df.write.csv("D:\\.Repo\\Incomplete Project\\Thesis Project --- Spark\\diem_thi.csv")

print("Done!!!")
import os
import html
import csv
cur_path = os.path.dirname(__file__)
#path to data in another directory
data_path = os.path.relpath("D:/.Project/Python Project/raw_data_2line.txt", cur_path)
charmap_path = os.path.relpath("D:/.Project/Python Project/unicode.txt", cur_path)

file = open(data_path, "r")
sbd = 2000000
datas = file.read().split("\n")

# write header to *.csv
with open ("D:/.Project/Python Project/clean_data_2line.csv", "w", encoding="utf8", newline="") as file_csv:
    header = ["sbd", "tên", "dd", "mm", "yy", "toán", "ngữ văn", "khxh", "khtn","lịch sử", "địa lí", "gdcd", "sinh học", "vật lí", "hóa học", "tiếng anh"]
    writer = csv.writer(file_csv)
    writer.writerow(header)

# file = open("test_data1.txt", "w", encoding='utf8') #file ở trên làm việc xong có thể gán value cho file variable ở dưới
for data in datas:
    sbd += 1
    sbd_str = "0" + str(sbd)
    # with open(data_path,"r") as file:
    #make data becomes a list
    data = data.split("\\n")
    #remove \r \t redundance
    for i in range(len(data)):
        data[i] = data[i].replace("\\r", "")
        data[i] = data[i].replace("\\t", "")

    #remove tags
    for i in range(len(data)):
        #line trong list data
        # line = data[i]  -> #tạo 1 copy của data[i] -> data[i] ko đổi -> xử lý trực tiếp trên data[i]
        tags = []
        for j in range(len(data[i])):
            if data[i][j] == "<":
                begin = j
            if data[i][j] == ">":
                end = j
                tags.append(data[i][begin:end+1])
        for tag in tags:
            data[i] = data[i].replace(tag, "")

    #remove leading and trailing white space
    for i in range(len(data)):
        data[i] = data[i].strip() #str.strip() là execute function, cần phải gán value vào

    #remove empty lines
    unempty_line = []
    for i in range (len(data)):
        if data[i] != "":
            unempty_line.append(data[i])
    data = unempty_line

    if len(data) < 7:   #khi phân tích thấy những ai có điểm đầy đủ thì element data = 10 -> data < 10 là ko có thì bỏ qua
        continue

    # choose relevant information
    name = data[7]
    dob = data[8]
    score = data[9]

    # load unicode table
    chars = []  #special characters
    codes = []  #code of special characters

    with open(charmap_path, "r", encoding="utf8") as file:
        unicode_table = file.read().split("\n")

    for code in unicode_table:
        x = code.split(" ")
        chars.append(x[0])
        codes.append(x[1])

    # replace special characters in name and scores
    for i in range(len(chars)):
        name = name.replace(codes[i], chars[i])
        score = score.replace(codes[i], chars[i])

    #handle &#225 to "á"
    # for i in range(len(name)):
    #     if name[i:i+2] == "&#":
    #         name = name[:i] + chr(int(name[i+2:i+5])) + name[i+6:]
    #
    # for i in range(len(score)):
    #     if score[i:i+2] == "&#":
    #         score = score[:i] + chr(int(score[i+2:i+5])) + score[i+6:]

    for i in range(len(name)):
        if name[i:i+2] == "&#":
            name = name[:i] + html.unescape(name[i:i+5]) + name[i+6:]

    for i in range(len(score)):
        if score[i:i+2] == "&#":
            score = score[:i] + html.unescape(score[i:i+5]) + score[i+6:]

    # change to lower case
    name = name.lower()
    score = score.lower()

    #split dob
    dob_list = dob.split("/")
    dd = int(dob_list[0])
    mm = int(dob_list[1])
    yy = int(dob_list[2])

    # process scores
    # remove :
    score = score.replace(":", "")
    score = score.replace("khxh ", "khxh   ")
    score = score.replace("khtn ", "khtn   ")

    score_list = score.split("   ")
    data = [sbd_str, name.title(), str(dd), str(mm), str(yy)]

    # add score to data
    for subject in ["toán", "ngữ văn", "khxh", "khtn", "lịch sử", "địa lí", "gdcd", "sinh học", "vật lí", "hóa học", "tiếng anh"]:
        if subject in score_list:
            data.append(str(float(score_list[score_list.index(subject) + 1])))  #để chuyển về số float  7.00 -> 7.0
        else:
            data.append("-1")

# write data to test_data1.txt in normal
#     with open("test_data1.txt", "a", encoding='utf8') as test_file:
#         for line in data:
#             test_file.write(line + ",")
#         test_file.write("\n")
    # write data to *.csv
    with open("D:/.Project/Python Project/clean_data.csv", "a", encoding="utf8", newline='') as file_csv:
        writer = csv.writer(file_csv)
        writer.writerow(data)

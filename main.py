import html
import csv

with open ("D:\\.Project\\DS Project\\Phan_tich_diem_thi\\test_data.csv", "w", encoding="utf8", newline="") as file_csv:
    header = ["sbd", "tên", "dd", "mm", "yy", "toán", "ngữ văn", "khxh", "khtn","lịch sử", "địa lí", "gdcd", "sinh học", "vật lí", "hóa học", "tiếng anh"]
    writer = csv.writer(file_csv)
    writer.writerow(header)

file = open("D:\\.Project\\DS Project\\Phan_tich_diem_thi\\raw_data.txt", mode = "r", encoding="utf8")
datas = file.read().split("\n")
sbd = 2000000

# Vong lap chay qua tung thi sinh
for data in datas:
    sbd += 1
    sbd_str = "0" + str(sbd)
    data = data.split("\\n")
    if len(data) != 90:
        continue

    name = data[61].strip()
    dob = data[64].strip()
    score = data[67].strip()

    chars = []
    codes = []

    file = open("D:\\.Project\\DS Project\\Phan_tich_diem_thi\\unicode.txt", mode = "r", encoding="utf8")
    unicode_table = file.read().split("\n")
    for i in unicode_table:
        i = i.split()
        chars.append(i[0])
        codes.append(i[1])

    for i in range(len(chars)):
        name = name.replace(codes[i], chars[i])
        score = score.replace(codes[i], chars[i])

    for i in range(len(name)):
        if name[i:i+2] == "&#":
            name = name[:i] + html.unescape(name[i:i+5]) + name[i+6:]
    name = name[:-2]

    for i in range(len(score)):
        if score[i:i+2] == "&#":
            score = score[:i] + html.unescape(score[i:i+5]) + score[i+6:]
    score = score[:-2]

    # Doi thanh lower case
    name = name.lower()
    score = score.lower()

    dob = dob[:-2]
    # Split dob
    dob_list = dob.split("/")
    dd = int(dob_list[0])
    mm = int(dob_list[1])
    yy = int(dob_list[2])

    # Phan tich diem
    # Score -> List
    score = score.replace(":", "")
    score = score.replace("khxh ", "khxh   ")
    score = score.replace("khtn ", "khtn   ")

    score_list = score.split("   ")
    #print(score_list)


    data = [sbd_str, name.title(), str(dd), str(mm), str(yy)]

    # add score to data
    for subject in ["toán", "ngữ văn", "khxh", "khtn", "lịch sử", "địa lí", "gdcd", "sinh học", "vật lí", "hóa học", "tiếng anh"]:
        if subject in score_list:
            data.append(str(float(score_list[score_list.index(subject) + 1])))  # Chuyen 7.00 -> 7.0
        else:
            data.append("")


    # Ghi file
    file = open("D:\\.Project\\DS Project\\Phan_tich_diem_thi\\test.txt", mode = "w", encoding="utf8")
    for i in range(len(data)):
        file.write(data[i] + ",")


    with open("D:\\.Project\\DS Project\\Phan_tich_diem_thi\\test_data.csv", "a", encoding="utf8", newline='') as file_csv:
        writer = csv.writer(file_csv)
        writer.writerow(data)

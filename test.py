import html
import csv

file = open("D:\\.Project\\DS Project\\Phan_tich_diem_thi\\raw_data.txt", mode = "r", encoding="utf8")
datas = file.read().split("\n")
sbd = 2000000

file = open("D:\\.Project\\DS Project\\Phan_tich_diem_thi\\1line_data.txt", mode = "w", encoding="utf8")
# Vong lap chay qua tung thi sinh
for data in datas:
    sbd += 1
    sbd_str = "0" + str(sbd)
    data = data.split("\\n")
    if len(data) == 90:
        for i in range(len(data)):
            file.write(data[i] + "\n")
        break

for data in datas:
    sbd += 1
    sbd_str = "0" + str(sbd)
    data = data.split("\\n")
    if len(data) != 90:
        for i in range(len(data)):
            file.write(data[i] + "\n")
        break


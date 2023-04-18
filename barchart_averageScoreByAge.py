import matplotlib.pyplot as plt
import numpy as np

file = open("D:\\.Project\\DS Project\\Phan_tich_diem_thi\\clean_data.csv", encoding="utf8", mode = "r")
file = file.read().split("\n")
header = file[0].split(",")
#header = [sbd,tên,dd,mm,yy,toán,ngữ văn,khxh,khtn,lịch sử,địa lí,gdcd,sinh học,vật lí,hóa học,tiếng anh]
#len_header = 16
students = file[1:]
students.pop()
min = 100
max = 0
sum_age = [0 for i in range(100)]
total_score = [0 for i in range(100)]
so_mon = [0 for i in range(100)]

for i in range(len(students)):
    students[i] = students[i].split(",")
    age_student = 2020 - int(students[i][4])
    if age_student < min:
        min = age_student
    if age_student > max:
        max = age_student
    sum_age[age_student] += 1
    for s in students[i][5:7]:
        if s != "-1":
            so_mon[age_student] += 1
            total_score[age_student] += float(s)
    for s in students[i][9:]:
        if s != "-1":
            so_mon[age_student] += 1
            total_score[age_student] += float(s)
print(total_score[17:30])
print(so_mon[17:30])

average_score = [0 for i in range(13)]
for i in range(13):
    average_score[i] = round(total_score[i+17] / so_mon[i+17], 2)
# print(min, max) => min = 17, max = 57, tuy nhiên số lượng thí sinh >= 30 tuổi ít nên gộp tuổi 30+ lại
age = [str(i) for i in range(17, 30)]
age.append("30+")
# print(len(age))    => len = 14

print(average_score)
average_score.append(sum(total_score[30:]) / 28)

yxis_age = sum_age[17:30]
yxis_age.append(sum(sum_age[30:]))

figure, axis = plt.subplots()

y_pos = np.arange(len(age))

plt.bar(age, average_score, align='center')
plt.xlabel("Age")

axis.set_ylim(0, 10)
plt.ylabel("Total")
plt.title("Average score by age")

# Add number 
rects = axis.patches
for rect, label in zip(rects, average_score):
    height = rect.get_height()
    axis.text(rect.get_x() + rect.get_width()/2, height, label, ha = "center", va = "bottom")

plt.show()

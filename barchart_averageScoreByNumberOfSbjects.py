# Kiểm tra cho câu hỏi: Có phải số môn thi càng ít thì điểm càng thấp không?

import matplotlib.pyplot as plt
import numpy as np
file = open("D:\\.Project\\DS Project\\Phan_tich_diem_thi\\clean_data.csv", encoding="utf8", mode = "r")
file = file.read().split("\n")
header = file[0]
students = file[1:]
students.pop()

# Bỏ tổ hợp KHTN và KHXH nên còn 9 môn
subjects = ['Toán', 'Ngữ Văn', 'Lịch Sử', 'Địa Lí', 'GDCD', 'Sinh Học', 'Vật Lí', 'Hóa Học', 'Tiếng Anh']
count_score = [0 for i in range(10)]
average = [0 for i in range(10)]

for i in range(len(students)):
    students[i] = students[i].split(",")
    students[i] = students[i][5:7] + students[i][9:]
    total = 0
    count = 0
    for s in students[i]:
        if s != "-1":
            total += float(s)
            count += 1
    average[count] += total/count
    count_score[count] += 1

# Average score
for i in range(10):
    if count_score[i] != 0:
        average[i] = round(average[i] / count_score[i], 2)

# Draw barchart with matplotlib
figure, axis = plt.subplots()

labels = ['0 môn', '1 môn', '2 môn', '3 môn', '4 môn', '5 môn', '6 môn', '7 môn', '8 môn', '9 môn']
y_pos = np.arange(len(labels))

plt.bar(labels, average, align='center')
plt.xlabel("Number of subjects")

axis.set_ylim(0, 10)

plt.ylabel("Average score")
plt.title("Average score by number of subjects")

# Add number 
rects = axis.patches
for rect, label in zip(rects, average):
    height = rect.get_height()
    axis.text(rect.get_x() + rect.get_width()/2, height, label,
            ha = "center", va = "bottom")

plt.show()

"""
Hypothesis: Thi càng ít môn thì điểm càng kém (do chán -> bỏ thi).
Conclude: Không đúng.
"""

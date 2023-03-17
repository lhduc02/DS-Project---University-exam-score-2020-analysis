"""
Barchat có 2 datatype chính:
+ categorical data - nhóm (toán, ngữ văn, ... được gọi là các nhóm)
+ metric data - dữ liệu dạng số (cận nặng, số quốc gia trên thế giới, ...)
"""

# Barchart for "not take exam"
import matplotlib.pyplot as plt
import numpy as np
with open("D:\\.Project\\DS Project\\Phan_tich_diem_thi\\clean_data.csv",encoding="utf8") as file:
    data = file.read().split("\n")
    
header = data[0]                    # Name of subject
header = header.split(",")
print(header)
count_subject = 11

#toán, văn, khxh, khtn, sử, địa, gdcd, sinh, lí, hóa, anh
students = data[1:]                 # List students
students.pop()              # Loại bỏ vì student[-1] = ""
total_student = len(students)

for i in range(total_student):
    students[i] = students[i].split(",")

# Số sinh viên không thi ở từng môn (tổ hợp)
not_take_exam = [0 for i in range(11)]

for s in students:
    for i in range(5, 16):
        if s[i] == "-1":
            not_take_exam[i-5] += 1

not_take_exam_percentage = [not_take_exam[i]/total_student*100 for i in range(11)]
print(not_take_exam_percentage)

# Draw with matplotlib
figure, axis = plt.subplots()

subjects = ['Toán', 'Ngữ Văn', 'KHTN', 'KHXH', 'Lịch Sử', 'Địa Lí', 'GDCD', 'Sinh Học', 'Vật Lí', 'Hóa Học', 'Tiếng Anh']
y_pos = np.arange(len(subjects))

plt.bar(subjects, not_take_exam_percentage, align='center')
plt.xlabel("Suject")

axis.set_ylim(0, 100)

plt.ylabel("Percentage")
plt.title("Number of students who did not take the exam")

# Add number 
rects = axis.patches
for rect, label in zip(rects, not_take_exam):
    height = rect.get_height()
    axis.text(rect.get_x() + rect.get_width()/2, height+5, label,
            ha = "center", va = "bottom")

plt.show()

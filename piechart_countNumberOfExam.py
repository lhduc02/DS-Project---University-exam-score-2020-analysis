# Dem so hoc sinh thi 1 mon, 2 mon, 3 mon, ...
import matplotlib.pyplot as plt

file = open("D:\\.Project\\DS Project\\Phan_tich_diem_thi\\clean_data.csv", encoding="utf8", mode = "r")
file = file.read().split("\n")
header = file[0]
students = file[1:]
students.pop()

# Bỏ tổ hợp KHTN và KHXH nên còn 9 môn
subjects = ['Toán', 'Ngữ Văn', 'Lịch Sử', 'Địa Lí', 'GDCD', 'Sinh Học', 'Vật Lí', 'Hóa Học', 'Tiếng Anh']
count_score = [0 for i in range(10)]
for i in range(len(students)):
    students[i] = students[i].split(",")
    students[i] = students[i][5:7] + students[i][9:]
    count_subjects = 9-students[i].count("-1")
    count_score[count_subjects] += 1

# Draw piechart with matplotlib

labels = '3 môn', '4 môn', '5 môn', '6 môn', 'Khác'
sizes = [count_score[3], count_score[4], count_score[5], count_score[6], sum(count_score[1:3]) + sum(count_score[7:10])]

fig, ax = plt.subplots()
ax.pie(sizes, labels=labels, autopct='%1.1f%%', pctdistance=1.25, labeldistance=.6)
plt.show()

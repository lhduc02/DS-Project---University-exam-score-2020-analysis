import matplotlib.pyplot as plt

file = open("D:\\.Repo\\DS Repo\\Phan_tich_diem_thi\\clean_data.csv", encoding="utf8", mode = "r")

datas = file.read().split("\n")
print(len(datas))

math_score = []
for i in range(1, len(datas)):
    ms = datas[i].split(",")[5]
    math_score.append(float(ms))

y_axis = []
x_axis = [i/5 for i in range(0, 51)]

for i in x_axis:
    y_axis.append(math_score.count(i))

plt.bar(x_axis, y_axis, 0.1)
plt.title("Math score spectrum")
plt.xlabel("Score")
plt.ylabel("Quantity")
plt.show()

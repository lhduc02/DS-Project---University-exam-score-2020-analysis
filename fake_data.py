# Mở file1 để đọc
with open("D:\\.Repo\\Incomplete Project\\Thesis Project --- Spark\\raw_data.txt", mode="r", encoding="utf8") as file1:
    # Mở file2 để ghi
    with open("D:\\.Repo\\Incomplete Project\\Thesis Project --- Spark\\data_test_2GB.txt", mode="a", encoding="utf8") as file2:
        # Lặp lại 10 lần
        for i in range(10):
            # Di chuyển con trỏ về đầu file1
            file1.seek(0)
            # Đọc nội dung của file1
            file1_content = file1.read()
            # Ghi nội dung của file1 vào file2
            file2.write(file1_content)

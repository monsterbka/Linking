import base64
import math
import os


def split_file_to_base64(input_file, num_parts, output_prefix=""):
    # Đọc file dạng binary
    with open(input_file, "rb") as f:
        binary_data = f.read()

    # Encode sang base64 string
    b64_str = base64.b64encode(binary_data).decode("utf-8")

    # Tính kích thước mỗi phần
    part_size = math.ceil(len(b64_str) / num_parts)

    parts = []
    for i in range(num_parts):
        start = i * part_size
        end = start + part_size
        part_data = b64_str[start:end]

        filename = f"{output_prefix}{i+1:03d}.txt"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(part_data)

        parts.append(filename)

    print(f"Đã chia thành {len(parts)} file:", parts)


def merge_base64_to_file(input_prefix, num_parts, output_file):
    b64_str = ""

    # Đọc lần lượt theo thứ tự
    for i in range(num_parts):
        filename = f"{input_prefix}_{i+1:03d}.txt"
        with open(filename, "r", encoding="utf-8") as f:
            b64_str += f.read()

    # Decode base64 về binary
    binary_data = base64.b64decode(b64_str.encode("utf-8"))

    # Ghi lại file gốc
    with open(output_file, "wb") as f:
        f.write(binary_data)

    print(f"Đã phục hồi file: {output_file}")


# ====== Ví dụ sử dụng ======
if __name__ == "__main__":
    input_file = "lite-matching-engine-master.zip"   # file bất kỳ
    num_parts = 20

    # Chia file
    split_file_to_base64(input_file, num_parts)

    # Ghép lại file
    # merge_base64_to_file("chunk", num_parts, "reconstructed.bin")
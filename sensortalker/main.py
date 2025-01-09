filename = "Device data collector.py"  # ใส่ชื่อไฟล์ที่คุณต้องการ

command = f'pyinstaller -F "{filename}"'

with open("build_command.bat", "w") as file:
    file.write(command)

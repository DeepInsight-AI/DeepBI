# filename: run_detect_click_anomalies.py

import subprocess

# 运行 detect_click_anomalies.py 脚本
script_path = r'detect_click_anomalies.py'

result = subprocess.run(['python', script_path], capture_output=True, text=True)

# 输出脚本执行的结果
print(result.stdout)
print(result.stderr)
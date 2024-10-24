from datetime import datetime, timedelta
from ai.backend.util.db.auto_process.automatic_status_quo_analysis.generate_docx_current_situation_analysis import generate_docx,generate_docx_test
from ai.backend.util.db.auto_process.automatic_status_quo_analysis.main import find_brand_by_uid
from ai.backend.util.db.auto_process.summary.util.InserOnlineData import ProcessShowData as psd


def get_report_api(data):
    db, brand_name, brand_info = find_brand_by_uid(data['UID'])
    print(db, brand_name, brand_info)
    if brand_name:
        # # Update the JSON file with new dbinfo
        # update_db_info(dbinfo)

        # 获取今天的日期
        today = datetime.now()
        # 计算今天的前2天
        two_days_ago = today - timedelta(days=2)
        # 计算今天的前31天
        thirty_one_days_ago = today - timedelta(days=31)

        # 按照 '%Y-%m-%d' 格式生成日期字符串
        two_days_ago_str = two_days_ago.strftime('%Y-%m-%d')
        thirty_one_days_ago_str = thirty_one_days_ago.strftime('%Y-%m-%d')
        print(two_days_ago_str, thirty_one_days_ago_str)
        pdf_path = generate_docx(db, brand_name, data['market'], thirty_one_days_ago_str, two_days_ago_str)
        file = pdf_path
        data = {
            "UID": data['UID'],
            "CountryCode": data['market'],
            "send_email": 0  # 是否发送邮件 1:是  0:否，默认否
        }
        result, msg = psd.post_file(file, data)
        print(result, msg)

        return 200


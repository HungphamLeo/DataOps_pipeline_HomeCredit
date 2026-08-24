from prefect_orchestra.flow.daily_pipeline import master_pipeline_flow

if __name__ == "__main__":
    # Chạy pipeline một lần ngay lập tức để test
    master_pipeline_flow()

    # Để chạy pipeline này theo lịch (ví dụ: 1 giờ sáng hàng ngày),
    # bạn nên tạo một deployment bằng Prefect CLI:
    # prefect deploy --name "home-credit-pipeline" --flow prefect/flow/daily_pipeline.py:master_pipeline_flow --cron "0 1 * * *" --timezone "UTC"
    #
    # Hoặc dùng .serve() để chạy agent cục bộ (ít phổ biến hơn cho production):
    # master_pipeline_flow.serve(name="home-credit-daily-deployment", schedule=CronSchedule(cron="0 1 * * *", timezone="UTC"))

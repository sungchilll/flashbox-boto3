import boto3
import time

# AWS 클라이언트 생성
rds = boto3.client("rds", region_name="ap-northeast-2")
s3 = boto3.client("s3")

# 설정 값
DB_INSTANCE_ID = "your-rds-instance-id"
SNAPSHOT_ID = f"{DB_INSTANCE_ID}-snapshot-{int(time.time())}"
EXPORT_TASK_ID = f"{DB_INSTANCE_ID}-export-{int(time.time())}"
S3_BUCKET_NAME = "s3-bucket-name"
S3_PREFIX = "rds-backups/"
KMS_KEY_ARN = "your-kms-key-arn"  # KMS 키 사용 필수

def create_rds_snapshot():
    """RDS 스냅샷 생성"""
    print(f"Creating snapshot {SNAPSHOT_ID}...")
    rds.create_db_snapshot(
        DBInstanceIdentifier=DB_INSTANCE_ID,
        DBSnapshotIdentifier=SNAPSHOT_ID
    )

    # 스냅샷이 완료될 때까지 대기
    while True:
        snapshot = rds.describe_db_snapshots(DBSnapshotIdentifier=SNAPSHOT_ID)["DBSnapshots"][0]
        status = snapshot["Status"]
        print(f"Snapshot status: {status}")
        if status == "available":
            break
        time.sleep(30)

    print(f"Snapshot {SNAPSHOT_ID} created successfully.")

def export_snapshot_to_s3():
    """RDS 스냅샷을 S3로 내보내기"""
    print(f"Exporting {SNAPSHOT_ID} to S3...")
    
    rds.start_export_task(
        ExportTaskIdentifier=EXPORT_TASK_ID,
        SourceArn=f"arn:aws:rds:ap-northeast-2:your-account-id:snapshot:{SNAPSHOT_ID}",
        S3BucketName=S3_BUCKET_NAME,
        S3Prefix=S3_PREFIX,
        IamRoleArn="your-iam-role-arn",
        KmsKeyId=KMS_KEY_ARN
    )

    # 내보내기 완료 대기
    while True:
        export_status = rds.describe_export_tasks(ExportTaskIdentifier=EXPORT_TASK_ID)["ExportTasks"][0]["Status"]
        print(f"Export status: {export_status}")
        if export_status == "complete":
            break
        time.sleep(30)

    print(f"Export task {EXPORT_TASK_ID} completed successfully.")

def delete_rds_snapshot():
    """RDS 스냅샷 삭제"""
    print(f"Deleting snapshot {SNAPSHOT_ID}...")
    rds.delete_db_snapshot(DBSnapshotIdentifier=SNAPSHOT_ID)
    print(f"Snapshot {SNAPSHOT_ID} deleted.")

if __name__ == "__main__":
    create_rds_snapshot()
    export_snapshot_to_s3()
    delete_rds_snapshot()
    print("✅ RDS Snapshot backup and cleanup completed.")

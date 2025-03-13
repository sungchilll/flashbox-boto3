import boto3
import time

# AWS 클라이언트 생성
session = boto3.Session(profile_name="default")
rds = boto3.client("rds", region_name="ap-northeast-2")
s3 = boto3.client("s3")

# 설정 값
DB_INSTANCE_ID = "your-rds-instance-id"
S3_BUCKET_NAME = "s3-bucket-name"
S3_PREFIX = "rds-backups/"
KMS_KEY_ARN = "your-kms-key-arn"  # KMS 키 사용 필수
IAM_ROLE_ARN = "arn:aws:iam::123456789012:role/MyRDSSnapshotExportRole"  # IAM Role ARN 입력
AWS_ACCOUNT_ID = "123456789012"  # AWS 계정 ID 입력

SNAPSHOT_ID = f"{DB_INSTANCE_ID}-snapshot-{int(time.time())}"
EXPORT_TASK_ID = f"{DB_INSTANCE_ID}-export-{int(time.time())}"

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
        SourceArn=f"arn:aws:rds:ap-northeast-2:{AWS_ACCOUNT_ID}:snapshot:{SNAPSHOT_ID}",
        S3BucketName=S3_BUCKET_NAME,
        S3Prefix=S3_PREFIX,
        IamRoleArn=IAM_ROLE_ARN,
        KmsKeyId=KMS_KEY_ARN
    )

    # 내보내기 완료 대기
    while True:
        response = rds.describe_export_tasks(ExportTaskIdentifier=EXPORT_TASK_ID)
        export_status = response["ExportTasks"][0]["Status"]
        percent_progress = response["ExportTasks"][0].get("PercentProgress", 0)

        print(f"Export status: {export_status}, Progress: {percent_progress}%")

        if export_status == "complete":
            print(f"Export task {EXPORT_TASK_ID} completed successfully.")
            break
        elif export_status == "failed":
            failure_cause = response["ExportTasks"][0].get("FailureCause", "Unknown error")
            print(f"Export task {EXPORT_TASK_ID} failed. Reason: {failure_cause}")
            break

    print(f"Export task {EXPORT_TASK_ID} completed successfully.")

def delete_rds_snapshot():
    """RDS 스냅샷 삭제"""
    print(f"Sending delete request for snapshot: {SNAPSHOT_ID}")

    try:
        response = rds.delete_db_snapshot(DBSnapshotIdentifier=SNAPSHOT_ID)
        print(f"Delete request sent successfully. Response: {response}")

    except rds.exceptions.ClientError as e:
        if "DBSnapshotNotFound" in str(e):
            print(f"Snapshot {SNAPSHOT_ID} already deleted (not found).")
            return
        else:
            print(f"Error deleting snapshot: {e}")
            return  # 오류 발생 시 함수 종료

    print("Waiting for snapshot deletion confirmation...")

    # 스냅샷 삭제 대기 (최대 15분)
    timeout = time.time() + 900  # 900초 (15분) 후 타임아웃
    while True:
        try:
            snapshots = rds.describe_db_snapshots(DBSnapshotIdentifier=SNAPSHOT_ID)
            snapshot_status = snapshots["DBSnapshots"][0]["Status"]
            print(f"Current snapshot status: {snapshot_status}")

            if snapshot_status == "deleted":
                print(f"Snapshot {SNAPSHOT_ID} deleted successfully.")
                break

        except rds.exceptions.ClientError as e:
            if "DBSnapshotNotFound" in str(e):
                print(f"Snapshot {SNAPSHOT_ID} successfully deleted (not found).")
                break
            else:
                print(f"Error checking snapshot status: {e}")
        
        if time.time() > timeout:
            print(f"Timeout reached! Snapshot {SNAPSHOT_ID} may still exist.")
            break

        time.sleep(30)  # 30초 대기 후 다시 확인

if __name__ == "__main__":
    create_rds_snapshot()
    export_snapshot_to_s3()
    delete_rds_snapshot()
    print("RDS Snapshot backup and cleanup completed.")

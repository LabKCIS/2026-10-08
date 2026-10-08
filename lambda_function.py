"""S3 upload -> Lambda -> Textract (FORMS+TABLES) -> DynamoDB   (含除錯訊息版)
Runtime: Python 3.12
環境變數:
  TABLE_NAME  DynamoDB 表名 (預設 DocumentResults)
  PK_NAME     分區鍵欄位名 (預設 documentId)
  SK_NAME     排序鍵欄位名 (預設 sk)
  DEBUG       true/false,true 時輸出詳細內容 (預設 true)
Log 前綴: [CONFIG] [EVENT] [S3] [TEXTRACT] [PARSE] [DYNAMODB] ,錯誤一律以 ❌ 開頭並附「原因 / 建議」。
"""
import os, json, logging, urllib.parse
from datetime import datetime, timezone
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError
   
TABLE_NAME = os.environ.get("TABLE_NAME", "DocumentResults")
PK = os.environ.get("PK_NAME", "documentId")
SK = os.environ.get("SK_NAME", "sk")
DEBUG = os.environ.get("DEBUG", "true").lower() == "true"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
for noisy in ("boto3", "botocore", "urllib3", "s3transfer"):  # 避免 SDK 內部 debug 訊息淹沒自己的 log
    logging.getLogger(noisy).setLevel(logging.WARNING)

textract = boto3.client("textract")
ddb_client = boto3.client("dynamodb")
table = boto3.resource("dynamodb").Table(TABLE_NAME)

# ---- 錯誤碼 -> 原因/建議 對照 ---------------------------------------------
HINTS = {
    # S3
    ("S3", "NoSuchKey"): "S3 找不到該檔案。建議:確認 key 是否正確(空白/中文是否已 unquote_plus)、檔案是否已被刪除。",
    ("S3", "AccessDenied"): "Lambda Role 無權讀取 S3。建議:IAM policy 加入 s3:GetObject,Resource 要含 arn:aws:s3:::BUCKET/uploads/*。",
    # Textract
    ("TEXTRACT", "InvalidS3ObjectException"): "Textract 讀不到 S3 物件。建議:確認 bucket 與 Lambda/Textract 同一 Region,且 Role 有 s3:GetObject。",
    ("TEXTRACT", "UnsupportedDocumentException"): "文件格式不支援。建議:僅支援 PDF/PNG/JPEG/TIFF;同步 API 的 PDF 只能 1 頁,多頁請改 start_document_analysis。",
    ("TEXTRACT", "BadDocumentException"): "PDF 損毀或被密碼保護。建議:重新匯出 PDF,並移除密碼。",
    ("TEXTRACT", "DocumentTooLargeException"): "文件過大(同步上限 10MB)。建議:壓縮或改用非同步 API。",
    ("TEXTRACT", "AccessDeniedException"): "Lambda Role 無 Textract 權限。建議:IAM policy 加入 textract:AnalyzeDocument。",
    ("TEXTRACT", "ProvisionedThroughputExceededException"): "Textract 呼叫太頻繁。建議:降低上傳並行量或加入重試。",
    ("TEXTRACT", "ThrottlingException"): "Textract 被節流。建議:稍後重試。",
    ("TEXTRACT", "InvalidParameterException"): "Textract 參數不合法。建議:檢查 FeatureTypes 與 Document 參數。",
    # DynamoDB
    ("DYNAMODB", "ResourceNotFoundException"): "找不到 DynamoDB Table。建議:確認環境變數 TABLE_NAME 與資料表名稱(大小寫)一致,且資料表與 Lambda 在同一個 Region。",
    ("DYNAMODB", "AccessDeniedException"): "Lambda Role 無 DynamoDB 權限。建議:IAM policy 加入 dynamodb:PutItem、BatchWriteItem、DescribeTable,Resource 指向該資料表 ARN。",
    ("DYNAMODB", "ValidationException"): "資料不符合 Table 定義。常見原因:Key 欄位名稱/型別與 Table 不同(例如 Table 的 PK 叫 id,程式寫 documentId)、Key 值為空字串、Item 超過 400KB。",
    ("DYNAMODB", "ProvisionedThroughputExceededException"): "超出讀寫容量。建議:改用 On-demand 或提高 WCU。",
    ("DYNAMODB", "ItemCollectionSizeLimitExceededException"): "同一個 PK 的資料過多。建議:檢視 PK 設計。",
}


def explain(stage, err):
    """把例外轉成 ❌ 訊息(含原因/建議),回傳是否已處理"""
    if isinstance(err, ClientError):
        code = err.response["Error"]["Code"]
        msg = err.response["Error"].get("Message", "")
        hint = HINTS.get((stage, code)) or HINTS.get(("S3", code)) or "未收錄的錯誤碼,請對照 AWS 文件。"
        logger.error("❌ [%s] %s: %s\n   原因/建議: %s", stage, code, msg, hint)
    elif isinstance(err, NoCredentialsError):
        logger.error("❌ [%s] 找不到憑證。建議:確認 Lambda 已指定 Execution Role。", stage)
    elif isinstance(err, EndpointConnectionError):
        logger.error("❌ [%s] 連不到服務端點。建議:確認 Region 正確,若在 VPC 內需有 NAT 或 VPC Endpoint。", stage)
    else:
        logger.error("❌ [%s] %s: %s", stage, type(err).__name__, err)


# ---- 啟動前檢查 -----------------------------------------------------------
_preflight_done = False

def preflight():
    """確認 Table 存在、Key 欄位名稱與程式一致。每個執行環境只做一次。"""
    global _preflight_done
    if _preflight_done:
        return
    logger.info("[CONFIG] TABLE_NAME=%s PK_NAME=%s SK_NAME=%s DEBUG=%s region=%s",
                TABLE_NAME, PK, SK, DEBUG, boto3.session.Session().region_name)
    try:
        desc = ddb_client.describe_table(TableName=TABLE_NAME)["Table"]
    except Exception as e:
        explain("DYNAMODB", e)
        raise
    actual = {k["KeyType"]: k["AttributeName"] for k in desc["KeySchema"]}
    logger.info("[DYNAMODB] table=%s status=%s keySchema=%s billing=%s", TABLE_NAME, desc["TableStatus"], actual,
                desc.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED"))
    problems = []
    if actual.get("HASH") != PK:
        problems.append(f"Partition key 不符:Table 為 '{actual.get('HASH')}',程式使用 '{PK}'")
    if actual.get("RANGE") != SK:
        problems.append(f"Sort key 不符:Table 為 '{actual.get('RANGE')}',程式使用 '{SK}'")
    if problems:
        for p in problems:
            logger.error("❌ [DYNAMODB] %s", p)
        logger.error("   建議:修改 Lambda 環境變數 PK_NAME / SK_NAME,或重建 Table(PK=documentId, SK=sk,皆為 String)。")
        raise ValueError("DynamoDB key schema mismatch: " + "; ".join(problems))
    _preflight_done = True


# ---- Handler ---------------------------------------------------------------
def lambda_handler(event, context):
    logger.debug("[EVENT] %s", json.dumps(event, ensure_ascii=False)[:2000])
    preflight()
    records = event.get("Records")
    if not records:
        logger.error("❌ [EVENT] event 內沒有 Records。原因:可能是用 Console 的預設測試事件,而非 S3 事件。"
                     "建議:改用 sample_event.json 測試。")
        raise ValueError("event has no Records")
    for rec in records:
        try:
            bucket = rec["s3"]["bucket"]["name"]
            raw_key = rec["s3"]["object"]["key"]
        except KeyError as e:
            logger.error("❌ [EVENT] 事件格式缺少欄位 %s。建議:確認事件來源為 S3 ObjectCreated。", e)
            raise
        key = urllib.parse.unquote_plus(raw_key)
        logger.info("[S3] bucket=%s rawKey=%s decodedKey=%s", bucket, raw_key, key)
        if not key.lower().endswith(".pdf"):
            logger.warning("⚠️ [S3] 跳過非 PDF 檔案: %s", key)
            continue
        process(bucket, key)
    return {"statusCode": 200}


def process(bucket, key):
    doc_id = key.rsplit("/", 1)[-1]

    # 1) Textract
    try:
        resp = textract.analyze_document(
            Document={"S3Object": {"Bucket": bucket, "Name": key}},
            FeatureTypes=["FORMS", "TABLES"])
    except Exception as e:
        explain("TEXTRACT", e); raise
    blocks = resp["Blocks"]
    by_id = {b["Id"]: b for b in blocks}
    counts = {}
    for b in blocks:
        counts[b["BlockType"]] = counts.get(b["BlockType"], 0) + 1
    logger.info("[TEXTRACT] pages=%s blocks=%d types=%s",
                resp.get("DocumentMetadata", {}).get("Pages"), len(blocks), counts)

    # 2) 解析
    kvs = extract_kv(blocks, by_id)
    tables = extract_tables(blocks, by_id)
    logger.info("[PARSE] key-value=%d tables=%d", len(kvs), len(tables))
    if not kvs:
        logger.warning("⚠️ [PARSE] 沒有解析到任何 Key-Value。可能原因:文件不是「欄位: 值」型式、掃描品質太差、或語言不支援(Textract 對英文最佳)。")
    for kv in kvs:
        logger.debug("[PARSE] KV  %-22s = %-28s conf=%.1f", kv["name"], kv["value"], kv["conf"])
        if not kv["name"]:
            logger.warning("⚠️ [PARSE] 發現空白的 Key(value=%r),將以 field_N 命名", kv["value"])
        if not kv["value"]:
            logger.warning("⚠️ [PARSE] Key '%s' 沒有對應的 Value", kv["name"])
        if kv["conf"] < 90:
            logger.warning("⚠️ [PARSE] Key '%s' 信心分數偏低 (%.1f),建議人工複核", kv["name"], kv["conf"])
    for t, rows in enumerate(tables, 1):
        logger.debug("[PARSE] TABLE %d: %d rows x %d cols, header=%s", t, len(rows), max(map(len, rows)) if rows else 0, rows[0] if rows else None)

    # 3) 組 Items 並寫入前驗證
    now = datetime.now(timezone.utc).isoformat()
    items = [{PK: doc_id, SK: "META", "s3Bucket": bucket, "s3Key": key, "status": "PARSED",
              "processedAt": now, "kvCount": len(kvs), "tableCount": len(tables),
              "fields": {(k["name"] or f"field_{i}"): k["value"] for i, k in enumerate(kvs, 1)}}]
    for i, kv in enumerate(kvs, 1):
        items.append({PK: doc_id, SK: f"KV#{i:03d}", "fieldName": kv["name"], "fieldValue": kv["value"],
                      "confidence": Decimal(str(round(kv["conf"], 2)))})
    for t, rows in enumerate(tables, 1):
        for r, row in enumerate(rows, 1):
            items.append({PK: doc_id, SK: f"TBL#{t:02d}#ROW#{r:03d}", "cells": row})
    for it in items:
        validate_item(it)

    # 4) DynamoDB
    try:
        with table.batch_writer() as bw:
            for it in items:
                bw.put_item(Item=it)
    except Exception as e:
        explain("DYNAMODB", e)
        logger.error("   寫入的 Item 範例: %s", json.dumps(items[0], ensure_ascii=False, default=str)[:600])
        raise
    logger.info("✅ [DYNAMODB] saved documentId=%s items=%d (META=1, KV=%d, TBL rows=%d)",
                doc_id, len(items), len(kvs), len(items) - 1 - len(kvs))


def validate_item(it):
    """寫入前自我檢查,把常見的 DynamoDB ValidationException 提早說清楚"""
    for k in (PK, SK):
        if k not in it:
            logger.error("❌ [DYNAMODB] Item 缺少 Key 欄位 '%s'。實際欄位: %s", k, list(it.keys()))
            raise ValueError(f"item missing key attribute {k}")
        if not isinstance(it[k], str) or it[k] == "":
            logger.error("❌ [DYNAMODB] Key '%s' 的值不可為空字串/非字串: %r", k, it[k])
            raise ValueError(f"invalid key value for {k}")
    if len(json.dumps(it, default=str).encode("utf-8")) > 380_000:
        logger.error("❌ [DYNAMODB] Item %s/%s 接近 400KB 上限", it[PK], it[SK])
        raise ValueError("item too large")
    for k, v in it.items():
        if isinstance(v, float):
            logger.error("❌ [DYNAMODB] 屬性 '%s' 是 float,DynamoDB 只接受 Decimal。", k)
            raise TypeError(f"float not allowed: {k}")


# ---- 解析函式 --------------------------------------------------------------
def text_of(block, by_id):
    words = []
    for rel in block.get("Relationships", []):
        if rel["Type"] == "CHILD":
            for cid in rel["Ids"]:
                c = by_id[cid]
                if c["BlockType"] == "WORD":
                    words.append(c["Text"])
                elif c["BlockType"] == "SELECTION_ELEMENT" and c["SelectionStatus"] == "SELECTED":
                    words.append("[X]")
    return " ".join(words)


def extract_kv(blocks, by_id):
    out = []
    for b in blocks:
        if b["BlockType"] == "KEY_VALUE_SET" and "KEY" in b.get("EntityTypes", []):
            value_txt, conf = "", b["Confidence"]
            for rel in b.get("Relationships", []):
                if rel["Type"] == "VALUE":
                    for vid in rel["Ids"]:
                        value_txt = text_of(by_id[vid], by_id)
                        conf = min(conf, by_id[vid]["Confidence"])
            out.append({"name": text_of(b, by_id).rstrip(":").strip(), "value": value_txt, "conf": conf})
    return out


def extract_tables(blocks, by_id):
    tables = []
    for b in blocks:
        if b["BlockType"] == "TABLE":
            grid = {}
            for rel in b.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    for cid in rel["Ids"]:
                        c = by_id[cid]
                        if c["BlockType"] == "CELL":
                            grid.setdefault(c["RowIndex"], {})[c["ColumnIndex"]] = text_of(c, by_id)
            tables.append([[row[c] for c in sorted(row)] for _, row in sorted(grid.items())])
    return tables

"""S3 upload -> Lambda -> Textract (FORMS+TABLES) -> DynamoDB
Runtime: Python 3.12 | Env: TABLE_NAME (default DocumentResults)
"""
import os, json, logging, urllib.parse
from datetime import datetime, timezone
from decimal import Decimal
import boto3

logger = logging.getLogger(); logger.setLevel(logging.INFO)
textract = boto3.client("textract")
table = boto3.resource("dynamodb").Table(os.environ.get("TABLE_NAME", "DocumentResults"))


def lambda_handler(event, context):
    for rec in event["Records"]:
        bucket = rec["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])  # 空白會被編成 '+'
        if not key.lower().endswith(".pdf"):
            logger.info("skip non-pdf: %s", key); continue
        process(bucket, key)
    return {"statusCode": 200}


def process(bucket, key):
    doc_id = key.rsplit("/", 1)[-1]
    resp = textract.analyze_document(  # 同步 API:單頁 PDF/圖片
        Document={"S3Object": {"Bucket": bucket, "Name": key}},
        FeatureTypes=["FORMS", "TABLES"])
    blocks = resp["Blocks"]
    by_id = {b["Id"]: b for b in blocks}

    kvs = extract_kv(blocks, by_id)
    tables = extract_tables(blocks, by_id)
    now = datetime.now(timezone.utc).isoformat()

    with table.batch_writer() as bw:
        bw.put_item(Item={
            "documentId": doc_id, "sk": "META",
            "s3Bucket": bucket, "s3Key": key, "status": "PARSED",
            "processedAt": now, "kvCount": len(kvs), "tableCount": len(tables),
            "fields": {k["name"]: k["value"] for k in kvs},   # 方便一次讀完
        })
        for i, kv in enumerate(kvs, 1):                       # 每組 name/value 也各存一筆
            bw.put_item(Item={"documentId": doc_id, "sk": f"KV#{i:03d}",
                              "fieldName": kv["name"], "fieldValue": kv["value"],
                              "confidence": Decimal(str(round(kv["conf"], 2)))})
        for t, rows in enumerate(tables, 1):
            for r, row in enumerate(rows, 1):
                bw.put_item(Item={"documentId": doc_id, "sk": f"TBL#{t:02d}#ROW#{r:03d}",
                                  "cells": row})
    logger.info("saved %s: %d kv, %d tables", doc_id, len(kvs), len(tables))


def text_of(block, by_id):
    """把 KEY/VALUE/CELL 底下的 WORD 串成文字"""
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
            out.append({"name": text_of(b, by_id).rstrip(":").strip(),
                        "value": value_txt, "conf": conf})
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
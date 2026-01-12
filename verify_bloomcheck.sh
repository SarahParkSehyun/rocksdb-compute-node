#!/bin/bash
# BloomCheck 요청-응답 매칭 검증 스크립트

LOG_FILE="${1:-bloomcheck_debug.log}"

if [ ! -f "$LOG_FILE" ]; then
  echo "Usage: $0 <log_file>"
  echo "Example: $0 bloomcheck_debug.log"
  exit 1
fi

echo "Analyzing BloomCheck request-response pairs..."
echo "================================================"

# 임시 파일 생성
TMP_REQ=$(mktemp)
TMP_RESP=$(mktemp)

# 요청 추출
grep "Sending request.*user_key_hex=" "$LOG_FILE" | while read line; do
  key=$(echo "$line" | sed -n 's/.*user_key_hex=\([0-9A-F]*\).*/\1/p')
  echo "$key" >> "$TMP_REQ"
done

# 응답 추출
grep "Received response.*user_key_hex=" "$LOG_FILE" | while read line; do
  key=$(echo "$line" | sed -n 's/.*user_key_hex=\([0-9A-F]*\).*/\1/p')
  echo "$key" >> "$TMP_RESP"
done

# 매칭 확인
echo "Checking if all responses have matching requests..."
REQ_COUNT=$(wc -l < "$TMP_REQ")
RESP_COUNT=$(wc -l < "$TMP_RESP")

echo "Total requests: $REQ_COUNT"
echo "Total responses: $RESP_COUNT"

# 각 응답이 요청과 매칭되는지 확인
UNMATCHED=0
while read resp_key; do
  if ! grep -q "^$resp_key$" "$TMP_REQ"; then
    echo "WARNING: Response with key $resp_key has no matching request!"
    UNMATCHED=$((UNMATCHED + 1))
  fi
done < "$TMP_RESP"

if [ $UNMATCHED -eq 0 ]; then
  echo "✓ All responses have matching requests"
else
  echo "✗ Found $UNMATCHED unmatched responses"
fi

# 정리
rm -f "$TMP_REQ" "$TMP_RESP"

echo ""
echo "To see detailed request-response pairs, run:"
echo "  grep -A 20 'Sending request.*user_key_hex=YOUR_KEY' $LOG_FILE"


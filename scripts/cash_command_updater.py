"""
젠테 Cash Command 자동 업데이트 스크립트
=========================================
매일 KST 06:05 GitHub Actions에서 실행.
Google Sheets(합산관리 탭) + BigQuery(은행잔고) → jente_cash_command.html 업데이트.

사용법:
  python cash_command_updater.py                # 기본 실행
  python cash_command_updater.py --dry-run      # HTML 변경 없이 데이터만 확인
  python cash_command_updater.py --month 2026-02  # 특정 월 강제 지정
"""

import os
import sys
import json
import re
import logging
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 경로 설정
# ──────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
DATADECK_DIR = SCRIPT_DIR.parent
HTML_PATH = DATADECK_DIR / 'jente_cash_command.html'

MARKER_BEGIN = '// AUTO_UPDATE_BEGIN'
MARKER_END = '// AUTO_UPDATE_END'

# ──────────────────────────────────────────────
# RESET 시트 ID (매월 추가 필요)
# ──────────────────────────────────────────────
RESET_SHEET_IDS = {
    '2026-01': '19Y6bnLPmHUlKUTHzmJfhTgHRtDgt7UnHr5assPJNHC0',
    '2026-02': '1Dcq0EQdDTasi_ZahF0pDnd5zE3OgdXglXPrEQzXNVts',
    '2026-03': '1YkK5US7AQNjCC-IMlxTMy3DJrhAEyOEI7mUdDFdNP8o',
}

# 매출데이터 시트 (GMV 분해용: API판매 + 재고판매)
MAECHUL_SHEET_IDS = {
    '2026-01': '1RA_Y8zg7oH3ywzRqkLWIJCfKeCcSW8X0ggbNA05F1Zg',
    '2026-02': '1JnV7qOGPrUFZyfwClEquKWz9kNbmeYHOi-KBXmsA6Gc',
    '2026-03': '1FfOTrQFu_njmWH5mm5y-sHSKs7kcDAEt_lw-BRx_1eo',
}

# 합산관리 탭: 시트 내 22번째 탭 (0-indexed)
HAPSAN_TAB_INDEX = 22
HAPSAN_TAB_NAME = '합산관리'

# ──────────────────────────────────────────────
# 행 라벨 → 필드 매핑 (col C 기준)
# ──────────────────────────────────────────────
# 합산관리 탭 구조:
#   col A: 구분코드(합/원/비), col B: lvl, col C: 메인 라벨
#   col D: 서브 라벨 (lvl2), col E: 서서브 라벨 (lvl3)
#   col 14~: 일별 데이터 (3.1, 3.2, ...)
ROW_LABEL_MAP = [
    # (패턴, 필드명, 제외 패턴) — 순서 중요: 먼저 매칭되는 것 사용
    ('합산 실제 거래액', 'gmv', None),
    ('합산 실제 환불액', 'refund_current', None),
    ('실제 매출액', 'revenue', '목표'),
    ('실제 원가', 'cogs', '목표'),
    ('실제 매출총이익', 'gp', '목표'),
    ('실제 전월 환불액', 'refund_prev', '한도'),  # '실제 전월 환불액 한도' 제외
    ('실제 판매관리비', 'sga', '목표'),
    ('부채 상환', 'debt_repay', None),
    # ▼ 신규 — 현금 기준 메트릭
    ('통장 입금액', 'settlement', None),           # R52: "실제 입금액=통장 입금액"
]

# 목표 행 매핑 (col 13 = 월합계)
TARGET_LABEL_MAP = [
    ('합산 목표 거래액', 'gmv'),
    ('목표 매출액', 'revenue'),
    ('목표 원가', 'cogs'),
    ('목표 매출총이익', 'gp'),
    ('목표 전월 환불액 한도', 'refund_prev_limit'),
    ('목표 판매관리비', 'sga'),
    ('목표 입금액', 'deposit'),
]
# 서브 라벨 (col D, 부채 상환 하위)
SUB_LABEL_MAP = {
    '무신사': 'musinsa_repay',
    '부티크 송금예정액': 'cogs_boutique',  # R30: 실제 원가 하위
}

# BigQuery 설정
BQ_PROJECT = 'gowid-prd'
BQ_TABLE = 'dw_metric.balance__date__corporation'
BQ_CORP_ID = 5658801620


# ═══════════════════════════════════════════════
# A. 인증
# ═══════════════════════════════════════════════
def get_credentials():
    """Sheets용 + BQ용 credentials를 각각 반환. returns (sheets_creds, bq_creds)."""
    sheets_creds = _get_sheets_credentials()
    bq_creds = _get_bq_credentials()
    return sheets_creds, bq_creds


def _get_sheets_credentials():
    """Sheets API용: GOWID_ADC_JSON(SA) → 로컬 ADC(OAuth) 순서."""
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly',
    ]

    # 1) SHEETS_ADC_JSON 환경변수 (GitHub Actions — OAuth refresh token)
    sheets_json = os.environ.get('SHEETS_ADC_JSON')
    if sheets_json:
        adc_data = json.loads(sheets_json)
        if adc_data.get('type') == 'service_account':
            log.info("Sheets 인증: SHEETS_ADC_JSON (SA)")
            return _creds_from_sa_dict(adc_data, SCOPES)
        log.info("Sheets 인증: SHEETS_ADC_JSON (OAuth)")
        return _creds_from_adc_dict(adc_data)

    # 2) 로컬 ADC (OAuth — kwon@gowidnexus.com + pitstop quota)
    adc_paths = [
        os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', ''),
        str(Path.home() / '.config' / 'gcloud' / 'application_default_credentials.json'),
        str(Path(os.environ.get('APPDATA', '')) / 'gcloud' / 'application_default_credentials.json'),
    ]
    # Windows Microsoft Store Python 경로
    local_app = os.environ.get('LOCALAPPDATA', '')
    if local_app:
        import glob
        ms_paths = glob.glob(os.path.join(local_app, 'packages', 'PythonSoftwareFoundation*', 'LocalCache', 'Roaming', 'gcloud', 'application_default_credentials.json'))
        adc_paths.extend(ms_paths)

    for p in adc_paths:
        if p and os.path.exists(p):
            with open(p) as f:
                adc_data = json.load(f)
            if adc_data.get('type') == 'service_account':
                log.info(f"Sheets 인증: SA ({p})")
                return _creds_from_sa_dict(adc_data, SCOPES)
            log.info(f"Sheets 인증: OAuth ADC ({p})")
            return _creds_from_adc_dict(adc_data)

    log.warning("Sheets 인증 없음 — Sheets 데이터 수집 건너뜀")
    return None


def _get_bq_credentials():
    """BigQuery용: SA key 우선."""
    SCOPES = ['https://www.googleapis.com/auth/bigquery']

    # 1) GOWID_ADC_JSON 환경변수
    adc_json = os.environ.get('GOWID_ADC_JSON')
    if adc_json:
        adc_data = json.loads(adc_json)
        if adc_data.get('type') == 'service_account':
            log.info("BQ 인증: GOWID_ADC_JSON (SA)")
            return _creds_from_sa_dict(adc_data, SCOPES)

    # 2) 스크립트 옆 SA 키 파일
    sa_key_path = SCRIPT_DIR / 'gowid-prd-sa-key.json'
    if sa_key_path.exists():
        log.info(f"BQ 인증: SA 키 파일 ({sa_key_path})")
        with open(sa_key_path) as f:
            sa_data = json.load(f)
        return _creds_from_sa_dict(sa_data, SCOPES)

    # 3) 로컬 ADC 폴백
    adc_paths = [
        os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', ''),
        str(Path.home() / '.config' / 'gcloud' / 'application_default_credentials.json'),
        str(Path(os.environ.get('APPDATA', '')) / 'gcloud' / 'application_default_credentials.json'),
    ]
    for p in adc_paths:
        if p and os.path.exists(p):
            with open(p) as f:
                adc_data = json.load(f)
            if adc_data.get('type') == 'service_account':
                log.info(f"BQ 인증: SA ({p})")
                return _creds_from_sa_dict(adc_data, SCOPES)

    log.warning("BQ 인증 없음 — BigQuery 데이터 수집 건너뜀")
    return None


def _creds_from_sa_dict(sa_data, scopes):
    """Service Account JSON dict → credentials."""
    from google.oauth2.service_account import Credentials
    creds = Credentials.from_service_account_info(sa_data, scopes=scopes)
    log.info(f"SA 인증 완료: {sa_data.get('client_email')}")
    return creds


def _creds_from_adc_dict(adc_data):
    """ADC JSON dict → google.oauth2.credentials.Credentials."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    creds = Credentials(
        token=adc_data.get('access_token'),
        refresh_token=adc_data.get('refresh_token'),
        token_uri=adc_data.get('token_uri', 'https://oauth2.googleapis.com/token'),
        client_id=adc_data.get('client_id'),
        client_secret=adc_data.get('client_secret'),
        quota_project_id=adc_data.get('quota_project_id'),
    )

    if not creds.valid:
        log.info("토큰 갱신 중...")
        creds.refresh(Request())
        log.info("토큰 갱신 완료")

    return creds


# ═══════════════════════════════════════════════
# B. Google Sheets 데이터 수집
# ═══════════════════════════════════════════════
def fetch_sheets_data(creds, month_key):
    """합산관리 탭에서 일별 데이터 수집."""
    sheet_id = RESET_SHEET_IDS.get(month_key)
    if not sheet_id:
        log.warning(f"시트 ID 미등록: {month_key} → Sheets 건너뜀")
        return None

    from googleapiclient.discovery import build

    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)

    # 시트 메타데이터로 탭 이름 확인 (이름 기반 검색 우선)
    meta = service.spreadsheets().get(
        spreadsheetId=sheet_id,
        fields='sheets.properties'
    ).execute()

    sheets_list = meta.get('sheets', [])
    tab_name = None

    # 이름으로 먼저 검색
    for s in sheets_list:
        title = s['properties']['title']
        if HAPSAN_TAB_NAME in title:
            tab_name = title
            break

    # 이름으로 못 찾으면 인덱스 폴백
    if not tab_name and HAPSAN_TAB_INDEX < len(sheets_list):
        tab_name = sheets_list[HAPSAN_TAB_INDEX]['properties']['title']
        log.warning(f"이름 검색 실패, 인덱스 {HAPSAN_TAB_INDEX} 폴백: '{tab_name}'")

    if not tab_name:
        log.error("합산관리 탭을 찾을 수 없음")
        return None

    log.info(f"Sheets: {sheet_id} → 탭 '{tab_name}'")

    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!A1:BL100"
    ).execute()
    rows = result.get('values', [])
    if not rows:
        log.warning("합산관리 탭 데이터 없음")
        return None

    log.info(f"Sheets: {len(rows)}행 × {max(len(r) for r in rows)}열 수신")

    return _parse_hapsan(rows, month_key)


def _parse_hapsan(rows, month_key):
    """합산관리 탭 → daily dict 파싱.

    합산관리 탭 구조:
      col A(0): 구분코드  col B(1): lvl  col C(2): 메인 라벨
      col D(3): 서브 라벨  col E(4): 서서브 라벨
      col 14~: 일별 데이터 ('3. 1', '3. 2', ...)
    """
    import calendar
    year, mon = map(int, month_key.split('-'))
    days_in_month = calendar.monthrange(year, mon)[1]

    # 헤더행 찾기: 연속된 날짜 패턴 'M. 1', 'M. 2' (최소 2개 연속)
    day_col_start = None
    header_row_idx = None
    day1_patterns = [
        rf'^0?{mon}\.\s*0?1$',   # '3. 1', '03. 01'
        rf'^0?{mon}[-/]0?1$',    # '03-01', '3/1'
    ]
    day2_patterns = [
        rf'^0?{mon}\.\s*0?2$',
        rf'^0?{mon}[-/]0?2$',
    ]

    for i, row in enumerate(rows[:15]):
        for j, cell in enumerate(row):
            cell_str = str(cell).strip()
            is_day1 = any(re.match(p, cell_str) for p in day1_patterns)
            if not is_day1:
                continue
            # 연속 day2 확인 (바로 다음 열)
            if j + 1 < len(row):
                next_str = str(row[j + 1]).strip()
                is_day2 = any(re.match(p, next_str) for p in day2_patterns)
                if is_day2:
                    day_col_start = j
                    header_row_idx = i
                    break
        if day_col_start is not None:
            break

    if day_col_start is None:
        log.warning("일별 컬럼 오프셋을 찾을 수 없음 — 기본값 14 사용")
        day_col_start = 14
        header_row_idx = 7

    log.info(f"일별 컬럼 시작: col={day_col_start}, 헤더행={header_row_idx}")

    # 데이터 파싱
    daily = {}
    for field in ['gmv', 'refund_current', 'refund_prev', 'revenue', 'cogs',
                   'gp', 'sga', 'debt_repay', 'musinsa_repay', 'card_repay',
                   'settlement', 'cogs_boutique', 'cogs_import', 'cash_margin',
                   'api_sales', 'inventory_sales']:
        daily[field] = [None] * days_in_month

    for row in rows[header_row_idx + 1:]:
        if not row or len(row) < 3:
            continue

        # 메인 라벨 (col C, index 2)
        main_label = str(row[2]).strip() if len(row) > 2 else ''
        # 서브 라벨 (col D, index 3)
        sub_label = str(row[3]).strip() if len(row) > 3 else ''

        # 메인 라벨 매칭 (제외 패턴 적용)
        field = None
        for pattern, fld, exclude in ROW_LABEL_MAP:
            if pattern in main_label:
                if exclude and exclude in main_label:
                    continue
                field = fld
                break

        # 서브 라벨 매칭 (메인 라벨이 비어있을 때)
        if not field and sub_label:
            for pattern, fld in SUB_LABEL_MAP.items():
                if pattern == sub_label or pattern in sub_label:
                    field = fld
                    break

        if not field:
            continue

        log.info(f"  매핑: main='{main_label}' sub='{sub_label}' → {field}")

        for day_idx in range(days_in_month):
            col = day_col_start + day_idx
            if col >= len(row):
                break
            val = _parse_number(row[col])
            if val is not None:
                daily[field][day_idx] = val

    # gp 자동 계산 (revenue - cogs) — 시트 값 없으면 계산
    for i in range(days_in_month):
        if daily['gp'][i] is None and daily['revenue'][i] is not None and daily['cogs'][i] is not None:
            daily['gp'][i] = daily['revenue'][i] - daily['cogs'][i]

    # cogs_import = cogs - cogs_boutique (부대비용 자동 산출)
    for i in range(days_in_month):
        if daily['cogs'][i] is not None and daily['cogs_boutique'][i] is not None:
            daily['cogs_import'][i] = daily['cogs'][i] - daily['cogs_boutique'][i]

    # cash_margin = settlement - cogs_boutique - cogs_import
    for i in range(days_in_month):
        if daily['settlement'][i] is not None:
            cb = daily['cogs_boutique'][i] or 0
            ci = daily['cogs_import'][i] or 0
            daily['cash_margin'][i] = daily['settlement'][i] - cb - ci

    # 목표 행 파싱 (col 13 = 월합계)
    targets = {}
    for row in rows[header_row_idx + 1:]:
        if not row or len(row) < 14:
            continue
        main_label = str(row[2]).strip() if len(row) > 2 else ''
        for pattern, tgt_key in TARGET_LABEL_MAP:
            if pattern in main_label:
                val = _parse_number(row[13]) if len(row) > 13 else None
                if val is not None:
                    targets[tgt_key] = val
                    log.info(f"  목표: '{main_label}' → {tgt_key} = {val:,}")
                break

    return daily, targets


def _parse_number(val):
    """셀 값 → int 또는 None."""
    if val is None or val == '' or val == '-':
        return None
    s = str(val).strip().replace(',', '').replace('₩', '').replace('원', '')
    # 괄호 = 음수: (1234) → -1234
    if s.startswith('(') and s.endswith(')'):
        s = '-' + s[1:-1]
    try:
        f = float(s)
        return int(round(f))
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════
# B-2. GMV 분해 (매출_jentestore 탭: API판매 + 재고판매)
# ═══════════════════════════════════════════════
def fetch_gmv_breakdown(creds, month_key):
    """매출_jentestore 탭에서 GMV = API판매 + 재고판매 일별 분해 추출.

    매출_jentestore 탭 구조:
      R2: 날짜 헤더 (결제일, '', '', 2026-03-01, 2026-03-02, ...)
      R3: 결제액 (GMV) — col 2=합계, col 3~=일별
      R4: ㄴ API
      R5: ㄴ 위탁
      R6: ㄴ 매입
      R7: ㄴ 반품
    """
    sheet_id = MAECHUL_SHEET_IDS.get(month_key)
    if not sheet_id:
        log.warning(f"매출데이터 시트 미등록: {month_key} → GMV 분해 건너뜀")
        return None

    from googleapiclient.discovery import build
    import calendar
    year, mon = map(int, month_key.split('-'))
    days_in_month = calendar.monthrange(year, mon)[1]

    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"'매출_jentestore'!A1:AP10"
        ).execute()
        rows = result.get('values', [])
    except Exception as e:
        log.warning(f"매출_jentestore 탭 읽기 실패: {e}")
        return None

    if len(rows) < 7:
        log.warning("매출_jentestore 데이터 부족")
        return None

    dates_row = rows[1]  # R2: 날짜 헤더
    gmv_row = rows[2]    # R3: 결제액 (GMV)
    api_row = rows[3]    # R4: API
    consign_row = rows[4]  # R5: 위탁
    purchase_row = rows[5]  # R6: 매입
    returns_row = rows[6]   # R7: 반품

    api_sales = [None] * days_in_month
    inventory_sales = [None] * days_in_month

    for col_idx in range(3, len(dates_row)):
        date_str = str(dates_row[col_idx]).strip()
        if not date_str.startswith(f"{year}-{mon:02d}-"):
            continue
        try:
            day = int(date_str.split('-')[2])
        except (IndexError, ValueError):
            continue
        if day < 1 or day > days_in_month:
            continue
        day_idx = day - 1

        api_val = _parse_number(api_row[col_idx]) if col_idx < len(api_row) else None
        consign_val = _parse_number(consign_row[col_idx]) if col_idx < len(consign_row) else None
        purchase_val = _parse_number(purchase_row[col_idx]) if col_idx < len(purchase_row) else None
        returns_val = _parse_number(returns_row[col_idx]) if col_idx < len(returns_row) else None

        if api_val is not None:
            api_sales[day_idx] = api_val
        inv = (consign_val or 0) + (purchase_val or 0) + (returns_val or 0)
        if consign_val is not None or purchase_val is not None or returns_val is not None:
            inventory_sales[day_idx] = inv

    filled = sum(1 for v in api_sales if v is not None)
    log.info(f"GMV 분해: {filled}일 (API+재고)")
    return {'api_sales': api_sales, 'inventory_sales': inventory_sales}


def fetch_daily_mur(creds, month_key):
    """RESET [일별실적_API] 탭 62행에서 일별 누적 MUR(부대,vat환급고려) 수집.

    컬럼 레이아웃: col[14]=1일, col[15]=2일, ... col[13+day]=day일
    """
    sheet_id = RESET_SHEET_IDS.get(month_key)
    if not sheet_id:
        log.warning(f"RESET 시트 미등록: {month_key} → MUR 건너뜀")
        return None

    import calendar
    from googleapiclient.discovery import build
    year, mon = map(int, month_key.split('-'))
    days_in_month = calendar.monthrange(year, mon)[1]

    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range="'일별실적_API'!A62:AP62"
        ).execute()
        row = result.get('values', [[]])[0]
    except Exception as e:
        log.warning(f"일별실적_API 탭 읽기 실패: {e}")
        return None

    mur = [None] * days_in_month
    filled = 0
    for day in range(1, days_in_month + 1):
        col = 13 + day  # col[14]=1일, col[15]=2일, ...
        if col >= len(row):
            break
        val = str(row[col]).strip().replace('%', '')
        if not val:
            continue
        try:
            mur[day - 1] = round(float(val), 2)
            filled += 1
        except ValueError:
            continue

    log.info(f"MUR 수집: {filled}일")
    return mur


def fetch_pg_settlement(creds, month_key):
    """jentestore_승인(카드/가상계좌) 탭에서 주문별 PG 정산액을 수집, 일별 합산 반환.

    이를 통해 GMV(매출_jentestore)와 정산예정액이 같은 주문 기반으로 연결된다.
    RESET R52 수기값 대신 주문 단위 PG 정산 합산을 사용.

    Returns:
        dict: {'settlement_pg': [day1, day2, ...]} — 일별 PG 정산 합산 배열
    """
    sheet_id = MAECHUL_SHEET_IDS.get(month_key)
    if not sheet_id:
        log.warning(f"매출데이터 시트 미등록: {month_key} → PG 정산 건너뜀")
        return None

    from googleapiclient.discovery import build
    import calendar
    year, mon = map(int, month_key.split('-'))
    days_in_month = calendar.monthrange(year, mon)[1]

    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    settlement_by_day = [0.0] * days_in_month
    has_data = [False] * days_in_month

    # ─── 1. foret 탭: 주문번호 → 결제일 매핑 ───
    try:
        foret_result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range="'foret'!A1:J10000"
        ).execute()
        foret_rows = foret_result.get('values', [])
    except Exception as e:
        log.warning(f"foret 탭 읽기 실패: {e}")
        return None

    # 주문번호 → pay_date 매핑
    order_date = {}  # order_no -> day_idx (0-based)
    if len(foret_rows) > 2:
        for r in foret_rows[2:]:
            if len(r) < 3:
                continue
            order_no = str(r[1]).strip() if len(r) > 1 else ''
            pay_date = str(r[2]).strip() if len(r) > 2 else ''
            if not order_no or not pay_date.startswith(f"{year}-{mon:02d}"):
                continue
            try:
                day = int(pay_date.split('-')[2])
                if 1 <= day <= days_in_month:
                    order_date[order_no] = day - 1
            except (IndexError, ValueError):
                continue
    log.info(f"PG 정산: foret {len(order_date)}개 주문 날짜 매핑")

    # ─── 2. jentestore_승인(카드) ───
    card_count = 0
    try:
        card_result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range="'jentestore_승인(카드)'!A1:Z10000"
        ).execute()
        card_rows = card_result.get('values', [])
        if len(card_rows) > 2:
            for r in card_rows[2:]:
                if not r or len(r) < 5:
                    continue
                ono = str(r[0]).strip()
                if ono in order_date:
                    settlement_val = _parse_number(r[4])
                    if settlement_val is not None:
                        day_idx = order_date[ono]
                        settlement_by_day[day_idx] += settlement_val
                        has_data[day_idx] = True
                        card_count += 1
    except Exception as e:
        log.warning(f"승인(카드) 탭 읽기 실패: {e}")

    log.info(f"PG 정산: 카드 {card_count}건 매칭")

    # ─── 3. jentestore_승인(가상계좌) ───
    va_count = 0
    try:
        va_result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range="'jentestore_승인(가상계좌)'!A1:Z10000"
        ).execute()
        va_rows = va_result.get('values', [])
        if len(va_rows) > 2:
            for r in va_rows[2:]:
                if not r or len(r) < 4:
                    continue
                ono = str(r[0]).strip()
                if ono in order_date:
                    settlement_val = _parse_number(r[3])
                    if settlement_val is not None:
                        day_idx = order_date[ono]
                        settlement_by_day[day_idx] += settlement_val
                        has_data[day_idx] = True
                        va_count += 1
    except Exception as e:
        log.warning(f"승인(가상계좌) 탭 읽기 실패: {e}")

    log.info(f"PG 정산: 가상계좌 {va_count}건 매칭")

    # 결과: 데이터 있는 날만 값, 없는 날은 None
    settlement_pg = []
    for i in range(days_in_month):
        if has_data[i]:
            settlement_pg.append(int(settlement_by_day[i]))
        else:
            settlement_pg.append(None)

    total = sum(v for v in settlement_pg if v is not None)
    filled = sum(1 for v in settlement_pg if v is not None)
    log.info(f"PG 정산 합산: {filled}일, 총 {total:,.0f}원 (카드 {card_count} + 가상계좌 {va_count})")

    return {'settlement_pg': settlement_pg}


# ═══════════════════════════════════════════════
# C. BigQuery 은행잔고
# ═══════════════════════════════════════════════
def fetch_bank_data(creds):
    """DW에서 은행잔고 조회."""
    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=BQ_PROJECT, credentials=creds)

        # daily: 최근 14일
        sql_daily = f"""
        SELECT date_id, normal_account_balance
        FROM `{BQ_PROJECT}.{BQ_TABLE}`
        WHERE corp_id = {BQ_CORP_ID}
          AND date_id >= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 14 DAY)
        ORDER BY date_id DESC
        """

        daily_rows = list(client.query(sql_daily).result())
        daily = []
        for row in daily_rows:
            d = row.date_id
            date_str = d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else f"{str(d)[:4]}-{str(d)[4:6]}-{str(d)[6:8]}"
            daily.append({'date': date_str, 'balance': int(row.normal_account_balance)})

        log.info(f"BigQuery daily: {len(daily)}건")

        # monthly: 최근 6개월 (각 월 마지막 날)
        sql_monthly = f"""
        WITH ranked AS (
          SELECT date_id, normal_account_balance,
                 ROW_NUMBER() OVER (
                   PARTITION BY FORMAT_DATE('%Y%m', date_id)
                   ORDER BY date_id DESC
                 ) AS rn
          FROM `{BQ_PROJECT}.{BQ_TABLE}`
          WHERE corp_id = {BQ_CORP_ID}
            AND date_id >= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 6 MONTH)
        )
        SELECT date_id, normal_account_balance
        FROM ranked WHERE rn = 1
        ORDER BY date_id DESC
        """

        monthly_rows = list(client.query(sql_monthly).result())
        monthly = []
        for row in monthly_rows:
            d = row.date_id
            month_str = d.strftime('%Y-%m') if hasattr(d, 'strftime') else f"{str(d)[:4]}-{str(d)[4:6]}"
            monthly.append({'month': month_str, 'balance': int(row.normal_account_balance)})

        log.info(f"BigQuery monthly: {len(monthly)}건")

        return {'daily': daily, 'monthly': monthly, 'source': f'gowid-prd DW (corp_id:{BQ_CORP_ID})'}

    except Exception as e:
        log.error(f"BigQuery 실패: {e}")
        return None


# ═══════════════════════════════════════════════
# D. CASH_DATA 병합
# ═══════════════════════════════════════════════
def merge_cash_data(existing_data, sheets_daily, bank_data, month_key, today, targets=None, gmv_breakdown=None, pg_settlement=None, daily_mur=None):
    """기존 CASH_DATA에 새 데이터 병합."""
    import calendar
    year, mon = map(int, month_key.split('-'))
    days_in_month = calendar.monthrange(year, mon)[1]

    data = json.loads(json.dumps(existing_data))  # deep copy

    # D-1. daily 병합 (어제까지만 업데이트 — 06:05 실행 시 오늘 데이터 미완성)
    # 같은 월이면 어제까지, 다른 월이면 전체
    if today.year == year and today.month == mon:
        max_day_idx = today.day - 2  # day 1 = index 0, yesterday = index (today.day - 2)
    else:
        max_day_idx = days_in_month - 1  # 이전 월은 전체 업데이트

    if sheets_daily:
        for field in ['gmv', 'refund_current', 'refund_prev', 'revenue', 'cogs',
                       'gp', 'sga', 'debt_repay', 'musinsa_repay', 'card_repay',
                       'settlement', 'cogs_boutique', 'cogs_import', 'cash_margin',
                       'api_sales', 'inventory_sales']:
            existing_arr = data.get('daily', {}).get(field, [None] * days_in_month)
            new_arr = sheets_daily.get(field, [None] * days_in_month)

            # 배열 길이 맞추기
            while len(existing_arr) < days_in_month:
                existing_arr.append(None)
            while len(new_arr) < days_in_month:
                new_arr.append(None)

            # 어제까지만 새 데이터로 업데이트, 이후는 기존값 유지
            merged = []
            for i in range(days_in_month):
                if i <= max_day_idx and new_arr[i] is not None:
                    merged.append(new_arr[i])
                else:
                    merged.append(existing_arr[i])
            data['daily'][field] = merged

        data['daily']['month'] = month_key
        data['daily']['days'] = days_in_month

    # D-1b. GMV 분해 병합 (api_sales, inventory_sales)
    if gmv_breakdown:
        for field in ['api_sales', 'inventory_sales']:
            existing_arr = data.get('daily', {}).get(field, [None] * days_in_month)
            new_arr = gmv_breakdown.get(field, [None] * days_in_month)
            while len(existing_arr) < days_in_month:
                existing_arr.append(None)
            while len(new_arr) < days_in_month:
                new_arr.append(None)
            merged = []
            for i in range(days_in_month):
                if i <= max_day_idx and new_arr[i] is not None:
                    merged.append(new_arr[i])
                else:
                    merged.append(existing_arr[i])
            data['daily'][field] = merged

    # D-1c. PG 정산 기반 settlement 병합 (주문별 합산 → 일별)
    if pg_settlement:
        new_arr = pg_settlement.get('settlement_pg', [None] * days_in_month)
        existing_arr = data.get('daily', {}).get('settlement_pg', [None] * days_in_month)
        while len(existing_arr) < days_in_month:
            existing_arr.append(None)
        while len(new_arr) < days_in_month:
            new_arr.append(None)
        merged = []
        for i in range(days_in_month):
            if i <= max_day_idx and new_arr[i] is not None:
                merged.append(new_arr[i])
            else:
                merged.append(existing_arr[i])
        data['daily']['settlement_pg'] = merged

    # D-1d. 일별 MUR 병합
    if daily_mur:
        existing_arr = data.get('daily', {}).get('mur', [None] * days_in_month)
        while len(existing_arr) < days_in_month:
            existing_arr.append(None)
        while len(daily_mur) < days_in_month:
            daily_mur.append(None)
        merged = []
        for i in range(days_in_month):
            if i <= max_day_idx and daily_mur[i] is not None:
                merged.append(daily_mur[i])
            else:
                merged.append(existing_arr[i])
        data['daily']['mur'] = merged

    # D-2. bank 병합
    if bank_data:
        data['bank'] = bank_data

    # D-3. monthly 현재 월 재계산 (daily 합산)
    if sheets_daily and month_key in data.get('monthly', {}):
        daily_d = data['daily']
        m = data['monthly'][month_key]
        m['gmv'] = _sum_arr(daily_d.get('gmv', []))
        m['refund'] = _sum_arr(daily_d.get('refund_current', []))
        total_gmv = m['gmv'] or 1
        m['refund_rate'] = round((m['refund'] or 0) / total_gmv, 3) if total_gmv else 0
        m['revenue'] = _sum_arr(daily_d.get('revenue', []))
        m['cogs_cash'] = _sum_arr(daily_d.get('cogs', []))
        gp = _sum_arr(daily_d.get('gp', []))
        m['gp'] = gp
        rev = m['revenue'] or 1
        m['gp_rate'] = round(gp / rev, 3) if rev else 0
        m['refund_prev'] = _sum_arr(daily_d.get('refund_prev', []))
        m['sga'] = _sum_arr(daily_d.get('sga', []))
        m['op_cf'] = (gp or 0) - (m['sga'] or 0)
        # 현금 기준 메트릭
        m['settlement'] = _sum_arr(daily_d.get('settlement', []))
        m['cogs_boutique'] = _sum_arr(daily_d.get('cogs_boutique', []))
        m['cogs_import'] = _sum_arr(daily_d.get('cogs_import', []))
        m['cash_margin'] = _sum_arr(daily_d.get('cash_margin', []))
        settlement = m['settlement'] or 1
        m['cash_margin_rate'] = round((m['cash_margin'] or 0) / settlement, 3) if settlement else 0
        # PG 정산 기반 settlement
        m['settlement_pg'] = _sum_arr(daily_d.get('settlement_pg', []))
        # GMV 분해
        m['api_sales'] = _sum_arr(daily_d.get('api_sales', []))
        m['inventory_sales'] = _sum_arr(daily_d.get('inventory_sales', []))
        m['fin_debt_repay'] = _sum_arr(daily_d.get('debt_repay', []))
        m['fin_musinsa'] = _sum_arr(daily_d.get('musinsa_repay', []))
        m['fin_card'] = _sum_arr(daily_d.get('card_repay', []))
        m['fin_cf'] = -((m['fin_debt_repay'] or 0) + (m['fin_musinsa'] or 0) + (m['fin_card'] or 0))
        m['net_cash_real'] = (m['op_cf'] or 0) + (m['fin_cf'] or 0)
        m['mtd'] = True

    # D-4. meta 업데이트
    bdays = _count_business_days(year, mon)
    elapsed = _count_elapsed_bdays(year, mon, today)
    data['meta'] = {
        'updated': today.strftime('%Y-%m-%d'),
        'updated_time': today.strftime('%H:%M'),
        'current_month': month_key,
        'business_days': bdays,
        'elapsed_bdays': elapsed,
        'today': today.day
    }
    data['daily']['business_days'] = bdays
    data['daily']['elapsed_bdays'] = elapsed

    # D-5. targets 병합 (RESET 목표 → CASH_DATA.targets)
    if targets:
        mon_idx = mon - 1  # 0-based
        existing_targets = data.get('targets', {})
        for key, val in targets.items():
            if key not in existing_targets:
                existing_targets[key] = [None] * 12
            elif len(existing_targets[key]) < 12:
                existing_targets[key].extend([None] * (12 - len(existing_targets[key])))
            existing_targets[key][mon_idx] = val
        data['targets'] = existing_targets
        log.info(f"목표 {len(targets)}개 항목 병합 → month_idx={mon_idx}")

    return data


def _sum_arr(arr):
    """배열 합산 (None 무시)."""
    total = sum(v for v in arr if v is not None)
    return total if any(v is not None for v in arr) else None


def _count_business_days(year, month):
    """월간 영업일 수 (일요일만 제외 — 이커머스 토요일 매출 포함)."""
    import calendar
    days = calendar.monthrange(year, month)[1]
    count = 0
    for d in range(1, days + 1):
        wd = datetime(year, month, d).weekday()
        if wd < 6:  # 월~토 (일요일=6만 제외)
            count += 1
    return count


def _count_elapsed_bdays(year, month, today):
    """현재까지 경과 영업일 (일요일만 제외)."""
    count = 0
    end_day = min(today.day, 31)
    if today.year != year or today.month != month:
        return _count_business_days(year, month)
    for d in range(1, end_day + 1):
        wd = datetime(year, month, d).weekday()
        if wd < 6:  # 월~토
            count += 1
    return count


# ═══════════════════════════════════════════════
# E. HTML 업데이트
# ═══════════════════════════════════════════════
def read_existing_cash_data(html_content):
    """HTML에서 기존 CASH_DATA 파싱."""
    begin_idx = html_content.find(MARKER_BEGIN)
    end_idx = html_content.find(MARKER_END)
    if begin_idx == -1 or end_idx == -1:
        raise ValueError(f"HTML 마커 누락: {MARKER_BEGIN} 또는 {MARKER_END}")

    # CASH_DATA = { ... }; 블록 추출
    block = html_content[begin_idx + len(MARKER_BEGIN):end_idx]

    # JS 객체를 JSON으로 변환
    json_str = _js_obj_to_json(block)
    return json.loads(json_str)


def _js_obj_to_json(js_block):
    """JS 객체 리터럴 → JSON 변환 (state machine 방식)."""
    s = js_block.strip()
    s = re.sub(r'^const\s+CASH_DATA\s*=\s*', '', s)
    s = s.rstrip().rstrip(';').rstrip()

    # Phase 1: 주석 제거 (문자열 내부 보호)
    out = []
    i = 0
    in_sq = False
    in_dq = False
    while i < len(s):
        c = s[i]
        if in_sq:
            out.append(c)
            if c == '\\' and i + 1 < len(s):
                out.append(s[i + 1])
                i += 2
                continue
            if c == "'":
                in_sq = False
        elif in_dq:
            out.append(c)
            if c == '\\' and i + 1 < len(s):
                out.append(s[i + 1])
                i += 2
                continue
            if c == '"':
                in_dq = False
        elif c == '/' and i + 1 < len(s) and s[i + 1] == '/':
            while i < len(s) and s[i] != '\n':
                i += 1
            continue
        elif c == "'":
            in_sq = True
            out.append(c)
        elif c == '"':
            in_dq = True
            out.append(c)
        else:
            out.append(c)
        i += 1
    s = ''.join(out)

    # Phase 2: 작은따옴표 문자열 → 큰따옴표 문자열
    out = []
    i = 0
    while i < len(s):
        if s[i] == "'":
            j = i + 1
            content = []
            while j < len(s) and s[j] != "'":
                if s[j] == '\\' and j + 1 < len(s):
                    content.append(s[j])
                    content.append(s[j + 1])
                    j += 2
                elif s[j] == '"':
                    content.append('\\"')
                    j += 1
                else:
                    content.append(s[j])
                    j += 1
            out.append('"')
            out.extend(content)
            out.append('"')
            i = j + 1
        else:
            out.append(s[i])
            i += 1
    s = ''.join(out)

    # Phase 3: 따옴표 없는 식별자 키 → 큰따옴표
    s = re.sub(r'(?<=[\{,\n])\s*([a-zA-Z_]\w*)\s*:', r' "\1":', s)

    # Phase 4: 따옴표 없는 숫자 키 → 큰따옴표
    s = re.sub(r'(?<=[\{,\n])\s*(\d+)\s*:', r' "\1":', s)

    # Phase 5: trailing comma 제거
    s = re.sub(r',(\s*[\]}])', r'\1', s)

    return s


def render_cash_data_js(data):
    """CASH_DATA dict → JS 코드 문자열."""
    lines = []
    lines.append('const CASH_DATA = {')

    # daily
    d = data['daily']
    lines.append('  daily: {')
    lines.append(f"    month:'{d['month']}', days:{d['days']}, business_days:{d['business_days']}, elapsed_bdays:{d['elapsed_bdays']},")
    for field in ['gmv', 'refund_current', 'refund_prev', 'revenue', 'cogs', 'gp', 'sga', 'debt_repay', 'musinsa_repay', 'card_repay',
                   'settlement', 'settlement_pg', 'cogs_boutique', 'cogs_import', 'cash_margin',
                   'api_sales', 'inventory_sales']:
        arr = d.get(field, [])
        arr_str = ','.join('null' if v is None else str(v) for v in arr)
        padding = ' ' * max(0, 16 - len(field))
        lines.append(f"    {field}:{padding}[{arr_str}],")
    # mur
    mur = d.get('mur', [None] * d['days'])
    mur_str = ','.join('null' if v is None else str(v) for v in mur)
    lines.append(f"    // MUR: [일별실적_API] 탭 58행 참조. 일별 마크업률(%)")
    lines.append(f"    mur:            [{mur_str}]")
    lines.append('  },')

    # monthly
    lines.append('  monthly: {')
    for mk, mv in data['monthly'].items():
        parts = []
        for k, v in mv.items():
            if isinstance(v, bool):
                parts.append(f'{k}:{"true" if v else "false"}')
            elif v is None:
                parts.append(f'{k}:null')
            elif isinstance(v, float):
                parts.append(f'{k}:{v}')
            else:
                parts.append(f'{k}:{v}')
        line = ', '.join(parts)
        lines.append(f"    '{mk}': {{ {line} }},")
    lines.append('  },')

    # inventory
    lines.append('  inventory: ' + _dict_to_js(data['inventory'], indent=2) + ',')

    # returns
    lines.append('  returns: ' + _dict_to_js(data['returns'], indent=2) + ',')

    # debt
    lines.append('  debt: ' + _dict_to_js(data['debt'], indent=2) + ',')

    # working_capital
    lines.append('  working_capital: ' + _dict_to_js(data['working_capital'], indent=2) + ',')

    # schedule_annual
    lines.append('  schedule_annual: {')
    lines.append('    items: [')
    for item in data['schedule_annual']['items']:
        tip_escaped = item['tip'].replace("'", "\\'")
        vals_str = ','.join('null' if v is None else str(v) for v in item['vals'])
        lines.append(f"      {{cat:'{item['cat']}',label:'{item['label']}',tip:'{tip_escaped}',vals:[{vals_str}]}},")
    lines.append('    ],')
    opcf = data['schedule_annual']['op_cf_est']
    opcf_str = ','.join('null' if v is None else str(v) for v in opcf)
    lines.append(f"    op_cf_est:[{opcf_str}]")
    lines.append('  },')

    # schedule_monthly
    lines.append('  schedule_monthly: {')
    for day_key, items in data['schedule_monthly'].items():
        item_strs = []
        for item in items:
            tip_escaped = item['tip'].replace("'", "\\'")
            item_strs.append(f"{{label:'{item['label']}',amount:{item['amount']},tip:'{tip_escaped}'}}")
        lines.append(f"    {day_key}:[{','.join(item_strs)}],")
    lines.append('  },')

    # bank
    lines.append('  // DW 은행잔고 (고위드 연동 계좌, corp_id:5658801620)')
    lines.append('  bank: {')
    lines.append('    daily: [')
    for b in data['bank']['daily']:
        lines.append(f"      {{date:'{b['date']}', balance:{b['balance']}}},")
    lines.append('    ],')
    lines.append('    monthly: [')
    for b in data['bank']['monthly']:
        lines.append(f"      {{month:'{b['month']}', balance:{b['balance']}}},")
    lines.append('    ],')
    lines.append(f"    source: '{data['bank']['source']}'")
    lines.append('  },')

    # targets (RESET 시트 목표값, 월별 12개 배열)
    if 'targets' in data and data['targets']:
        lines.append('  // RESET 시트 목표값 (월별 1~12월, 원 단위)')
        lines.append('  targets: {')
        for tgt_key, tgt_arr in data['targets'].items():
            arr_str = ','.join('null' if v is None else str(v) for v in tgt_arr)
            lines.append(f"    {tgt_key}: [{arr_str}],")
        lines.append('  },')

    # meta
    m = data['meta']
    lines.append(f"  meta: {{ updated:'{m['updated']}', updated_time:'{m['updated_time']}', current_month:'{m['current_month']}', business_days:{m['business_days']}, elapsed_bdays:{m['elapsed_bdays']}, today:{m['today']} }}")

    lines.append('};')

    return '\n'.join(lines)


def _dict_to_js(obj, indent=0):
    """간단한 dict/list → JS 리터럴 변환 (compact)."""
    prefix = '  ' * indent
    if isinstance(obj, dict):
        parts = []
        for k, v in obj.items():
            val_str = _dict_to_js(v, indent + 1)
            # 키에 따옴표 필요 여부
            if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', str(k)):
                parts.append(f"{k}:{val_str}")
            else:
                parts.append(f"'{k}':{val_str}")
        inner = ', '.join(parts)
        # 길이가 길면 멀티라인
        if len(inner) > 120:
            lines_inner = []
            for k, v in obj.items():
                val_str = _dict_to_js(v, indent + 1)
                if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', str(k)):
                    lines_inner.append(f"{'  ' * (indent + 1)}{k}:{val_str}")
                else:
                    lines_inner.append(f"{'  ' * (indent + 1)}'{k}':{val_str}")
            return '{\n' + ',\n'.join(lines_inner) + '\n' + prefix + '}'
        return '{' + inner + '}'
    elif isinstance(obj, list):
        items = [_dict_to_js(v, indent) for v in obj]
        inner = ','.join(items)
        if len(inner) > 100:
            return '[\n' + ',\n'.join(f"{'  ' * (indent + 1)}{item}" for item in items) + '\n' + prefix + ']'
        return '[' + inner + ']'
    elif isinstance(obj, bool):
        return 'true' if obj else 'false'
    elif obj is None:
        return 'null'
    elif isinstance(obj, str):
        return f"'{obj}'"
    elif isinstance(obj, float):
        return str(obj)
    else:
        return str(obj)


def update_html(html_content, new_js_block):
    """HTML에서 AUTO_UPDATE 마커 사이의 CASH_DATA 블록 교체."""
    begin_idx = html_content.find(MARKER_BEGIN)
    end_idx = html_content.find(MARKER_END)

    if begin_idx == -1 or end_idx == -1:
        raise ValueError(f"HTML 마커 누락: MARKER_BEGIN={begin_idx != -1}, MARKER_END={end_idx != -1}")

    before = html_content[:begin_idx + len(MARKER_BEGIN)]
    after = html_content[end_idx:]

    return before + '\n' + new_js_block + '\n' + after


# ═══════════════════════════════════════════════
# main
# ═══════════════════════════════════════════════
def main():
    import argparse
    parser = argparse.ArgumentParser(description='젠테 Cash Command 자동 업데이트')
    parser.add_argument('--dry-run', action='store_true', help='HTML 변경 없이 데이터만 확인')
    parser.add_argument('--month', type=str, help='대상 월 (예: 2026-03)')
    args = parser.parse_args()

    # KST 기준 현재 시각
    from datetime import timezone
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    today = now.replace(tzinfo=None)

    month_key = args.month or today.strftime('%Y-%m')
    log.info(f"대상 월: {month_key}, 오늘: {today.strftime('%Y-%m-%d %H:%M')}")

    # HTML 읽기
    if not HTML_PATH.exists():
        log.error(f"HTML 파일 없음: {HTML_PATH}")
        sys.exit(1)

    html_content = HTML_PATH.read_text(encoding='utf-8')
    log.info(f"HTML 로드: {HTML_PATH} ({len(html_content):,} bytes)")

    # 기존 데이터 파싱
    try:
        existing = read_existing_cash_data(html_content)
        log.info("기존 CASH_DATA 파싱 완료")
    except Exception as e:
        log.error(f"CASH_DATA 파싱 실패: {e}")
        sys.exit(1)

    # 인증 (Sheets용 + BQ용 분리)
    try:
        sheets_creds, bq_creds = get_credentials()
    except Exception as e:
        log.error(f"인증 실패: {e}")
        sys.exit(1)

    # Sheets 데이터 수집
    sheets_daily = None
    sheets_targets = None
    if sheets_creds:
        try:
            result = fetch_sheets_data(sheets_creds, month_key)
            if result:
                sheets_daily, sheets_targets = result
                filled = sum(1 for v in sheets_daily.get('gmv', []) if v is not None)
                log.info(f"Sheets: {filled}일 데이터 + {len(sheets_targets)}개 목표 수집 완료")
        except Exception as e:
            log.warning(f"Sheets 실패 (partial update 계속): {e}")

    # GMV 분해 (매출_jentestore: API판매 + 재고판매)
    gmv_breakdown = None
    if sheets_creds:
        try:
            gmv_breakdown = fetch_gmv_breakdown(sheets_creds, month_key)
        except Exception as e:
            log.warning(f"GMV 분해 실패 (기존 데이터 유지): {e}")

    # PG 정산 (주문별 합산 → 일별 settlement_pg)
    pg_settlement = None
    if sheets_creds:
        try:
            pg_settlement = fetch_pg_settlement(sheets_creds, month_key)
        except Exception as e:
            log.warning(f"PG 정산 실패 (기존 데이터 유지): {e}")

    # 일별 MUR (RESET [일별실적_API] 탭)
    daily_mur = None
    if sheets_creds:
        try:
            daily_mur = fetch_daily_mur(sheets_creds, month_key)
        except Exception as e:
            log.warning(f"MUR 수집 실패 (기존 데이터 유지): {e}")

    # BigQuery 은행잔고
    bank_data = None
    if bq_creds:
        try:
            bank_data = fetch_bank_data(bq_creds)
        except Exception as e:
            log.warning(f"BigQuery 실패 (기존 bank 데이터 유지): {e}")

    # 데이터 병합
    if not sheets_daily and not bank_data:
        log.info("새 데이터 없음 — meta만 업데이트")

    merged = merge_cash_data(existing, sheets_daily, bank_data, month_key, today, sheets_targets, gmv_breakdown, pg_settlement, daily_mur)

    # JS 렌더링
    new_js = render_cash_data_js(merged)

    if args.dry_run:
        log.info("=== DRY RUN — 생성된 JS ===")
        print(new_js)
        return

    # HTML 업데이트
    new_html = update_html(html_content, new_js)
    HTML_PATH.write_text(new_html, encoding='utf-8')
    log.info(f"HTML 업데이트 완료: {HTML_PATH}")
    log.info(f"업데이트 시각: {today.strftime('%Y-%m-%d %H:%M')} KST")


if __name__ == '__main__':
    main()

# 컴퓨트 노드 RocksDB 통합 - 상세 구현 가이드

## 현재 상황

- **스토리지 노드**: BloomCheck gRPC 서버(`bloomd`)가 실행 중 (10.1.1.1:50051)
- **컴퓨트 노드**: RocksDB 코드를 수정하여 자동으로 BloomCheck 서비스를 호출하도록 구현 필요
- **목표**: RocksDB의 `Get` 연산 시, SST 파일을 읽기 전에 스토리지 노드의 Bloom filter를 먼저 체크하여 불필요한 네트워크 I/O를 방지

## 구현 전략

### 통합 지점: `db/db_impl/db_impl.cc`의 `DBImpl::GetImpl()` 메서드

**현재 코드 흐름:**
```
DBImpl::Get() 
  → DBImpl::GetImpl()
    → MemTable에서 검색
    → Immutable MemTable에서 검색
    → Version::Get() → SST 파일에서 검색  ← 여기 전에 BloomCheck 호출
```

**수정 방향:**
1. `Version::Get()` 호출 전에 관련 SST 파일들의 경로를 수집
2. 각 SST 파일에 대해 BloomCheck 서비스 호출
3. `may_match=false`인 SST 파일은 스킵 (하지만 실제로는 Version::Get 내부에서 필터링되므로 옵션으로 구현)

**주의**: SST 파일 경로를 얻는 것이 복잡할 수 있으므로, 실제로는 **옵션으로 BloomCheck를 활성화**하고, 활성화된 경우에만 호출하는 방식이 안전합니다.

## 단계별 구현 가이드

### 1단계: 헤더 파일 포함 및 클라이언트 추가

**파일**: `db/db_impl/db_impl.cc`

```cpp
// 파일 상단에 추가
#include "tools/bloomcheck_client_lib.h"
#include <atomic>
#include <memory>
```

**DBImpl 클래스에 멤버 추가** (db_impl.h 또는 db_impl.cc 상단):

```cpp
// DBImpl 클래스 내부에 추가
class DBImpl {
  // ... 기존 코드 ...
  
 private:
  // BloomCheck 서비스 클라이언트 (옵션)
  std::unique_ptr<BloomCheckServiceClient> bloomcheck_client_;
  std::string bloomcheck_server_address_;  // 예: "10.1.1.1:50051"
  std::atomic<bool> bloomcheck_enabled_{false};
  
  // ... 기존 코드 ...
};
```

### 2단계: DBOptions에 BloomCheck 설정 추가

**파일**: `include/rocksdb/options.h` 또는 `options/options_helper.h`

```cpp
// DBOptions 구조체에 추가
struct DBOptions {
  // ... 기존 옵션들 ...
  
  // BloomCheck 서비스 설정
  std::string bloomcheck_server_address = "";  // 비어있으면 비활성화
  bool enable_bloomcheck_service = false;      // 기본값: false
};
```

**또는 간단하게 환경 변수로 설정:**

```cpp
// db/db_impl/db_impl.cc의 생성자에서
DBImpl::DBImpl(...) {
  // ... 기존 초기화 코드 ...
  
  // 환경 변수에서 BloomCheck 서버 주소 읽기
  const char* server_addr = getenv("BLOOMCHECK_SERVER");
  if (server_addr && strlen(server_addr) > 0) {
    bloomcheck_server_address_ = std::string(server_addr);
    bloomcheck_client_ = std::make_unique<BloomCheckServiceClient>(
        bloomcheck_server_address_);
    bloomcheck_enabled_.store(true);
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "BloomCheck service enabled: %s\n",
                   bloomcheck_server_address_.c_str());
  }
}
```

### 3단계: GetImpl에서 BloomCheck 호출

**파일**: `db/db_impl/db_impl.cc`

**위치**: `DBImpl::GetImpl()` 메서드 내, `sv->current->Get()` 호출 전

```cpp
// db/db_impl/db_impl.cc의 GetImpl 메서드 내부
// 약 5712줄 부근, sv->current->Get() 호출 전

if (!cache_only) {
  // Check tables
  PinnedIteratorsManager pinned_iters_mgr;
  
  // ===== BloomCheck 서비스 호출 (추가) =====
  if (bloomcheck_enabled_.load() && bloomcheck_client_) {
    // User key 추출
    Slice user_key = ExtractUserKey(lkey.internal_key());
    std::string user_key_str = user_key.ToString();
    
    // Version에서 관련 SST 파일 경로 수집
    // 참고: Version::GetColumnFamilyMetaData()를 참고하여 구현
    // 하지만 실제로는 Version::Get 내부에서 이미 필터링되므로,
    // 여기서는 옵션으로만 구현하고 실제 필터링은 Version::Get에 위임
    
    // 간단한 구현: 모든 레벨의 SST 파일을 체크 (비효율적일 수 있음)
    // 실제 프로덕션에서는 Version::Get 내부에서 체크하는 것이 더 효율적
    
    // TODO: Version::Get 내부에서 BloomCheck를 호출하도록 수정하는 것이 더 좋음
    // 현재는 옵션으로만 남겨둠
  }
  // ===== BloomCheck 끝 =====
  
  sv->current->Get(read_options, lkey, /*value=*/nullptr, /*columns=*/nullptr,
                   timestamp, &s, &merge_context, &max_covering_tombstone_seq,
                   &pinned_iters_mgr, nullptr /* value_found */,
                   found_record_for_key, seq, nullptr /*read_callback*/,
                   is_blob_index);
  
  // ... 나머지 코드 ...
}
```

**더 나은 방법: Version::Get 내부에서 호출**

실제로는 `db/version_set.cc`의 `Version::Get()` 메서드에서 각 SST 파일을 읽기 전에 BloomCheck를 호출하는 것이 더 효율적입니다. 하지만 이것은 더 복잡한 수정이 필요합니다.

### 4단계: Version::Get에서 BloomCheck 호출 (권장)

**파일**: `db/version_set.cc`

`Version::Get()` 메서드에서 각 레벨의 파일을 순회할 때, 각 SST 파일에 대해 BloomCheck를 호출:

```cpp
// Version::Get() 메서드 내부에서
// 각 레벨의 파일을 순회하는 부분에 추가

// BloomCheck 클라이언트를 어떻게 전달할까?
// 옵션 1: DBImpl에서 Version으로 전달 (함수 파라미터로)
// 옵션 2: ImmutableOptions에 포함
// 옵션 3: ReadOptions에 포함

// 가장 간단한 방법: ReadOptions에 클라이언트 포인터 추가
// 하지만 ReadOptions는 const로 전달되므로, 대신 ImmutableOptions에 추가

// 실제 구현은 복잡하므로, 일단 DBImpl 레벨에서만 구현하는 것을 권장
```

## 간단한 구현 (1단계)

복잡한 통합을 피하고, 먼저 옵션으로만 활성화:

### 1. 환경 변수 기반 활성화

**파일**: `db/db_impl/db_impl.cc`의 생성자

```cpp
// DBImpl 생성자에 추가
const char* bloomcheck_server = getenv("BLOOMCHECK_SERVER");
if (bloomcheck_server && strlen(bloomcheck_server) > 0) {
  bloomcheck_server_address_ = std::string(bloomcheck_server);
  bloomcheck_client_ = std::make_unique<BloomCheckServiceClient>(
      bloomcheck_server_address_);
  bloomcheck_enabled_.store(true);
}
```

### 2. GetImpl에서 로깅만 추가 (테스트용)

```cpp
if (bloomcheck_enabled_.load()) {
  Slice user_key = ExtractUserKey(lkey.internal_key());
  // TODO: 실제 SST 파일 경로 수집 및 BloomCheck 호출
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "BloomCheck: checking key %s\n",
                 user_key.ToString(true).c_str());
}
```

## Makefile 수정

**파일**: `Makefile`

RocksDB 라이브러리를 빌드하는 부분에 gRPC 라이브러리 추가:

```makefile
# 기존 LIBRARY 빌드 규칙 찾기
# 또는 SHARED_LIBRARY 빌드 규칙에 추가

# 예시 (실제 위치는 Makefile 구조에 따라 다를 수 있음):
PLATFORM_LDFLAGS += -lgrpc++ -lprotobuf -lgpr -labsl_synchronization -pthread
```

**더 안전한 방법**: 조건부 링크

```makefile
# Makefile 상단에
BLOOMCHECK_ENABLED ?= 0

ifeq ($(BLOOMCHECK_ENABLED),1)
  PLATFORM_LDFLAGS += -lgrpc++ -lprotobuf -lgpr -labsl_synchronization -pthread
  CXXFLAGS += -DROCKSDB_BLOOMCHECK_ENABLED
endif
```

## 빌드 및 테스트

```bash
# 1. 환경 변수 설정
export BLOOMCHECK_SERVER=10.1.1.1:50051

# 2. 빌드 (BloomCheck 활성화)
make clean
make static_lib BLOOMCHECK_ENABLED=1
# 또는
make static_lib PLATFORM_LDFLAGS="-lgrpc++ -lprotobuf -lgpr -labsl_synchronization -pthread"

# 3. 테스트
# RocksDB를 사용하는 애플리케이션에서 Get 연산 수행
# 로그에서 BloomCheck 호출 확인
```

## 주의사항

1. **성능**: gRPC 호출은 네트워크 지연이 있으므로, 실제로는 배치 요청을 사용하는 것이 좋습니다.
2. **에러 처리**: 네트워크 오류 시 항상 `true`를 반환하여 False Negative를 방지해야 합니다.
3. **SST 파일 경로**: Version에서 SST 파일 경로를 얻는 것은 복잡합니다. `Version::GetColumnFamilyMetaData()`를 참고하세요.
4. **옵션 활성화**: 기본적으로는 비활성화하고, 환경 변수나 옵션으로 활성화하는 것이 안전합니다.

## 다음 단계

1. ✅ 기본 통합 (옵션 활성화, 로깅)
2. ⏳ SST 파일 경로 수집 구현
3. ⏳ 실제 BloomCheck 호출 추가
4. ⏳ 배치 요청으로 최적화
5. ⏳ 성능 측정 및 튜닝


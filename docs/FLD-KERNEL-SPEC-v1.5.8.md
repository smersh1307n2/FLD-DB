# FLD KERNEL SPECIFICATION — v1.5.8-DRAFT
## Consolidated Final Specification

**Статус:** NORMATIVE DESIGN DRAFT (pre-FINAL FREEZE)
**Формат:** FLD1 (magic `46 4C 44 31`, binaryVersion 1)
**Целевой рантайм:** C# 12+ / .NET 10 (FLD1 Core + BinSync.Net integration)
**Совместимость (протокол):** BIN-SYNC CORE **v2.3.6** + addendum/errata **v2.3.7** (E-1…E-13) — без изменения wire-нормы
**Совместимость (пакет):** `BinSync.Core` **≥ 2.6.5** (типизированные `NodeId`/`RecordId`/`Digest16`, outbox compact); SemVer пакета ≠ версия протокола
**Консолидация:** v1.3 → v1.4.1–10 → v1.5.0–8; все errata вшиты в тело; **v1.5.8+compat-2.6.5**
**Правило чтения:** PART I–VI — обязательства; PART VII — ADR без обязательств; PART VIII — приёмка; PART IX — реализационные заметки.
**Конвенция:** диапазоны inclusive; байтовые индексы перечисляются явно.

---

## PART 0. ОБЗОР, ГРАНИЦЫ, ТЕРМИНЫ

### 0.1. Суть
Embedded field-store с multi-process доступом через FLD Shared Engine. Единица хранения — версия поля `(namespaceId, recordId, fieldId) → {value, hlc, authorNodeId, keyEpoch}`. Единица I/O — immutable run-файл FLD1. Единица поиска — PGM-проекция по `recordId`. Единица синхронизации — канонический байт коммита ([E-ONE-ENCODING]).

### 0.2. Архитектурная модель (v1.5+)
```
Application A                    Application B
     │                                │
 Fld.Client.dll                  Fld.Client.dll
     │                                │
     └────────── IPC/SHM ─────────────┘
                   │
            FLD ENGINE PROCESS (один на базу)
                   │
       ┌───────────┼───────────┐
       │           │           │
      WAL       Manifest     Runs
       │           │           │
       └───────────┴───────────┘
                   │
              FLD1 FILES
```

**[E-ENGINE-SINGLETON]** Один Engine на физическую базу; единственный владелец изменяемого состояния (WAL, manifest, L0, compaction). Клиенты читают immutable runs через локальный read-only MMF.

### 0.3. Цели / не-цели
**Цели:** детерминированная сходимость; 1 логическое чтение inline / 2 external; zero-copy hot path; field LWW; delete-wins lifecycle; crash-safety без torn-state; конкурентные писатели через group commit.
**Не-цели:** линеаризуемость; кросс-ns атомарность; SQL в ядре; блобы > 4 MB; реалтайм-текст CRDT; NFS/SMB; time-travel; page compression до ADR-11; per-key TTL; distributed consensus.

### 0.4. Термины
| Термин | Определение |
|---|---|
| Run | Immutable FLD1-файл: один namespace, кластеры полей, PGM, slot-массив |
| Кластер | Все состояния полей одного recordId в одном run |
| Логический ключ | `(NamespaceId, RecordId, FieldId)` |
| Contributor | alive / ghost / tombstone в GC-окне |
| Ghost | поле скрыто чтением (field.hlc ≤ lifecycle.hlc при isDead), но в дайджесте |
| InternId | локальный u32 в run; `0` = нет; digest/tie-break — полные 16 B |
| CertifiedWatermark | HLC, ниже которого tombstone можно физически удалять |
| SafeOutboxSeq | seq-шкала удержания WAL per `(authorNodeId, ns)` |
| DurableWalOffset | байтовый offset WAL, до которого fsync успешен |
| DurableCommitSeq | max CommitSeq в области ≤ DurableWalOffset |
| FinalizedThroughSeq | непрерывная граница окончательно классифицированных RequestSeq |
| HLC | wall (микросекунды UTC) + logical (u32) + authorNodeId (16B) |
| Группа коммита | мутации одного ns, объединённые лидером в одну WAL-запись |
| Commit kind | LOCAL / APPLY |

### 0.5. Файловый набор
```
<dbdir>/
  manifest.fldm                // binary manifest + NsRegistry + CommitResultLog metadata
  .fld_engine.lock             // ownership lock (Engine держит до exit)
  .fld_bootstrap.lock          // bootstrap lock (только на время запуска)
  wal/ns_<id>.<gen>.log        // WAL per namespace, с ротацией
  runs/<runId>.fld1            // immutable runs
  del/<runId>.fld1.deleted     // EBR-deferred unlink
  shm.fldm                     // shared memory segment
  tmp/                         // tmp builders, manifest.tmp
```

### 0.6. Endianness, ключи, UUIDv7
Все многобайтовые целые — **little-endian**, кроме сырых байтовых полей (magic, RecordId, ShortKey, authorNodeId). RecordId — сырые 16 байт UUIDv7; memcmp ключей — **unsigned BE** по байтам.

```
UUIDv7 layout:
  uuid[0..5]  timestamp_ms (48 bit, BE)
  uuid[6]     hi nibble = 0x7 (version), lo nibble = rand_a[11..8]
  uuid[7]     rand_a[7..0]
  uuid[8]     hi 2 bits = 0b10 (variant), rest rand_b
  uuid[9..15] rand_b
K(u) = (ExtractTimestampMs(u) << 12) | ExtractRandA(u)   // 60 bit
```

**[K-UUIDV7]** Ingest/build: неверный version/variant → `INVALID_RECORD_ID`, run не публикуется. Читатель не reject'ит non-v7: корректность держит fallback-поиск.

---

## PART I. ФИЗИЧЕСКИЙ ФОРМАТ FLD1 RUN

### 1.1. HEADER (256 B, 4 cache line)
```
0x00 [4B]  magic = 0x46 0x4C 0x44 0x31 ('F','L','D','1')
0x04 [2B]  binaryVersion = 1
0x06 [2B]  flags: 0x01 HAS_VALUES | 0x02 IS_COMPACTED |
                  0x04 HAS_HLC_OVERRIDES | 0x08 HAS_KEY_EPOCHS
0x08 [4B]  namespaceId          // zero-extend(BIN-SYNC u16)
0x0C [4B]  numClusters
0x10 [4B]  numFields
0x14 [4B]  internCount
0x18 [4B]  pgmSegments
0x1C [4B]  pgmEpsilon           // ≤ 32
0x20 [8B]  internOff            // = 256
0x28 [8B]  pgmOff
0x30 [8B]  slotOff
0x38 [8B]  dataOff
0x40 [8B]  valuesOff            // = footerOff при HAS_VALUES = 0
0x48 [8B]  valuesLen            // с учётом 8B-паддинга; кратна 8
0x50 [8B]  footerOff            // N − 8, кратна 8
0x58 [4B]  numExternalValues
0x5C [4B]  aliveFields
0x60 [4B]  contributorCount     // = aliveFields + ghostCount + retainedTombs
0x64 [4B]  ghostCount
0x68 [16B] summaryXor
0x78 [16B] summarySum
0x88 [32B] rootDigest           // Run StateDigest
0xA8 [88B] reservedPad          // нули
```

**[A1-MAGIC]** memcmp 4 байт против `'FLD1'`; `ReadUInt32LE == const` запрещено.
**[F-NS]** formatVersion 1: `namespaceId ≤ 0xFFFF`.
**[F-COUNTS]** `contributorCount == aliveFields + ghostCount + retainedTombs`.
**[F-DUP-KEY]** В STATE run ≤ 1 физического winner на логический ключ.
**[A6-RESERVED]** Ненулевые reserved-поля → `CORRUPT_RUN`. **Исключение:** FieldHlcOverride байты 2–3 = KeyEpoch.
**[D-FLAGS]**
```
HAS_HLC_OVERRIDES  ≡ popcount(FieldDesc bit1) > 0
HAS_EXTERNAL       ≡ ∃ ValLen = 0xFFFF
HAS_KEY_EPOCHS     ≡ ∃ FieldHlcOverride.KeyEpoch ≠ 0
При HAS_VALUES == 0: valuesOff == footerOff, valuesLen == 0, numExternalValues == 0.
При HAS_VALUES == 1:
  valuesOff + valuesLen == footerOff
  valuesOff % 8 == 0, valuesLen % 8 == 0, footerOff % 8 == 0
```
**[A8-EMPTY]** Пустой run: файл 264 B; `HAS_VALUES = 0`; `rootDigest = BLAKE3-256("FLD1-RUNSTATE-v1" ‖ ns u32 LE)`. Get → miss, не CORRUPT_RUN.

### 1.2. INTERN TABLE
Плотный массив 16-байтовых `AuthorNodeId`, unsigned memcmp 16B, без дубликатов. `internId = index + 1`; `0` = нет.

### 1.3. НОРМАЛИЗОВАННЫЙ PGM-ИНДЕКС
Ключ `K = (ms << 12) | rand_a` (60 бит).
**[K-PGM-MONOTONIC]** A ≤ B ⇒ K(A) ≤ K(B).
**[C-ORDER-PRIMARY]** Первичный порядок — полный RecordId (unsigned BE). K — только проекция для PGM.

**Segment [32B]:** `StartKey[8] | StartIdx[4] | SegEps[2] | Reserved[2]=0 | Num[8] | Den[8]`
```
Predict(s, k): k <= s.StartKey → s.StartIdx
               p = ((UInt128)(k - s.StartKey) * s.Num) / s.Den
               r = StartIdx + p; clamp to ulong
Window(s, k, n, defaultEps): eps = SegEps ?: defaultEps
               [max(0, pred - eps), min(n - 1, pred + eps)]
```

**[K-PGM-DUP-RANGE]** Билдер поднимает SegEps до `min(ceil((G−1)/2), 65535)` для групп G.
**[A9-PGM-TRUTH]** Slot-массив — source of truth; PGM — fast path. Miss → lower_bound по полному RecordId.

### 1.4. COMPACT SLOT ARRAY
`CompactSlot [16B]`: `DataOffset[8] | ShortKey[4] | Pad[4]=0`.
**[A2-SHORTKEY]** `ShortKey` = `RecordId[12..15]`. Только SIMD prefilter внутри PGM-окна.
**[K-SLOT-VERIFY]** Полная 128-bit проверка RecordId в Cluster Header обязательна.

### 1.5. DATA SECTION (кластеры)
```
+0        [40B] Cluster Header:
                  RecordId[16] | NumFields[2] | Flags[2] |
                  BaseWall[8]  | BaseLogical[4] | BaseInternId[4] |
                  Reserved[4]=0
+40       [8B × N]      FieldDesc[] (FieldId ASC)
+40+8N    [24B × M]     FieldHlcOverride[] (FieldIdx ASC)
+40+8N+24M              payloadArea (inline, без дыр, FieldId ASC)
...       pad до 8B
```

**Cluster Header.Flags:** `bit0 DEAD`, `bit1 HAS_EXTERNAL`, остальное reserved=0.
**[C-LIFECYCLE-CACHE]** `DEAD ⇔ survivor поля 0x0000 = 0x01`.
**[A7-IMPLICIT]** Отсутствие 0x0000 = implicit alive.
**[F-EMPTY-CLUSTER]** `NumFields == 0 ⇒ DEAD == 0`.
**[A7-LC-FIELD]** Поле 0x0000: FieldWrite с value ∈ {0x00, 0x01}, 1 байт.
**[C-BASE-MODE]** `(BaseWall, BaseLogical, BaseInternId)` = мода эффективных троек всех FieldDesc кластера.

**FieldDesc [8B]:** `FieldId[2] | ValLen[2] | Flags[1] | Reserved[1]=0 | Offset[2]`
**[A4-THRESHOLDS]**
- placement policy ∈ [0..256], **дефолт 256**
- inline: `ValLen ∈ [0..256]` = длина
- external: `ValLen = 0xFFFF`; `Offset` = индекс ValueRef
- `ValLen ∈ [257..0xFFFE]` в файле → `CORRUPT_RUN`
- ingest: `len > 4 MB` → reject; `257 B … 4 MB` → external

**FieldDesc.Flags:** `bit0 TOMBSTONE`, `bit1 HAS_HLC_OVERRIDE`, остальное reserved=0.
**[K-CLUSTER-LIMIT]** payloadArea ≤ 65535 B.
**[A3-SPILL]** Spill-очередь = sort(ValLen DESC, FieldId ASC).

**FieldHlcOverride [24B]:** `FieldIdx[2] | KeyEpoch[2] | Wall[8] | Logical[4] | InternId[4] | Pad[4]=0`
**[KEYEPOCH-SLOT]** Байты 2–3 = KeyEpoch u16 LE (диапазон 0..65535).
**[KEYEPOCH-DEFAULT]** Нет override ⇒ keyEpoch = 0.
**[KEYEPOCH-OVERRIDE-REQ]** keyEpoch ≠ 0 ⇒ FieldDesc bit1 = 1.
**[KEYEPOCH-FLAG]** `flags.HAS_KEY_EPOCHS ⇔ ∃ override с KeyEpoch ≠ 0`.
**[KEYEPOCH-WIRE]** Сериализация в digest: строго u16 LE.
**[KEYEPOCH-E2EE]** В E2EE ciphertext-значение обязано иметь KeyEpoch, согласованный с KEY_ROTATION.
**[KEYEPOCH-ZERO-SEMANTICS]** `keyEpoch = 0` — допустимое начальное поколение.

### 1.6. VALUES SECTION
**ValueRef-каталог:** массив 16B, сортировка `(RecordId, FieldId)`: `DataOffset[8] | Length[4] | Crc32C[4]`.
**Blob Data Pool:** сырые payload'ы, непрерывно.

**Упаковка + паддинг:**
- `DataOffset[i+1] = DataOffset[i] + Length[i]`
- В конце Values — нулевой паддинг (0..7 B) до 8-байтовой границы footer.

**[K-BLOB-CRC]** Crc32C при первом чтении; `Length ≤ 4 MB`.
**[K-VALUES-ALIGN]** `footerOff % 8 == 0`.

### 1.7. FOOTER и порядок сборки
`xxHash64(bytes[0..footerOff), seed=0)` [8B].
**[F-FOOTER]** mismatch → `CORRUPT_RUN`.
**[F-FILE-ALIGN]** `N = footerOff + 8`, `N % 8 == 0`.
**[F-FOOTER-ORDER]**
1. тело (header + intern + pgm + slots + data + values без паддинга)
2. нулевой паддинг до 8B
3. вычислить `summaryXor/Sum/rootDigest` из содержимого
4. вписать в header (digest'ы, valuesLen с паддингом, footerOff)
5. `xxHash64(bytes[0..footerOff))` → footer

---

## PART II. КАНОН СОРТИРОВКИ И ВОСПРОИЗВОДИМОСТИ

1. Кластеры: RecordId ASC (BE memcmp 16B)
2. Поля: FieldId ASC, без дубликатов
3. FieldHlcOverride: FieldIdx ASC
4. Intern Table: AuthorNodeId ASC
5. ValueRef: (RecordId, FieldId) ASC
6. PGM: детерминированный greedy с неделимостью dup-групп (§2.1)
7. payloadArea: без дыр, FieldId ASC
8. Spill: ValLen DESC, FieldId ASC

**[C-DETERMINISM]** Два билдера на одинаковом входе → байт-идентичный run.
**[C-NO-FLOAT]** Float/double запрещены в канонических путях.
**[TAG-ASCII]** Теги — сырые ASCII без NUL/pad: `FLD1-RUNSTATE-v1` (16B), `FLD1-DBSTATE-v1` (15B), `FLD1-COMP-v1` (12B).

### 2.1. PGM build: неделимость dup-групп
Greedy по непрерывным группам равных K; сегмент либо покрывает группу целиком, либо не касается. Post-pass SegEps по [K-PGM-DUP-RANGE]. Граница сегмента внутри группы запрещена (**PGM-DUP-SPLIT-001**).

---

## PART III–X
См. исходный нормативный документ v1.5.8-DRAFT (Shared Engine, WAL v2, Manifest, BIN-SYNC contracts, ADR, gates).

Полный текст PART III (FLD Shared Engine), PART IV (BIN-SYNC), PART V (Micro-WAL), PART VI (Manifest), PART VII (ADR), PART VIII (Gates), PART IX (Implementation notes), PART X (Changelog) сохранён в истории проектирования и должен быть восстановлен/расширен при необходимости реализации Engine/WAL.

**Критерии FINAL FREEZE:**
1. WAL durability proof
2. EBR memory ordering proof
3. SPSC fencing proof
4. BIN-SYNC integration proof

**Сигнал к реализации Потока A:** `Fld1.Core` + `Fld1.Run` + `Fld1.Wal` + `Fld1.Manifest` против v1.5.8.


---

## PART III. FLD SHARED ENGINE (summary)

**[E-ENGINE-SINGLETON]** Один Engine на физическую базу.
**[E-ENGINE-PRINCIPLE-2]** Engine — владелец записи, не единственный читатель.
**[E-IMMUTABLE-READ]** Чтение runs через локальный read-only MMF.
**[E-ENGINE-GROUP]** Group commit в Engine; клиенты не пишут WAL сами.

### Bootstrap
Normalize path → bootstrap lock → check active Engine → launch Host → Handshake → SESSION_REGISTER → SHM + EBR slot → release bootstrap lock.

### SHM Header [256 B]
Magic `'FLDS'`, ShmVersion, AbiTag, PubSeq, PublishedL0Generation, CurrentEpoch, RunCount, EbrSlotCount, SessionSlotCount, offsets, EngineInstanceNonce, EnginePid, Coordinator lease fields.

### L0
Immutable generations; L0Entry 64B; AuthorRef → L0AuthorTable; publication via seqlock; session overlay for read-your-writes.

### SPSC Request Queues
Vyukov absolute tickets; RequestSlot 96B; SessionGeneration fencing; abandon on process death.

### EBR
FREE→CLAIMING→ACTIVE→FREE; pin-then-deref; RetireEpoch without −1; reclaim iff RetireEpoch < MinActiveEpoch; CLAIMING → conservative min=0.

---

## PART IV. BIN-SYNC / CRDT

- LWW: (Wall, Logical, AuthorNodeId full 16B)
- Lifecycle field 0x0000; ghost/tombstone visibility per BIN-SYNC §7.4
- Two digest canons: FldFieldDigestPayload (with stateTag) vs BinSyncFieldDigestPayload (wire)
- Five digest roles: Delta, Run State, DB Root, Composition, Wire State
- CertifiedWatermark vs SafeOutboxSeq dual scales
- Outbox API: ReadOutboxBatch, AcknowledgeOutbox, GetOutboxWatermark + lease fencing

---

## PART V. MICRO-WAL (v2)

### Header [64 B]
Magic raw `'FLDW'`, WALVersion=2, NamespaceId, FirstCommitSeq, HeaderCrc32C.

### Record
Envelope 20B (CommitSeq, Length, Kind, Flags, EnvelopeCrc) + Payload + RecordCrc + pad to 8B.
Kinds: 0x01 GROUP_COMMIT, 0x02 NS_CREATE, 0x03 SNAPSHOT_INSTALL.
Serialized append under mutex; PublishedWalEnd monotonic; fsync capture BEFORE durable offset update.
Recovery: EnvelopeCrc first; full CRC-invalid never auto-truncate; order check on CommitSeq.

### Rotation
fsync old WAL → new WAL tmp → header fsync → rename → dir fsync → manifest atomic swap → switch writer.

---

## PART VI. MANIFEST

Generation, Crc32C, LastFlushedCommitSeq, CertifiedWatermark, Ns[], RunEntry[], Retain[], ResultClients (FinalizedThroughSeq contiguous, AcceptedBitmap, ProvenanceIndex), CoordinatorLeaseEpoch.
Atomic: temp → fsync → rename → dir fsync.

---

## PART VII. ADR (non-binding)

ADR-10 Commit-demon replaced by FLD Shared Engine. ADR-11 codecs post-I3. Others: maplet, secondary indexes, SQLite vtab, watchers, NativeAOT — post-baseline.

---

## PART VIII. GATES

I1: builder/reader 10⁵ put/get, empty run 264B, RUN-VERIFY-001, alignment.
I2: kill -9 recovery, 0 GC alloc reads, ≥500k ops/s group commit.
I3: STATE_DIGEST wire, WAL full-author, lifecycle digest 34B.
Engine: START/SESSION/CRASH/DRAIN races, EBR, SPSC fencing.
Perf: LowLatency (30k ops/s) and Throughput (500k ops/s) profiles.

---

## PART IX. IMPLEMENTATION NOTES

Modules: Fld1.Core, Fld1.Run, Fld1.Wal, Fld1.Manifest, Fld1.Engine, Fld1.Client.
SHM: Linux/Windows/macOS 64-bit lock-free atomics; .NET AcquirePointer not DangerousGetHandle.
CRC32C from System.IO.Hashing; BLAKE3 pluggable; 8B alignment valuesLen/footerOff.

---

## PART X. CHANGELOG (v1.5.8)

v1.5.8-DRAFT: EBR RetireEpoch без −1 + CLAIMING→min_epoch=0; WAL serialized append; RequestSlot SessionGeneration u64; FinalizedThroughSeq contiguous; WAL rotation fsync-first; SPSC single Consumer + exclusive lock.


---

## PART XI. COMPAT с BinSync.Core 2.6.5 (2026-11-04)

### Разделение версий
| Слой | Версия | Смысл |
|------|--------|--------|
| Протокол / спека | CORE **v2.3.6** + errata **v2.3.7** | Wire, HLC µs, LWW, digests, E-1…E-13 |
| NuGet `BinSync.Core` | **2.6.5** | API surface, perf, typed ids, outbox compact |

Пакет 2.3→2.6 **не** поднимал нормативную версию протокола в `BIN-SYNC-SPECIFICATION.txt`.

### Что не ломает FLD storage
- HLC wall = **микросекунды** UTC (совпадает)
- Durable author = bare **16 B** `nodeId` (E-1)
- Wire digests на стороне BIN-SYNC: **Digest16** (BLAKE3 truncated); FLD `rootDigest` run = **BLAKE3-256** (локальный Run StateDigest) — разные роли, см. PART IV
- RecordId / NodeId на wire — 16 B; FLD RecordId sort = **unsigned BE** (UUIDv7/PGM)

### Обязательное выравнивание LWW author
**[A-CMP-AUTHOR-NODEID]** Сравнение `authorNodeId` для LWW / HLC tie-break / intern ASC авторов =
порядок **BinSync.NodeId**: сначала `Lo = UInt64LE(bytes[0..7])`, затем `Hi = UInt64LE(bytes[8..15])`.
**Не** использовать BE memcmp для author (он остаётся только для RecordId).

### Outbox (интеграция Engine)
- At-least-once + wire-dedup по ChangeId
- **CompactOutbox** (2.6.5): после ACK можно удалять записи, acked всеми interested peers; peers без PeerState удерживают entry (relay-safe)
- Self-echo: self-authored уже в storage не re-queue ([E-ECHO] / E-13)
- FLD `SafeOutboxSeq` / `AcknowledgeOutbox` должны сохранять **OutboxSeqRef** continuity после restart (CursorEnd = gap-norm для новых peers)

### Public API BinSync (2.2+)
`Storage` / `Outbox` maps **internal** на `Node`; хост ходит через engine API / Outbox API FLD, не мутирует коллекции напрямую.

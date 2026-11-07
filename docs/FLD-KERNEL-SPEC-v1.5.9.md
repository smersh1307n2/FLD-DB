# FLD KERNEL SPECIFICATION — v1.5.9-DRAFT
## Consolidated Final Specification

**Статус:** NORMATIVE DESIGN DRAFT (pre-FINAL FREEZE)
**Формат:** FLD1 (magic `46 4C 44 31`, binaryVersion 1)
**Целевой рантайм:** C# 12+ / .NET 10 (FLD1 Core + BinSync.Net integration)
**Совместимость (протокол):** BIN-SYNC CORE **v2.4.17 FINAL** (включая errata v2.3.7, v2.3.8, v2.4.0, v2.4.4–v2.4.8 и v2.4.17) — без изменения формата FLD1 Run/WAL
**Совместимость (пакет):** `BinSync.Core` **3.0.0**; SemVer пакета ≠ версия протокола. Нормативная совместимость wire-протокола определяется BIN-SYNC CORE v2.4.17 FINAL, а не номером NuGet-пакета.
**Консолидация:** v1.3 → v1.4.1–10 → v1.5.0–9; совместимость с runtime API `BinSync.Core` 3.0.0; wire baseline — BIN-SYNC CORE v2.4.17 FINAL.
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
  local-author.flda            // stable 16-byte LocalAuthor identity + CRC
  .fld_engine.lock             // ownership lock (Engine держит до exit)
  .fld_bootstrap.lock          // bootstrap lock (только на время запуска)
  wal/ns_<id>.<gen>.log        // WAL per namespace, с ротацией
  outbox/ns_<id>.obx           // local-author Outbox
  outbox/ns_<id>.obx.ack       // safe local-Outbox ACK watermark
  outbox/ns_<id>.obx.state     // WAL materialization watermark
  outbox/ns_<id>.obx.lease     // fenced local-Outbox consumer owner + epoch
  outbox/ns_<id>.apply         // durable APPLY receipts + opaque protocol delta + relay mutations
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

## PART IV. BIN-SYNC / CRDT (v2.4.17)

When FLD-DB stores synchronized user data, it implements the durable field-store side of BIN-SYNC CORE v2.4.17 FINAL. This PART IV defines that synchronized-database role only. A separate use case — FLD-DB as a generic Metadata Store for an external Data Provider — is defined by the BIN-SYNC Metadata Storage and External Data Provider Architecture (https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/METADATA-STORAGE-ARCHITECTURE.md). The related pending-change lifecycle and safe LWW coalescing policy are defined in the BIN-SYNC Change Staging, Coalescing, and Metadata Retention decision (https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/CHANGE-STAGING-AND-RETENTION.md). That use case requires an explicit Storage-only role and MUST NOT be implemented by routing metadata writes through ordinary synchronized FldDatabase semantics. FLD-DB MUST NOT define an independent wire protocol or reinterpret BIN-SYNC cursors, identities, merge rules, or digest domains.

### 4.1. Canonical identity and authorship
- `NodeId`, `RecordId`, `ChangeId`, `BatchId` are exactly 16 bytes at the durable/wire boundary. BIN-SYNC public API normalization does not authorize FLD to hash, truncate, or regenerate an already accepted identifier.
- `authorNodeId` is the durable 16-byte identity of the original change author, never the current relay node or session alias.
- Session `origin` aliases are resolved by BIN-SYNC before storage. FLD persists only the resolved durable author.
- `LocalAuthor` is persisted once per physical FLD database and remains stable across reopen. When migrating a non-empty legacy database without persisted author identity, opening requires an explicit original `LocalAuthor`; a conflicting configured author fails closed. In external-storage mode it MUST equal `BinSync.NodeId`.
- Author tie-break is unsigned `UInt64LE(bytes[0..7])`, then unsigned `UInt64LE(bytes[8..15])`, exactly as BIN-SYNC §5.4. RecordId ordering remains unsigned BE; these orders MUST NOT be conflated.

### 4.2. HLC and LWW
- HLC wall is unsigned UTC microseconds; logical is u32. FLD does not narrow wall to Int64 or use HLC as a replication cursor.
- Field winner order is (wall, logical, durable authorNodeId) using BIN-SYNC §5.4. FieldWrite and FieldTomb are compared by the same total order; a tombstone wins only on an exact full-key tie where the protocol specifies it.
- Lifecycle field 0x0000 follows BIN-SYNC §7.4: lifecycle author is persisted separately; dead wins only on an exact lifecycle-key tie; ghost visibility is derived using field author versus lifecycle author.
- FieldTomb is a distinct operation kind from an empty FieldWrite. Digest and relay logic MUST preserve this distinction.
- Remote HLC values are not used to advance the local HLC generator as if they were locally-authored writes. Receive-side HLC validation/skew policy remains a BIN-SYNC responsibility.

### 4.3. Change identity and relay provenance
- Every accepted DELTA_BATCH record has a nonzero, unique 16-byte ChangeId. It is immutable across retries, snapshots where applicable, and every relay hop.
- FLD MUST reject missing, zero, malformed, or conflicting ChangeId values. It MUST NOT silently generate a replacement ChangeId for remote operations.
- Relay eligibility is based on provenance, not whether an incoming operation wins LWW. A new remote operation that loses LWW may still be required for downstream convergence.
- Exact self-echo and already-seen ChangeId are no-ops for storage/relay. A duplicate batch still receives the original durable receipt/ACK identity.
- Every published local mutation has one stable nonzero ChangeId, preserved through WAL, frozen Outbox, compaction, recovery, retry and relay. The current implementation generates it during Put/PutOwned; the proposed pending-staging design may defer protocol ChangeId allocation until the pending operation is frozen. Superseded unpublished provisional identities may be discarded, but a frozen/published ChangeId MUST never change. See the BIN-SYNC Change Staging, Coalescing, and Metadata Retention decision.

### 4.4. Local Outbox, relay journal, and author streams
- FLD local Outbox contains only operations authored by LocalAuthor. Its OutboxSeq is monotonic within the namespace and maps directly to the BIN-SYNC stream (LocalAuthor, namespaceId).
- Current implementation materializes Outbox entries at each durable Commit; multiple writes to one field before the same Commit are collapsed by the dirty overlay, but separate commits produce separate Outbox entries. The proposed optimization is a distinct durable pending-local staging area: coalesce only unpublished LWW operations by exact field key, then atomically freeze them into the append-only Outbox before any transport can expose them. This staging/freeze optimization is a design target, not implemented behavior. The Outbox itself MUST NOT be rewritten to coalesce unacknowledged entries.
- FLD does not assign sequence numbers to remote relay operations. RelayJournal stores immutable original mutation data and provenance; BIN-SYNC owns relay policy and assigns/restores per-author relay sequence.
- FLD OutboxSeq is an immutable local durable cursor. BIN-SYNC peer-relative StreamOffset/wire cursor mapping belongs to BIN-SYNC PeerState and MUST NOT renumber or mutate FLD Outbox entries.
- WAL CommitSeq, local OutboxSeq, HLC wall, relay sequence, and peer wire cursor are distinct domains and MUST NOT be substituted for one another.
- FLD does not aggregate author streams into a namespace-wide scalar cursor. StateRootDigest excludes Outbox cursor and all ACK/peer progress.
- Safe local-Outbox and relay-retention boundaries are computed by BIN-SYNC from interested-peer ACK state and supplied to FLD. Missing peer state is not an ACK and MUST hold the relevant prefix. Acknowledgement/compaction MUST be fenced against stale consumers.
- Compaction may remove only entries at or below the proven safe boundary; it MUST preserve local Outbox sequence continuity and all ChangeId/provenance fields of retained Outbox and relay entries.

### 4.5. Inbox, batch atomicity, and durable receipt
- Inbox idempotency key is (issuerNodeId, namespaceId, issuerEpoch, batchId). The durable receipt also preserves the author stream identity needed to replay the correct ACK after reconnect/restart.
- Batch chain identity is scoped exactly as BIN-SYNC §12.17.2; target-peer scope MUST NOT be dropped where required by the protocol.
- Applying a DELTA_BATCH is atomic: field state, dedup/inbox receipt, accepted cursor/digest-chain state, and relay-eligible mutations become durable together before ACK is emitted.
- A repeated batch returns the original receipt and ACK semantics; it MUST NOT reapply mutations or enqueue duplicate relay work.
- Snapshot chunks use the same LWW primitives, preserve field tombstones and lifecycle state, and do not create local-author Outbox entries. Snapshot installation and tail boundary must refer to one consistent state view.

**Current storage amplification and target decomposition:**

- Current ApplyCommitCodec v2 serializes Fields and RelayFields separately even though RelayFields is validated as an exact subset of Fields by ChangeId and byte-identical operation data. Relay payload bytes are therefore duplicated inside one APPLY_COMMIT.
- FldDatabase then appends the same full encoded APPLY_COMMIT payload to both WAL and ApplyJournal. ApplyJournal retains the full original Fields array even though relay reads only need RelayFields and duplicate-batch handling needs a compact receipt.
- The inspected ApplyJournal has no public ACK/compaction/truncation boundary. It currently couples duplicate-receipt retention to relay-payload retention and can grow independently of WAL checkpointing.
- Target: encode each incoming operation once in WAL and represent relay selection with compact indexes/bitmap; persist a compact durable receipt separately from a relay-only journal; materialize both sidecars idempotently from the authoritative WAL CommitSeq; truncate WAL only after sidecars are durable.
- Receipt GC follows Inbox expiry plus confirmed-log-segment semantics. Relay payload GC follows BIN-SYNC safe downstream ACK. These are independent lifetimes. Do not delete the receipt when relay completes, or relay payload when the receipt expires.
- This is an internal storage-format/retention redesign, not a wire-format change. The detailed analysis and required crash tests are in the BIN-SYNC Change Staging, Coalescing, and Metadata Retention decision.

### 4.6. Digest domains
- BIN-SYNC RecordDigest and StateRootDigest follow v2 domains/formulas (§9.1), including FieldWrite-vs-FieldTomb kind and lifecycle author. Outbox cursor/ACK progress is excluded from StateRootDigest.
- FLD Run StateDigest, DB RootDigest, composition digest, and physical file checksum are private FLD integrity domains. They MUST NOT be substituted for BIN-SYNC RecordDigest, BucketDigest, or StateRootDigest.
- E2EE values are stored and digested as received ciphertext; FLD MUST NOT decrypt/re-encrypt or normalize ciphertext bytes. KeyEpoch is preserved exactly.
- Canonical CRDT payloads outside the FLD LWW field model are either preserved as opaque canonical bytes or rejected as unsupported. FLD MUST NOT silently coerce COUNTER_G, COUNTER_PN, SET_OR, SEQ_FRACTIONAL, or BLOB_REF into LWW_FIELD.

### 4.7. Schema and protocol validation
- BIN-SYNC validates aliases, schema descriptors, opKind/payload tag, RecordTombstone envelope identity, ChangeId, batch digest chain, and canonical payloads before durable apply.
- FLD validates storage invariants and atomically persists only a fully validated apply request. It does not reinterpret session aliases or silently skip malformed protocol metadata.
- Unsupported merge policies fail explicitly; they MUST NOT be treated as LWW_FIELD by fallback.


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

## PART XI. COMPATIBILITY WITH BIN-SYNC CORE v2.4.17 FINAL

**Normative baseline:** `BIN-SYNC-SPECIFICATION.txt`, version **2.4.17 FINAL**, dated 2026-11-06. The storage boundary additionally follows the project contract [`FLD-DB-INTEGRATION-CONTRACT.md`](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/FLD-DB-INTEGRATION-CONTRACT.md); FLD stores its protocol extension opaquely and does not own BIN-SYNC semantics. This supersedes the former v2.3.6 + v2.3.7 compatibility claim. The current baseline includes ChangeId provenance (v2.3.8), immutable per-peer cursor offsets and ACK bounds (v2.4.0), bidirectional snapshot baseline (v2.4.4), snapshot resource limits (v2.4.5), StateRoot v2 without Outbox cursor (v2.4.6), canonical STATE_DIGEST (v2.4.7), NEED_RANGE/NEED_KEYS validation (v2.4.8), and canonical CRDT/SCHEMA_MAP validation (v2.4.17).

### 11.1. Version layers
| Layer | Version | Contract |
|---|---|---|
| Wire protocol | BIN-SYNC CORE **2.4.17 FINAL** | Identity, HLC, LWW/lifecycle, ChangeId, batch chain, digests, snapshots, ACK, CRDT canonicalization |
| .NET package API | `BinSync.Core` **3.0.0** | Current public API/runtime profile; package SemVer is not a substitute for protocol version |
| FLD physical format | FLD1 binaryVersion **1** | Unchanged by this compatibility alignment |

### 11.2. Explicit cursor mapping
- Local FLD OutboxSeq maps only to the BIN-SYNC stream `(LocalAuthor, namespaceId)`.
- FLD OutboxSeq remains immutable and local. Peer-relative StreamOffset and wire cursor are owned by BIN-SYNC per `(peer, namespaceId, authorNodeId)`.
- `CommitSeq` is WAL ordering only. It is never a sync cursor.
- There is no namespace-wide aggregate Outbox cursor in StateRootDigest. Legacy STATE_DIGEST OutboxCursor is zero on send and ignored on receive.
- Missing PeerState means unacknowledged; compaction cannot infer ACK from absence.
- ACK is validated against the exact peer/author stream pushed range before retention state can advance.

### 11.3. Durable change identity
- ChangeId is mandatory, nonzero, exactly 16 bytes, and immutable from local creation through relay.
- Remote input with absent/invalid ChangeId is rejected. FLD MUST NOT synthesize a new ChangeId.
- BIN-SYNC owns the semantics of Inbox, receive cursors, digest-chain state, SeenChanges, and relay policy. It prepares an opaque ProtocolStateDelta for the storage transaction; FLD stores that extension without interpreting it. FLD atomically commits field winners, ChangeId/provenance state, relay journal entries, and ProtocolStateDelta before returning a durable receipt. A duplicate batch returns its original durable receipt; BIN-SYNC remains responsible for interpreting it and producing the protocol ACK.
- LWW loser retention is separate from field winner publication: provenance-valid new operations remain relay-eligible even when they do not replace the current field winner, except exact self-echo/already-seen operations.

### 11.4. Local and remote commit kinds
- LOCAL commit creates local author Outbox entries only.
- APPLY commit stores remote field winners, ChangeId/provenance state, relay-eligible operations, and the opaque BIN-SYNC ProtocolStateDelta in one atomic durable transaction. It MUST NOT reclassify remote mutations as locally-authored writes.
- Snapshot installation uses APPLY semantics and never generates local Outbox entries.
- BIN-SYNC constructs and interprets ProtocolStateDelta (including Inbox/receive cursor/digest-chain/SeenChanges changes); FLD persists the bytes atomically with field state and relay journal, without interpreting protocol semantics.
- Successful ApplyRemoteBatch return means the complete transaction is durable and may be used by BIN-SYNC to complete receive processing and ACK. On failure, none of the transaction's state may be partially published.

### 11.5. Digest compatibility
- BIN-SYNC uses `BIN-SYNC/RECORD/v2` and `BIN-SYNC/STATE/v2` domains. StateRoot is derived only from replicated namespace state.
- FLD physical Run/DB/composition digests are independent integrity mechanisms. No digest is reused across domains merely because the byte width happens to match.
- Field tombstone kind and lifecycle author are part of the BIN-SYNC digest semantics.
- E2EE ciphertext and KeyEpoch are preserved byte-for-byte.

### 11.6. Schema and merge-policy compatibility
- FLD's native field LWW model supports LWW_FIELD and the lifecycle semantics explicitly defined in §§4.2 and 7.4.
- Other BIN-SYNC merge policies are not silently coerced to LWW_FIELD. They require opaque canonical preservation or explicit rejection until a matching FLD storage model exists.
- Canonical payload validation and SCHEMA_MAP strictness follow BIN-SYNC §§8 and 18.4; malformed, duplicate, unsorted, or ambiguous descriptors are rejected before commit.

### 11.7. Implementation conformance gates
The following gates are tracked independently; partial implementation is not full BIN-SYNC conformance:

| Gate | Current implementation state |
|---|---|
| LOCAL/APPLY WAL separation; remote writes never enter local Outbox | **Implemented.** KindApplyCommit is separate from KindGroupCommit; recovery dispatches by kind. Only local group commits materialize to .obx. |
| Mandatory immutable ChangeId; durable batch dedup | **Partial.** Remote ChangeId is required, nonzero and immutable. Apply Journal deduplicates the durable inbox key (IssuerNodeId, NamespaceId, IssuerEpoch, BatchId) across restart; global SeenChanges/ChangeId dedup remains BIN-SYNC-owned. |
| Relay retention for LWW losers; self-echo suppression | **Partial.** RelayWrites is an explicit BIN-SYNC-prepared subset, independent of LWW winners; LWW losers are retained. BIN-SYNC must exclude already-seen/self-echo operations before constructing the subset. Relay ACK/compaction is not implemented yet. |
| Atomic ProtocolStateDelta + field state + relay data before ACK | **Partial.** ApplyRemoteBatch writes APPLY_COMMIT to WAL, flushes, materializes the complete payload in outbox/ns_<id>.apply, then publishes field winners and returns a durable receipt. Recovery rematerializes a missing journal record from WAL. Full protocol-state GC/retention remains open. |
| Local Outbox cursor; relay author streams; peer offsets; bounded ACK/fenced retention | **Partial.** Local Outbox remains LocalAuthor-only and has a fenced lease. Relay journal paging uses an internal JournalSeq (not a sync cursor); relay lease, ACK watermark and safe compaction remain open. |
| Snapshot consistency and snapshot-tail binding | **Not implemented.** |
| BIN-SYNC v2 RecordDigest/StateRoot compatibility tests | **Not implemented.** |
| Strict schema and unsupported merge-policy behavior | **Not implemented.** |
| Crash/reopen, lost ACK, duplicate batch, relay mesh, compaction and concurrent snapshot tests | **Partial.** APPLY WAL recovery, journal rematerialization, duplicate batch receipt, LWW-loser retention and local-Outbox isolation have tests; mesh, relay ACK/compaction and snapshot concurrency remain open. |

**Current status:** LOCAL/APPLY separation and the first durable remote-apply path are implemented, but FLD-DB does **not** yet claim full BIN-SYNC v2.4.17 conformance. The remaining gates above are still required.

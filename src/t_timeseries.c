/*
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 * Native Redis time series type with Gorilla timestamp encoding.
 *
 * Commands:
 *   TS.SET   key timestamp value [labels]
 *   TS.GET   key timestamp
 *   TS.RANGE key start end aggregation   (aggregation: max|min|sum|avg)
 */

#include "server.h"
#include "t_timeseries.h"
#include <math.h>
#include <float.h>

/* --------------------------------------------------------------------------
 * Gorilla timestamp compression (Facebook Gorilla / VLDB 2015)
 *
 * First timestamp: full 64-bit value.
 * Subsequent: delta-of-delta (DoD) with variable-length prefixes:
 *   DoD == 0                 -> 0
 *   -63  <= DoD <= 64        -> 10  + 7-bit  (biased by +64)
 *   -255 <= DoD <= 256       -> 110 + 9-bit  (biased by +256)
 *   -2047 <= DoD <= 2048     -> 1110 + 12-bit (biased by +2048)
 *   otherwise                -> 1111 + 32-bit signed DoD
 * -------------------------------------------------------------------------- */

void gorillaInit(gorillaTS *g) {
    memset(g, 0, sizeof(*g));
}

void gorillaReset(gorillaTS *g) {
    zfree(g->buf);
    memset(g, 0, sizeof(*g));
}

void gorillaFree(gorillaTS *g) {
    gorillaReset(g);
}

static int gorillaEnsure(gorillaTS *g, size_t extra_bytes) {
    size_t need = g->byte_len + extra_bytes + 8;
    if (need <= g->alloc) return C_OK;
    size_t na = g->alloc ? g->alloc * 2 : 64;
    while (na < need) na *= 2;
    unsigned char *nb = zrealloc(g->buf, na);
    if (!nb) return C_ERR;
    /* Zero new region so partial-byte writes are clean. */
    if (na > g->alloc) memset(nb + g->alloc, 0, na - g->alloc);
    g->buf = nb;
    g->alloc = na;
    return C_OK;
}

/* Write up to 64 bits MSB-first into the bit buffer. */
static int gorillaWriteBits(gorillaTS *g, uint64_t value, int nbits) {
    if (nbits <= 0) return C_OK;
    if (gorillaEnsure(g, (nbits + 7) / 8 + 1) == C_ERR) return C_ERR;

    while (nbits > 0) {
        int space = 8 - g->bit_pos;
        int take = nbits < space ? nbits : space;
        uint64_t shift = (uint64_t)(nbits - take);
        unsigned char bits = (unsigned char)((value >> shift) & ((1u << take) - 1));
        g->buf[g->byte_len] |= (unsigned char)(bits << (space - take));
        g->bit_pos += take;
        nbits -= take;
        if (g->bit_pos == 8) {
            g->bit_pos = 0;
            g->byte_len++;
            if (g->byte_len >= g->alloc) {
                if (gorillaEnsure(g, 8) == C_ERR) return C_ERR;
            }
            /* next byte already zeroed by ensure */
        }
    }
    return C_OK;
}

int gorillaAppend(gorillaTS *g, int64_t timestamp) {
    if (g->count == 0) {
        /* Store first timestamp as full 64-bit signed value. */
        if (gorillaWriteBits(g, (uint64_t)timestamp, 64) == C_ERR) return C_ERR;
        g->prev_ts = timestamp;
        g->prev_delta = 0;
        g->count = 1;
        return C_OK;
    }

    int64_t delta = timestamp - g->prev_ts;
    int64_t dod = delta - g->prev_delta;

    if (dod == 0) {
        if (gorillaWriteBits(g, 0, 1) == C_ERR) return C_ERR;
    } else if (dod >= -63 && dod <= 64) {
        if (gorillaWriteBits(g, 0x2, 2) == C_ERR) return C_ERR; /* 10 */
        if (gorillaWriteBits(g, (uint64_t)(dod + 64), 7) == C_ERR) return C_ERR;
    } else if (dod >= -255 && dod <= 256) {
        if (gorillaWriteBits(g, 0x6, 3) == C_ERR) return C_ERR; /* 110 */
        if (gorillaWriteBits(g, (uint64_t)(dod + 256), 9) == C_ERR) return C_ERR;
    } else if (dod >= -2047 && dod <= 2048) {
        if (gorillaWriteBits(g, 0xE, 4) == C_ERR) return C_ERR; /* 1110 */
        if (gorillaWriteBits(g, (uint64_t)(dod + 2048), 12) == C_ERR) return C_ERR;
    } else {
        if (gorillaWriteBits(g, 0xF, 4) == C_ERR) return C_ERR; /* 1111 */
        if (gorillaWriteBits(g, (uint64_t)(int32_t)dod, 32) == C_ERR) return C_ERR;
    }

    g->prev_delta = delta;
    g->prev_ts = timestamp;
    g->count++;
    return C_OK;
}

size_t gorillaBytes(const gorillaTS *g) {
    return g->byte_len + (g->bit_pos ? 1 : 0);
}

/* Bit reader for decompression. */
typedef struct {
    const unsigned char *buf;
    size_t byte_len;    /* total bytes available (including partial) */
    size_t byte_pos;
    int bit_pos;
    size_t total_bits;
    size_t bits_read;
} gorillaReader;

static void gorillaReaderInit(gorillaReader *r, const gorillaTS *g) {
    r->buf = g->buf;
    r->byte_len = gorillaBytes(g);
    r->byte_pos = 0;
    r->bit_pos = 0;
    r->total_bits = g->byte_len * 8 + (size_t)g->bit_pos;
    r->bits_read = 0;
}

static int gorillaReadBits(gorillaReader *r, int nbits, uint64_t *out) {
    if (nbits <= 0) {
        *out = 0;
        return C_OK;
    }
    if (r->bits_read + (size_t)nbits > r->total_bits) return C_ERR;
    uint64_t val = 0;
    int left = nbits;
    while (left > 0) {
        if (r->byte_pos >= r->byte_len) return C_ERR;
        int avail = 8 - r->bit_pos;
        int take = left < avail ? left : avail;
        unsigned char cur = r->buf[r->byte_pos];
        unsigned char bits = (unsigned char)((cur >> (avail - take)) & ((1u << take) - 1));
        val = (val << take) | bits;
        r->bit_pos += take;
        left -= take;
        r->bits_read += (size_t)take;
        if (r->bit_pos == 8) {
            r->bit_pos = 0;
            r->byte_pos++;
        }
    }
    *out = val;
    return C_OK;
}

int gorillaDecodeAll(const gorillaTS *g, int64_t *out, size_t max_out) {
    if (g->count == 0) return 0;
    if (max_out < g->count) return -1;

    gorillaReader r;
    gorillaReaderInit(&r, g);

    uint64_t first;
    if (gorillaReadBits(&r, 64, &first) == C_ERR) return -1;
    out[0] = (int64_t)first;
    int64_t prev_ts = out[0];
    int64_t prev_delta = 0;

    for (size_t i = 1; i < g->count; i++) {
        uint64_t bit;
        if (gorillaReadBits(&r, 1, &bit) == C_ERR) return -1;
        int64_t dod;
        if (bit == 0) {
            dod = 0;
        } else {
            if (gorillaReadBits(&r, 1, &bit) == C_ERR) return -1;
            if (bit == 0) {
                /* 10 + 7 bits */
                uint64_t v;
                if (gorillaReadBits(&r, 7, &v) == C_ERR) return -1;
                dod = (int64_t)v - 64;
            } else {
                if (gorillaReadBits(&r, 1, &bit) == C_ERR) return -1;
                if (bit == 0) {
                    /* 110 + 9 bits */
                    uint64_t v;
                    if (gorillaReadBits(&r, 9, &v) == C_ERR) return -1;
                    dod = (int64_t)v - 256;
                } else {
                    if (gorillaReadBits(&r, 1, &bit) == C_ERR) return -1;
                    if (bit == 0) {
                        /* 1110 + 12 bits */
                        uint64_t v;
                        if (gorillaReadBits(&r, 12, &v) == C_ERR) return -1;
                        dod = (int64_t)v - 2048;
                    } else {
                        /* 1111 + 32 bits signed */
                        uint64_t v;
                        if (gorillaReadBits(&r, 32, &v) == C_ERR) return -1;
                        dod = (int64_t)(int32_t)v;
                    }
                }
            }
        }
        int64_t delta = prev_delta + dod;
        int64_t ts = prev_ts + delta;
        out[i] = ts;
        prev_delta = delta;
        prev_ts = ts;
    }
    return (int)g->count;
}

/* --------------------------------------------------------------------------
 * Time series object
 * -------------------------------------------------------------------------- */

redisTimeSeries *tsCreate(void) {
    redisTimeSeries *ts = zcalloc(sizeof(*ts));
    gorillaInit(&ts->ts);
    ts->mem_usage = sizeof(*ts);
    return ts;
}

void tsFree(redisTimeSeries *ts) {
    if (!ts) return;
    for (size_t i = 0; i < ts->len; i++) {
        if (ts->samples[i].labels) sdsfree(ts->samples[i].labels);
    }
    zfree(ts->samples);
    gorillaFree(&ts->ts);
    zfree(ts);
}

static void tsRebuildGorilla(redisTimeSeries *ts, const int64_t *timestamps) {
    gorillaReset(&ts->ts);
    for (size_t i = 0; i < ts->len; i++) {
        gorillaAppend(&ts->ts, timestamps[i]);
    }
}

void tsDecodeTimestamps(redisTimeSeries *ts, int64_t *out) {
    if (ts->len == 0) return;
    int n = gorillaDecodeAll(&ts->ts, out, ts->len);
    serverAssert(n == (int)ts->len);
}

size_t tsMemUsage(redisTimeSeries *ts) {
    size_t sz = sizeof(*ts) + ts->alloc * sizeof(tsSample) + ts->ts.alloc;
    for (size_t i = 0; i < ts->len; i++) {
        if (ts->samples[i].labels)
            sz += sdsAllocSize(ts->samples[i].labels);
    }
    ts->mem_usage = sz;
    return sz;
}

size_t tsLen(redisTimeSeries *ts) {
    return ts->len;
}

/* Release backing pages after the fork child has serialized the key (CoW
 * avoidance). Mirrors dismissObject() policy: only bother when the serialized
 * value is large enough that individual allocations may span a page. */
void tsDismiss(redisTimeSeries *ts, size_t size_hint) {
    if (size_hint < (size_t)server.page_size) return;

    if (ts->samples)
        dismissMemory(ts->samples, ts->alloc * sizeof(tsSample));
    if (ts->ts.buf)
        dismissMemory(ts->ts.buf, ts->ts.alloc);

    /* Label strings are usually small; only dismiss ones that look page-sized. */
    for (size_t i = 0; i < ts->len; i++) {
        if (ts->samples[i].labels &&
            sdsAllocSize(ts->samples[i].labels) >= (size_t)server.page_size)
        {
            dismissMemory(ts->samples[i].labels,
                          sdsAllocSize(ts->samples[i].labels));
        }
    }
}

redisTimeSeries *tsDup(redisTimeSeries *src) {
    redisTimeSeries *dst = tsCreate();
    if (src->len == 0) return dst;

    int64_t *timestamps = zmalloc(sizeof(int64_t) * src->len);
    tsDecodeTimestamps(src, timestamps);

    dst->samples = zmalloc(sizeof(tsSample) * src->len);
    dst->alloc = src->len;
    dst->len = src->len;
    for (size_t i = 0; i < src->len; i++) {
        dst->samples[i].value = src->samples[i].value;
        dst->samples[i].labels = src->samples[i].labels ?
            sdsdup(src->samples[i].labels) : NULL;
        gorillaAppend(&dst->ts, timestamps[i]);
    }
    zfree(timestamps);
    tsMemUsage(dst);
    return dst;
}

/* Binary search on decoded timestamps. Returns index or -1. */
static ssize_t tsFindIndex(const int64_t *timestamps, size_t len, int64_t ts) {
    size_t lo = 0, hi = len;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (timestamps[mid] < ts) lo = mid + 1;
        else if (timestamps[mid] > ts) hi = mid;
        else return (ssize_t)mid;
    }
    return -1;
}

/* Lower-bound index for range start. */
static size_t tsLowerBound(const int64_t *timestamps, size_t len, int64_t ts) {
    size_t lo = 0, hi = len;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (timestamps[mid] < ts) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

static int tsEnsureSamples(redisTimeSeries *ts, size_t need) {
    if (need <= ts->alloc) return C_OK;
    size_t na = ts->alloc ? ts->alloc * 2 : 8;
    while (na < need) na *= 2;
    tsSample *ns = zrealloc(ts->samples, na * sizeof(tsSample));
    if (!ns) return C_ERR;
    ts->samples = ns;
    ts->alloc = na;
    return C_OK;
}

int tsSet(redisTimeSeries *ts, int64_t timestamp, double value, const char *labels) {
    int64_t *timestamps = NULL;
    if (ts->len > 0) {
        timestamps = zmalloc(sizeof(int64_t) * ts->len);
        tsDecodeTimestamps(ts, timestamps);
    }

    ssize_t idx = timestamps ? tsFindIndex(timestamps, ts->len, timestamp) : -1;
    if (idx >= 0) {
        /* Update existing sample. */
        ts->samples[idx].value = value;
        if (ts->samples[idx].labels) {
            sdsfree(ts->samples[idx].labels);
            ts->samples[idx].labels = NULL;
        }
        if (labels) ts->samples[idx].labels = sdsnew(labels);
        zfree(timestamps);
        tsMemUsage(ts);
        return 0;
    }

    /* Insert keeping timestamps sorted. */
    size_t ins = timestamps ? tsLowerBound(timestamps, ts->len, timestamp) : 0;
    if (tsEnsureSamples(ts, ts->len + 1) == C_ERR) {
        zfree(timestamps);
        return -1;
    }

    /* Shift samples right. */
    if (ins < ts->len) {
        memmove(ts->samples + ins + 1, ts->samples + ins,
                (ts->len - ins) * sizeof(tsSample));
    }
    ts->samples[ins].value = value;
    ts->samples[ins].labels = labels ? sdsnew(labels) : NULL;
    ts->len++;

    /* Rebuild gorilla stream with new timestamp list. */
    int64_t *new_ts = zmalloc(sizeof(int64_t) * ts->len);
    for (size_t i = 0; i < ts->len; i++) {
        if (i < ins) new_ts[i] = timestamps[i];
        else if (i == ins) new_ts[i] = timestamp;
        else new_ts[i] = timestamps[i - 1];
    }
    tsRebuildGorilla(ts, new_ts);
    zfree(new_ts);
    zfree(timestamps);
    tsMemUsage(ts);
    return 1;
}

int tsGet(redisTimeSeries *ts, int64_t timestamp, double *value, const char **labels) {
    if (ts->len == 0) return 0;
    int64_t *timestamps = zmalloc(sizeof(int64_t) * ts->len);
    tsDecodeTimestamps(ts, timestamps);
    ssize_t idx = tsFindIndex(timestamps, ts->len, timestamp);
    zfree(timestamps);
    if (idx < 0) return 0;
    *value = ts->samples[idx].value;
    if (labels) *labels = ts->samples[idx].labels;
    return 1;
}

size_t tsRangeAggregate(redisTimeSeries *ts, int64_t start, int64_t end,
                        int agg, double *out_value) {
    if (ts->len == 0 || start > end) return 0;

    int64_t *timestamps = zmalloc(sizeof(int64_t) * ts->len);
    tsDecodeTimestamps(ts, timestamps);

    size_t lo = tsLowerBound(timestamps, ts->len, start);
    size_t count = 0;
    double acc = 0;
    double best = 0;
    int have = 0;

    for (size_t i = lo; i < ts->len; i++) {
        if (timestamps[i] > end) break;
        double v = ts->samples[i].value;
        if (!have) {
            best = v;
            acc = v;
            have = 1;
        } else {
            acc += v;
            if (agg == TS_AGG_MAX && v > best) best = v;
            if (agg == TS_AGG_MIN && v < best) best = v;
        }
        count++;
    }
    zfree(timestamps);

    if (count == 0) return 0;

    switch (agg) {
    case TS_AGG_MAX:
    case TS_AGG_MIN:
        *out_value = best;
        break;
    case TS_AGG_SUM:
        *out_value = acc;
        break;
    case TS_AGG_AVG:
        *out_value = acc / (double)count;
        break;
    default:
        *out_value = acc;
        break;
    }
    return count;
}

/* --------------------------------------------------------------------------
 * Redis object helpers
 * -------------------------------------------------------------------------- */

robj *createTimeSeriesObject(void) {
    redisTimeSeries *ts = tsCreate();
    robj *o = createObject(OBJ_TIMESERIES, ts);
    o->encoding = OBJ_ENCODING_TIMESERIES;
    return o;
}

void freeTimeSeriesObject(robj *o) {
    tsFree(o->ptr);
}

robj *timeseriesTypeDup(robj *o) {
    redisTimeSeries *dup = tsDup(o->ptr);
    robj *newobj = createObject(OBJ_TIMESERIES, dup);
    newobj->encoding = OBJ_ENCODING_TIMESERIES;
    return newobj;
}

size_t timeseriesTypeAllocSize(robj *o) {
    return tsMemUsage(o->ptr);
}

size_t timeseriesObjectLength(robj *o) {
    return tsLen(o->ptr);
}

/* --------------------------------------------------------------------------
 * Commands
 * -------------------------------------------------------------------------- */

static robj *lookupTimeSeriesForWriteOrReply(client *c, robj *key) {
    robj *o = lookupKeyWrite(c->db, key);
    if (o == NULL) {
        o = createTimeSeriesObject();
        dbAdd(c->db, key, &o);
    } else if (checkType(c, o, OBJ_TIMESERIES)) {
        return NULL;
    }
    return o;
}

static int parseTsTimestampOrReply(client *c, robj *arg, int64_t *ts) {
    long long v;
    if (getLongLongFromObjectOrReply(c, arg, &v, "invalid timestamp") != C_OK)
        return C_ERR;
    *ts = (int64_t)v;
    return C_OK;
}

static int parseTsDoubleOrReply(client *c, robj *arg, double *out) {
    if (getDoubleFromObjectOrReply(c, arg, out, "invalid value: must be a double") != C_OK)
        return C_ERR;
    if (isnan(*out) || isinf(*out)) {
        addReplyError(c, "value must be a finite double");
        return C_ERR;
    }
    return C_OK;
}

/* TS.SET key timestamp value [labels] */
void tssetCommand(client *c) {
    int64_t timestamp;
    double value;
    if (parseTsTimestampOrReply(c, c->argv[2], &timestamp) != C_OK) return;
    if (parseTsDoubleOrReply(c, c->argv[3], &value) != C_OK) return;

    const char *labels = NULL;
    if (c->argc >= 5) labels = c->argv[4]->ptr;

    robj *o = lookupTimeSeriesForWriteOrReply(c, c->argv[1]);
    if (o == NULL) return;

    redisTimeSeries *ts = o->ptr;
    size_t old_len = ts->len;
    int created = tsSet(ts, timestamp, value, labels);
    if (created < 0) {
        addReplyError(c, "out of memory");
        return;
    }

    updateKeysizesHist(c->db, OBJ_TIMESERIES, (long long)old_len, (long long)ts->len);
    keyModified(c, c->db, c->argv[1], o, 1);
    notifyKeyspaceEvent(NOTIFY_TIMESERIES, "ts.set", c->argv[1], c->db->id);
    server.dirty++;
    addReply(c, shared.ok);
}

/* TS.GET key timestamp -> value [labels] as multi-bulk (value) or (value, labels) */
void tsgetCommand(client *c) {
    int64_t timestamp;
    if (parseTsTimestampOrReply(c, c->argv[2], &timestamp) != C_OK) return;

    robj *o = lookupKeyRead(c->db, c->argv[1]);
    if (o == NULL) {
        addReplyNull(c);
        return;
    }
    if (checkType(c, o, OBJ_TIMESERIES)) return;

    double value;
    const char *labels = NULL;
    if (!tsGet(o->ptr, timestamp, &value, &labels)) {
        addReplyNull(c);
        return;
    }

    if (labels) {
        addReplyArrayLen(c, 2);
        addReplyDouble(c, value);
        addReplyBulkCString(c, labels);
    } else {
        addReplyDouble(c, value);
    }
}

/* TS.RANGE key start end aggregation */
void tsrangeCommand(client *c) {
    int64_t start, end;
    if (parseTsTimestampOrReply(c, c->argv[2], &start) != C_OK) return;
    if (parseTsTimestampOrReply(c, c->argv[3], &end) != C_OK) return;

    int agg;
    if (!strcasecmp(c->argv[4]->ptr, "max")) agg = TS_AGG_MAX;
    else if (!strcasecmp(c->argv[4]->ptr, "min")) agg = TS_AGG_MIN;
    else if (!strcasecmp(c->argv[4]->ptr, "sum")) agg = TS_AGG_SUM;
    else if (!strcasecmp(c->argv[4]->ptr, "avg")) agg = TS_AGG_AVG;
    else {
        addReplyError(c, "unknown aggregation: use max, min, sum or avg");
        return;
    }

    robj *o = lookupKeyRead(c->db, c->argv[1]);
    if (o == NULL) {
        addReplyNull(c);
        return;
    }
    if (checkType(c, o, OBJ_TIMESERIES)) return;

    double result;
    size_t n = tsRangeAggregate(o->ptr, start, end, agg, &result);
    if (n == 0) {
        addReplyNull(c);
        return;
    }
    addReplyDouble(c, result);
}

/* AOF rewrite helper — emit TS.SET for each sample. */
int rewriteTimeSeriesObject(rio *r, robj *key, robj *o) {
    redisTimeSeries *ts = o->ptr;
    if (ts->len == 0) return 1;

    int64_t *timestamps = zmalloc(sizeof(int64_t) * ts->len);
    tsDecodeTimestamps(ts, timestamps);

    for (size_t i = 0; i < ts->len; i++) {
        int argc = ts->samples[i].labels ? 5 : 4;
        if (!rioWriteBulkCount(r, '*', argc)) goto err;
        if (!rioWriteBulkString(r, "TS.SET", 6)) goto err;
        if (!rioWriteBulkObject(r, key)) goto err;
        if (!rioWriteBulkLongLong(r, timestamps[i])) goto err;

        char buf[128];
        int dlen = snprintf(buf, sizeof(buf), "%.17g", ts->samples[i].value);
        if (!rioWriteBulkString(r, buf, dlen)) goto err;

        if (ts->samples[i].labels) {
            if (!rioWriteBulkString(r, ts->samples[i].labels,
                                    sdslen(ts->samples[i].labels)))
                goto err;
        }
    }
    zfree(timestamps);
    return 1;
err:
    zfree(timestamps);
    return 0;
}

/*
 *   Lessfs: A data deduplicating filesystem.
 *   Copyright (C) 2008 Mark Ruijter <mruijter@lessfs.com>
 *
 *   This program is free software.
 *   You can redistribute lessfs and/or modify it under the
 *   terms of either (1) the GNU General Public License;
 *   either version 3 of the License, or (at your option)
 *   any later version as published by the Free Software
 *   Foundation; or (2) obtain a commercial license
 *   by contacting the Author.
 *
 *   This program is distributed in the hope that it will
 *   be useful, but WITHOUT ANY WARRANTY; without even the
 *   implied warranty of MERCHANTABILITY or FITNESS FOR A
 *   PARTICULAR PURPOSE. See the GNU General Public License
 *   for more details.
 */
#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 29
#endif
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define _GNU_SOURCE

#ifdef LMDB

#ifndef LFATAL
#include "lib_log.h"
#endif
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <lmdb.h>
#include <fuse/fuse_lowlevel.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/file.h>
#include <fuse.h>
#include <fcntl.h>
#include <pthread.h>
#include "lib_tc_replacements.h"
#include <stdbool.h>
#include <stdint.h>

#include "lib_safe.h"
#include "lib_cfg.h"
#include "retcodes.h"
#ifdef LZO
#include "lib_lzo.h"
#endif
#include "lib_qlz.h"
#include "lib_common.h"
#include "lib_crypto.h"
#include "file_io.h"
#include "lib_repl.h"
#include "lib_lmdb.h"


/* Max shard count; valid lmdb_db_count values:
 * 1, 2, 4, 8, 16 */
#define LMDB_MAX_SHARDS 16

/* Per-shard LMDB handles */
static MDB_env *lmdb_env[LMDB_MAX_SHARDS];
static MDB_dbi dbi_dbb[LMDB_MAX_SHARDS];      /* fileblock */
static MDB_dbi dbi_dbu[LMDB_MAX_SHARDS];      /* blockusage */
static MDB_dbi dbi_dbp[LMDB_MAX_SHARDS];      /* metadata */
static MDB_dbi dbi_dbl[LMDB_MAX_SHARDS];      /* hardlink */
static MDB_dbi dbi_dbs[LMDB_MAX_SHARDS];      /* symlink */
static MDB_dbi dbi_dbdta[LMDB_MAX_SHARDS];    /* blockdata */
static MDB_dbi dbi_dbdirent[LMDB_MAX_SHARDS]; /* dirent */
static MDB_dbi dbi_freelist[LMDB_MAX_SHARDS]; /* freelist */

/* Per-shard write transactions and mutexes */
static MDB_txn *write_txn[LMDB_MAX_SHARDS];
static pthread_mutex_t lmdb_mutex[LMDB_MAX_SHARDS];
static const char *lmdb_lockedby[LMDB_MAX_SHARDS];

int fdbdta;
int frepl = 0;
int freplbl = 0;
extern unsigned long long nextoffset;
unsigned long long lastoffset;
const char *bdb_lockedby;

#define die_lmdberr(fmt, ...) \
    do { LFATAL(fmt, ##__VA_ARGS__); exit(EXIT_DATAERR); \
    } while (0)

/*
 * Return the MDB_dbi handle for the given
 * shard and database selector constant.
 */
static MDB_dbi lmdb_set_db(int shard, int database)
{
    switch (database) {
    case 0: /* DBDTA */
        return dbi_dbdta[shard];
    case 1: /* DBU */
        return dbi_dbu[shard];
    case 2: /* DBB */
        return dbi_dbb[shard];
    case 3: /* DBP */
        return dbi_dbp[shard];
    case 4: /* DBS */
        return dbi_dbs[shard];
    case 5: /* DBL */
        return dbi_dbl[shard];
    case 6: /* FREELIST */
        return dbi_freelist[shard];
    case 7: /* DBDIRENT */
        return dbi_dbdirent[shard];
    default:
        die_lmdberr(
            "lmdb_set_db: invalid database %d",
            database);
    }
    return dbi_dbp[shard]; /* not reached */
}

/*
 * inode_to_shard: map an inode to its shard index.
 * Uses the last nibble of the inode and the
 * configured shard count.
 */
int inode_to_shard(unsigned long long inode)
{
    if (config->lmdb_db_count <= 1)
        return 0;
    return (int)((inode & 0xf)
                 / (16 / config->lmdb_db_count));
}

/*
 * shard_path: return path for shard s.
 * N=1 -> config->meta unchanged.
 * N>1 -> <meta>/db_XX  (zero-padded hex).
 * Caller must s_free() the result.
 */
static char *shard_path(int shard)
{
    if (config->lmdb_db_count <= 1)
        return s_strdup(config->meta);
    return as_sprintf(__FILE__, __LINE__,
                      "%s/db_%02x",
                      config->meta, shard);
}

/*
 * dbp_get_within_lock: read DBP for inode while
 * write_txn[held_shard] is open.
 * Same shard -> use held write_txn directly.
 * Cross-shard -> open a brief MDB_RDONLY txn.
 */
static DAT *dbp_get_within_lock(
    unsigned long long inode, int held_shard)
{
    int shard = inode_to_shard(inode);
    MDB_txn *txn;
    MDB_txn *rtxn = NULL;
    MDB_val mkey, mdata;
    DAT *result = NULL;
    int ret;

    if (shard == held_shard) {
        txn = write_txn[shard];
    } else {
        ret = mdb_txn_begin(lmdb_env[shard], NULL,
                            MDB_RDONLY, &rtxn);
        if (ret != 0)
            return NULL;
        txn = rtxn;
    }
    mkey.mv_data = &inode;
    mkey.mv_size = sizeof(unsigned long long);
    ret = mdb_get(txn, dbi_dbp[shard],
                  &mkey, &mdata);
    if (ret == 0) {
        result = s_malloc(sizeof(DAT));
        result->data = s_malloc(mdata.mv_size);
        memcpy(result->data, mdata.mv_data,
               mdata.mv_size);
        result->size = mdata.mv_size;
    }
    if (rtxn)
        mdb_txn_abort(rtxn);
    return result;
}

/*
 * dbl_keyval_within_lock: search DBL for key+val
 * while write_txn[held_shard] is open.
 */
static DAT *dbl_keyval_within_lock(
    unsigned long long inode, int held_shard,
    void *keydata, int keylen,
    void *val, int vallen)
{
    int shard = inode_to_shard(inode);
    MDB_txn *txn;
    MDB_txn *rtxn = NULL;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    DAT *result = NULL;
    int ret;

    if (shard == held_shard) {
        txn = write_txn[shard];
    } else {
        ret = mdb_txn_begin(lmdb_env[shard], NULL,
                            MDB_RDONLY, &rtxn);
        if (ret != 0)
            return NULL;
        txn = rtxn;
    }
    ret = mdb_cursor_open(txn,
                          dbi_dbl[shard], &cur);
    if (ret != 0) {
        if (rtxn)
            mdb_txn_abort(rtxn);
        return NULL;
    }
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = val;
    mdata.mv_size = vallen;
    if (val == NULL || vallen == 0) {
        ret = mdb_cursor_get(cur, &mkey, &mdata,
                             MDB_SET_KEY);
    } else {
        ret = mdb_cursor_get(cur, &mkey, &mdata,
                             MDB_GET_BOTH_RANGE);
    }
    if (ret == 0) {
        if (vallen == 0
                || (mdata.mv_size >= (size_t)vallen
                    && 0 == memcmp(mdata.mv_data,
                                   val, vallen))) {
            result = s_malloc(sizeof(DAT));
            result->data =
                s_malloc(mdata.mv_size);
            memcpy(result->data, mdata.mv_data,
                   mdata.mv_size);
            result->size = mdata.mv_size;
        }
    }
    mdb_cursor_close(cur);
    if (rtxn)
        mdb_txn_abort(rtxn);
    return result;
}

/*
 * bdb_lock_shard / release_bdb_lock_shard:
 * acquire/release write lock for a specific shard.
 */
void bdb_lock_shard(int shard, const char *msg)
{
    int ret;
    pthread_mutex_lock(&lmdb_mutex[shard]);
    lmdb_lockedby[shard] = msg;
    ret = mdb_txn_begin(lmdb_env[shard], NULL, 0,
                        &write_txn[shard]);
    if (ret != 0)
        die_lmdberr(
            "bdb_lock_shard txn_begin: %s",
            mdb_strerror(ret));
}

void release_bdb_lock_shard(int shard)
{
    if (write_txn[shard]) {
        int ret = mdb_txn_commit(write_txn[shard]);
        write_txn[shard] = NULL;
        if (ret != 0)
            die_lmdberr(
                "release_bdb_lock_shard commit: "
                "%s", mdb_strerror(ret));
    }
    lmdb_lockedby[shard] = NULL;
    pthread_mutex_unlock(&lmdb_mutex[shard]);
}

void bdb_lock(const char *msg)
{
    bdb_lock_shard(0, msg);
}

int try_bdb_lock(void)
{
    int ret = pthread_mutex_trylock(
                  &lmdb_mutex[0]);
    if (ret == 0) {
        int txret;
        lmdb_lockedby[0] = "try_bdb_lock";
        txret = mdb_txn_begin(lmdb_env[0], NULL, 0,
                               &write_txn[0]);
        if (txret != 0)
            die_lmdberr(
                "try_bdb_lock txn_begin: %s",
                mdb_strerror(txret));
    }
    return ret;
}

void release_bdb_lock(void)
{
    release_bdb_lock_shard(0);
}

void bdb_checkpoint(void)
{
    int shard;
    for (shard = 0;
         shard < config->lmdb_db_count; shard++)
        mdb_env_sync(lmdb_env[shard], 1);
}

void bdb_stat(void)
{
    MDB_stat mdbstat;
    int shard;
    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        if (0 == mdb_env_stat(lmdb_env[shard],
                              &mdbstat)) {
            LINFO("LMDB shard %d: "
                  "page_size=%u depth=%u "
                  "branch=%zu leaf=%zu "
                  "overflow=%zu entries=%zu",
                  shard, mdbstat.ms_psize,
                  mdbstat.ms_depth,
                  mdbstat.ms_branch_pages,
                  mdbstat.ms_leaf_pages,
                  mdbstat.ms_overflow_pages,
                  mdbstat.ms_entries);
        }
    }
}

void start_transactions(void)
{
    int ret, shard;
    FUNC;
    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        ret = mdb_txn_begin(
            lmdb_env[shard], NULL, 0,
            &write_txn[shard]);
        if (ret != 0)
            die_lmdberr(
                "txn_begin shard %d failed: %s",
                shard, mdb_strerror(ret));
    }
    EFUNC;
}

void commit_transactions(void)
{
    int ret, shard;
    FUNC;
    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        ret = mdb_txn_commit(write_txn[shard]);
        write_txn[shard] = NULL;
        if (ret != 0)
            die_lmdberr(
                "txn_commit shard %d failed: %s",
                shard, mdb_strerror(ret));
    }
    EFUNC;
}

void abort_transactions(void)
{
    int shard;
    FUNC;
    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        if (write_txn[shard]) {
            mdb_txn_abort(write_txn[shard]);
            write_txn[shard] = NULL;
        }
    }
    EFUNC;
}

void bdb_open(void)
{
    int ret, shard;
    struct stat stbuf;
    char *hashstr;
    DAT *data;
    char *sp, *dbname, *path;
    unsigned int db_flags;
    unsigned long long mapsize;

    FUNC;

    /* Replication log file setup */
    if (config->blockdata_io_type == FILE_IO) {
        sp = s_dirname(config->blockdata);
    } else {
        sp = s_strdup(config->blockdata);
    }
    dbname = as_sprintf(__FILE__, __LINE__,
                        "%s/replog.dta", sp);
    config->replication_logfile = s_strdup(dbname);
    if (-1 == (frepl = s_open2(dbname,
            O_CREAT | O_RDWR | O_NOATIME, S_IRWXU)))
        die_syserr();
    if (0 != flock(frepl, LOCK_EX | LOCK_NB)) {
        LFATAL("Failed to lock the replication "
               "logfile %s\nlessfs must be unmounted"
               " before using this option!",
               config->replication_logfile);
        exit(EXIT_USAGE);
    }
    if (config->replication
            && config->replication_role == 0) {
        if (-1 == (freplbl = s_open2(dbname,
                O_RDWR, S_IRWXU)))
            die_syserr();
    }
    s_free(dbname);
    s_free(sp);

    mapsize = config->lmdb_mapsize
              ? config->lmdb_mapsize
              : LMDB_DEFAULT_MAPSIZE;
    db_flags = MDB_CREATE;

    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        pthread_mutex_init(&lmdb_mutex[shard], NULL);
        path = shard_path(shard);
        LDEBUG("Open LMDB shard %d: %s",
               shard, path);

        ret = mdb_env_create(&lmdb_env[shard]);
        if (ret != 0)
            die_lmdberr(
                "mdb_env_create shard %d: %s",
                shard, mdb_strerror(ret));

        ret = mdb_env_set_maxdbs(
                  lmdb_env[shard], 8);
        if (ret != 0)
            die_lmdberr(
                "mdb_env_set_maxdbs shard %d: %s",
                shard, mdb_strerror(ret));

        ret = mdb_env_set_mapsize(
                  lmdb_env[shard], mapsize);
        if (ret != 0)
            die_lmdberr(
                "mdb_env_set_mapsize shard %d: %s",
                shard, mdb_strerror(ret));

        /* Create shard dir if needed */
        if (mkdir(path, 0755) != 0
                && errno != EEXIST) {
            die_lmdberr(
                "mkdir(%s): %s",
                path, strerror(errno));
        }
        ret = mdb_env_open(lmdb_env[shard], path,
                           MDB_NOSYNC | MDB_WRITEMAP,
                           0644);
        if (ret != 0)
            die_lmdberr("mdb_env_open(%s): %s",
                         path, mdb_strerror(ret));
        s_free(path);

        ret = mdb_txn_begin(lmdb_env[shard], NULL,
                             0, &write_txn[shard]);
        if (ret != 0)
            die_lmdberr(
                "initial txn_begin shard %d: %s",
                shard, mdb_strerror(ret));

        ret = mdb_dbi_open(write_txn[shard],
                           "metadata", db_flags,
                           &dbi_dbp[shard]);
        if (ret != 0)
            die_lmdberr(
                "open metadata shard %d: %s",
                shard, mdb_strerror(ret));

        ret = mdb_dbi_open(write_txn[shard],
                           "blockusage", db_flags,
                           &dbi_dbu[shard]);
        if (ret != 0)
            die_lmdberr(
                "open blockusage shard %d: %s",
                shard, mdb_strerror(ret));

        ret = mdb_dbi_open(write_txn[shard],
                           "fileblock", db_flags,
                           &dbi_dbb[shard]);
        if (ret != 0)
            die_lmdberr(
                "open fileblock shard %d: %s",
                shard, mdb_strerror(ret));

        ret = mdb_dbi_open(write_txn[shard],
                           "hardlink",
                           db_flags | MDB_DUPSORT,
                           &dbi_dbl[shard]);
        if (ret != 0)
            die_lmdberr(
                "open hardlink shard %d: %s",
                shard, mdb_strerror(ret));

        ret = mdb_dbi_open(write_txn[shard],
                           "symlink", db_flags,
                           &dbi_dbs[shard]);
        if (ret != 0)
            die_lmdberr(
                "open symlink shard %d: %s",
                shard, mdb_strerror(ret));

        ret = mdb_dbi_open(write_txn[shard],
                           "dirent",
                           db_flags | MDB_DUPSORT,
                           &dbi_dbdirent[shard]);
        if (ret != 0)
            die_lmdberr(
                "open dirent shard %d: %s",
                shard, mdb_strerror(ret));

        ret = mdb_dbi_open(write_txn[shard],
                           "freelist",
                           db_flags | MDB_DUPSORT,
                           &dbi_freelist[shard]);
        if (ret != 0)
            die_lmdberr(
                "open freelist shard %d: %s",
                shard, mdb_strerror(ret));

        ret = mdb_dbi_open(write_txn[shard],
                           "blockdata", db_flags,
                           &dbi_dbdta[shard]);
        if (ret != 0)
            die_lmdberr(
                "open blockdata shard %d: %s",
                shard, mdb_strerror(ret));

        ret = mdb_txn_commit(write_txn[shard]);
        write_txn[shard] = NULL;
        if (ret != 0)
            die_lmdberr(
                "dbi commit shard %d: %s",
                shard, mdb_strerror(ret));
    }

    LINFO("LMDB open: %d shard(s).",
          config->lmdb_db_count);
    open_trees();

    if (config->blockdata_io_type == FILE_IO) {
        if (-1 == (fdbdta = s_open2(
                config->blockdata,
                O_CREAT | O_RDWR | O_NOATIME,
                S_IRWXU)))
            die_syserr();
        if (-1 == (stat(config->blockdata, &stbuf)))
            die_syserr();
        hashstr = as_sprintf(__FILE__, __LINE__,
                             "NEXTOFFSET");
        config->nexthash = (char *)
            thash((unsigned char *) hashstr,
                  strlen(hashstr));
        s_free(hashstr);
        data = search_dbdata(DBU, config->nexthash,
                             config->hashlen, LOCK);
        if (NULL == data) {
            nextoffset = 0;
            LINFO("First use, nextoffset = 0");
        } else {
            memcpy(&nextoffset, data->data,
                   sizeof(unsigned long long));
            DATfree(data);
        }
        LINFO("nextoffset = %llu", nextoffset);
        lastoffset = nextoffset;
    }

    EFUNC;
}
void bdb_close(void)
{
    int shard;
    FUNC;
    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        if (write_txn[shard]) {
            mdb_txn_commit(write_txn[shard]);
            write_txn[shard] = NULL;
        }
    }
    close_trees();
    if (config->blockdata_io_type == FILE_IO) {
        fsync(fdbdta);
        close(fdbdta);
        free(config->nexthash);
    }
    flock(frepl, LOCK_UN);
    fsync(frepl);
    close(frepl);
    if (config->replication
            && config->replication_role == 0)
        close(freplbl);
    s_free(config->replication_logfile);

    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        mdb_dbi_close(lmdb_env[shard],
                      dbi_dbb[shard]);
        mdb_dbi_close(lmdb_env[shard],
                      dbi_dbu[shard]);
        mdb_dbi_close(lmdb_env[shard],
                      dbi_dbp[shard]);
        mdb_dbi_close(lmdb_env[shard],
                      dbi_dbl[shard]);
        mdb_dbi_close(lmdb_env[shard],
                      dbi_dbs[shard]);
        mdb_dbi_close(lmdb_env[shard],
                      dbi_dbdta[shard]);
        mdb_dbi_close(lmdb_env[shard],
                      dbi_dbdirent[shard]);
        mdb_dbi_close(lmdb_env[shard],
                      dbi_freelist[shard]);
        mdb_env_close(lmdb_env[shard]);
        lmdb_env[shard] = NULL;
    }
    EFUNC;
}
void drop_databases(void)
{
    MDB_txn *drop_txn;
    int ret, shard;

    FUNC;
    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        if (write_txn[shard]) {
            mdb_txn_commit(write_txn[shard]);
            write_txn[shard] = NULL;
        }
        ret = mdb_txn_begin(lmdb_env[shard], NULL,
                             0, &drop_txn);
        if (ret != 0)
            die_lmdberr(
                "drop txn_begin shard %d: %s",
                shard, mdb_strerror(ret));
        mdb_drop(drop_txn, dbi_dbb[shard], 0);
        mdb_drop(drop_txn, dbi_dbu[shard], 0);
        mdb_drop(drop_txn, dbi_dbp[shard], 0);
        mdb_drop(drop_txn, dbi_dbl[shard], 0);
        mdb_drop(drop_txn, dbi_dbs[shard], 0);
        mdb_drop(drop_txn, dbi_dbdta[shard], 0);
        mdb_drop(drop_txn, dbi_dbdirent[shard], 0);
        mdb_drop(drop_txn, dbi_freelist[shard], 0);
        ret = mdb_txn_commit(drop_txn);
        if (ret != 0)
            die_lmdberr(
                "drop commit shard %d: %s",
                shard, mdb_strerror(ret));
    }
    LINFO("All LMDB databases dropped.");
    EFUNC;
}
DAT *search_dbdata(int database, void *keydata,
                   int len, bool lock)
{
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    DAT *result = NULL;
    int ret;

    FUNC;
    if (lock)
        bdb_lock((char *) __PRETTY_FUNCTION__);

    dbi = lmdb_set_db(0, database);
    mkey.mv_data = keydata;
    mkey.mv_size = len;
    ret = mdb_get(write_txn[0], dbi,
                  &mkey, &mdata);
    if (ret == 0) {
        result = s_malloc(sizeof(DAT));
        result->data = s_malloc(mdata.mv_size);
        memcpy(result->data, mdata.mv_data,
               mdata.mv_size);
        result->size = mdata.mv_size;
    }

    if (lock)
        release_bdb_lock();
    EFUNC;
    return result;
}

void delete_key(int database, void *keydata,
                int len, const char *msg)
{
    MDB_dbi dbi;
    MDB_val mkey;
    int ret;

    bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(0, database);
    mkey.mv_data = keydata;
    mkey.mv_size = len;
    ret = mdb_del(write_txn[0], dbi,
                  &mkey, NULL);
    if (ret != 0 && ret != MDB_NOTFOUND) {
        if (msg)
            die_lmdberr("delete_key failed: %s",
                         mdb_strerror(ret));
        else
            LINFO("delete_key failed: %s",
                  mdb_strerror(ret));
    }
    release_bdb_lock();
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLDELETE,
                        keydata, len, NULL, 0,
                        MAX_ALLOWED_THREADS - 2);
    }
}

void bin_write_dbdata(int database, void *keydata,
                      int keylen, void *valdata,
                      int datalen)
{
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(0, database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    ret = mdb_put(write_txn[0], dbi,
                  &mkey, &mdata, 0);
    if (ret != 0) {
        mdb_txn_abort(write_txn[0]);
        write_txn[0] = NULL;
        LFATAL("bin_write_dbdata : database %u "
               "keylen %u datalen %u",
               database, keylen, datalen);
        die_lmdberr("Database write failed: %s",
                     mdb_strerror(ret));
    }
    release_bdb_lock();
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE,
                        keydata, keylen,
                        valdata, datalen,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

void bin_write(int database, void *keydata, int keylen,
               void *valdata, int datalen)
{
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(0, database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    ret = mdb_put(write_txn[0], dbi,
                  &mkey, &mdata, 0);
    if (ret != 0) {
        mdb_txn_abort(write_txn[0]);
        write_txn[0] = NULL;
        die_lmdberr("bin_write failed: %s",
                     mdb_strerror(ret));
    }
    release_bdb_lock();
}

void start_flush_commit(void)
{
    FUNC;
    if (config->transactions)
        lessfs_trans_stamp();
    if (lastoffset != nextoffset) {
        LDEBUG("write nextoffset=%llu", nextoffset);
        bin_write_dbdata(DBU, config->nexthash,
                         config->hashlen,
                         (unsigned char *) &nextoffset,
                         sizeof(unsigned long long));
        lastoffset = nextoffset;
    }
    sync_all_filesizes();
    if (config->blockdata_io_type == FILE_IO) {
        fsync(fdbdta);
    }
    /* Per-lock txns: each operation already committed.
       Sync all shard envs for durability. */
    {
        int shard;
        for (shard = 0;
             shard < config->lmdb_db_count; shard++)
            mdb_env_sync(lmdb_env[shard], 1);
    }
    EFUNC;
}

void end_flush_commit(void)
{
    int shard;
    FUNC;
    if (config->transactions) {
        for (shard = 0;
             shard < config->lmdb_db_count;
             shard++)
            mdb_env_sync(lmdb_env[shard], 1);
    }
    EFUNC;
}


/* === Btree / cursor functions === */

void btbin_write_dbdata(int database, void *keydata,
                        int keylen, void *valdata,
                        int datalen)
{
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(0, database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    ret = mdb_put(write_txn[0], dbi,
                  &mkey, &mdata, 0);
    if (ret != 0) {
        mdb_txn_abort(write_txn[0]);
        write_txn[0] = NULL;
        die_lmdberr("btbin_write_dbdata failed: %s",
                     mdb_strerror(ret));
    }
    release_bdb_lock();
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE,
                        keydata, keylen,
                        valdata, datalen,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

void btbin_write_dup(int database, void *keydata,
                     int keylen, void *valdata,
                     int datalen, bool lock)
{
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    if (lock)
        bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(0, database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    /* LMDB DUPSORT databases accept dups natively */
    ret = mdb_put(write_txn[0], dbi, &mkey, &mdata, 0);
    if (database == DBDIRENT && keylen == sizeof(unsigned long long) && datalen == sizeof(unsigned long long)) {
        unsigned long long trc_key, trc_val;
        memcpy(&trc_key, keydata, sizeof(trc_key));
        memcpy(&trc_val, valdata, sizeof(trc_val));
    }
    if (ret != 0) {
        mdb_txn_abort(write_txn[0]);
        write_txn[0] = NULL;
        die_lmdberr("btbin_write_dup failed: %s",
                     mdb_strerror(ret));
    }
    if (lock)
        release_bdb_lock();
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE,
                        keydata, keylen,
                        valdata, datalen,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

int bt_entry_exists(int database, void *keydata,
                    int keylen, void *valdata,
                    int datalen)
{
    MDB_dbi dbi;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret, found = 0;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(0, database);

    ret = mdb_cursor_open(write_txn[0], dbi, &cur);
    if (ret != 0) {
        release_bdb_lock();
        die_lmdberr("bt_entry_exists cursor: %s",
                     mdb_strerror(ret));
    }
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_GET_BOTH);
    if (ret == 0)
        found = 1;
    mdb_cursor_close(cur);
    release_bdb_lock();
    EFUNC;
    return found;
}

DAT *btsearch_keyval(int database, void *keydata,
                     int keylen, void *val,
                     int vallen, bool lock)
{
    MDB_dbi dbi;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    DAT *result = NULL;
    int ret;

    FUNC;
    if (lock)
        bdb_lock((char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(0, database);

    ret = mdb_cursor_open(write_txn[0], dbi, &cur);
    if (ret != 0) {
        if (lock) release_bdb_lock();
        die_lmdberr("btsearch_keyval cursor: %s",
                     mdb_strerror(ret));
    }
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = val;
    mdata.mv_size = vallen;

    if (val == NULL || vallen == 0) {
        ret = mdb_cursor_get(cur, &mkey, &mdata,
                             MDB_SET_KEY);
    } else {
        ret = mdb_cursor_get(cur, &mkey, &mdata,
                             MDB_GET_BOTH_RANGE);
    }
    if (ret == 0) {
        /*
         * Verify the value starts with the
         * requested prefix.
         */
        if (vallen == 0
                || (mdata.mv_size >= (size_t) vallen
                && 0 == memcmp(mdata.mv_data,
                               val, vallen))) {
            result = s_malloc(sizeof(DAT));
            result->data = s_malloc(mdata.mv_size);
            memcpy(result->data, mdata.mv_data,
                   mdata.mv_size);
            result->size = mdata.mv_size;
        }
    }
    mdb_cursor_close(cur);
    if (lock)
        release_bdb_lock();
    EFUNC;
    return result;
}

int btdelete_curkey(int database, void *keydata,
                    int keylen, void *value,
                    int vallen, const char *msg)
{
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    if (database == DBDIRENT
            && keylen == sizeof(unsigned long long)
            && vallen == sizeof(unsigned long long)) {
        unsigned long long trc_key, trc_val;
        memcpy(&trc_key, keydata, sizeof(trc_key));
        memcpy(&trc_val, value, sizeof(trc_val));
    }
    dbi = lmdb_set_db(0, database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = value;
    mdata.mv_size = vallen;
    /* mdb_del with both key+data deletes specific dup */
    ret = mdb_del(write_txn[0], dbi, &mkey, &mdata);
    if (ret != 0 && ret != MDB_NOTFOUND) {
        if (msg)
            LFATAL("btdelete_curkey %s: %s",
                   msg, mdb_strerror(ret));
    }
    release_bdb_lock();
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLDELETECURKEY,
                        keydata, keylen,
                        value, vallen,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
    return (ret == 0) ? 0 : -1;
}



/* ===================================================
 * Inode-aware variants: route DB ops to the correct
 * shard based on the inode number.
 * These are called by lib_common.c for operations on
 * DBP, DBB, DBDIRENT, DBL, DBS.
 * =================================================== */

DAT *search_inode_dbdata(int database,
                         unsigned long long inode,
                         void *keydata, int len,
                         bool lock)
{
    int shard = inode_to_shard(inode);
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    DAT *result = NULL;
    int ret;

    FUNC;
    if (lock)
        bdb_lock_shard(shard,
            (char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(shard, database);
    mkey.mv_data = keydata;
    mkey.mv_size = len;
    ret = mdb_get(write_txn[shard], dbi,
                  &mkey, &mdata);
    if (ret == 0) {
        result = s_malloc(sizeof(DAT));
        result->data = s_malloc(mdata.mv_size);
        memcpy(result->data, mdata.mv_data,
               mdata.mv_size);
        result->size = mdata.mv_size;
    }
    if (lock)
        release_bdb_lock_shard(shard);
    EFUNC;
    return result;
}

void bin_write_inode_dbdata(
    int database, unsigned long long inode,
    void *keydata, int keylen,
    void *valdata, int datalen)
{
    int shard = inode_to_shard(inode);
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    bdb_lock_shard(shard,
        (char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(shard, database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    ret = mdb_put(write_txn[shard], dbi,
                  &mkey, &mdata, 0);
    if (ret != 0) {
        mdb_txn_abort(write_txn[shard]);
        write_txn[shard] = NULL;
        LFATAL("bin_write_inode_dbdata: "
               "db %u keylen %u datalen %u",
               database, keylen, datalen);
        die_lmdberr(
            "inode DB write failed: %s",
            mdb_strerror(ret));
    }
    release_bdb_lock_shard(shard);
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE,
                        keydata, keylen,
                        valdata, datalen,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

void delete_inode_key(int database,
                      unsigned long long inode,
                      void *keydata, int len,
                      const char *msg)
{
    int shard = inode_to_shard(inode);
    MDB_dbi dbi;
    MDB_val mkey;
    int ret;

    bdb_lock_shard(shard,
        (char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(shard, database);
    mkey.mv_data = keydata;
    mkey.mv_size = len;
    ret = mdb_del(write_txn[shard], dbi,
                  &mkey, NULL);
    if (ret != 0 && ret != MDB_NOTFOUND) {
        if (msg)
            die_lmdberr(
                "delete_inode_key failed: %s",
                mdb_strerror(ret));
        else
            LINFO("delete_inode_key failed: %s",
                  mdb_strerror(ret));
    }
    release_bdb_lock_shard(shard);
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLDELETE,
                        keydata, len, NULL, 0,
                        MAX_ALLOWED_THREADS - 2);
    }
}

void btbin_write_inode_dbdata(
    int database, unsigned long long inode,
    void *keydata, int keylen,
    void *valdata, int datalen)
{
    int shard = inode_to_shard(inode);
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    bdb_lock_shard(shard,
        (char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(shard, database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    ret = mdb_put(write_txn[shard], dbi,
                  &mkey, &mdata, 0);
    if (ret != 0) {
        mdb_txn_abort(write_txn[shard]);
        write_txn[shard] = NULL;
        die_lmdberr(
            "btbin_write_inode_dbdata: %s",
            mdb_strerror(ret));
    }
    release_bdb_lock_shard(shard);
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE,
                        keydata, keylen,
                        valdata, datalen,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

void btbin_write_inode_dup(
    int database, unsigned long long inode,
    void *keydata, int keylen,
    void *valdata, int datalen, bool lock)
{
    int shard = inode_to_shard(inode);
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    if (lock)
        bdb_lock_shard(shard,
            (char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(shard, database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    ret = mdb_put(write_txn[shard], dbi,
                  &mkey, &mdata, 0);
    if (ret != 0) {
        mdb_txn_abort(write_txn[shard]);
        write_txn[shard] = NULL;
        die_lmdberr(
            "btbin_write_inode_dup: %s",
            mdb_strerror(ret));
    }
    if (lock)
        release_bdb_lock_shard(shard);
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE,
                        keydata, keylen,
                        valdata, datalen,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

int bt_inode_entry_exists(
    int database, unsigned long long inode,
    void *keydata, int keylen,
    void *valdata, int datalen)
{
    int shard = inode_to_shard(inode);
    MDB_dbi dbi;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret, found = 0;

    FUNC;
    bdb_lock_shard(shard,
        (char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(shard, database);
    ret = mdb_cursor_open(write_txn[shard],
                          dbi, &cur);
    if (ret != 0) {
        release_bdb_lock_shard(shard);
        die_lmdberr(
            "bt_inode_entry_exists cursor: %s",
            mdb_strerror(ret));
    }
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = valdata;
    mdata.mv_size = datalen;
    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_GET_BOTH);
    if (ret == 0)
        found = 1;
    mdb_cursor_close(cur);
    release_bdb_lock_shard(shard);
    EFUNC;
    return found;
}

DAT *btsearch_inode_keyval(
    int database, unsigned long long inode,
    void *keydata, int keylen,
    void *val, int vallen, bool lock)
{
    int shard = inode_to_shard(inode);
    MDB_dbi dbi;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    DAT *result = NULL;
    int ret;

    FUNC;
    if (lock)
        bdb_lock_shard(shard,
            (char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(shard, database);
    ret = mdb_cursor_open(write_txn[shard],
                          dbi, &cur);
    if (ret != 0) {
        if (lock)
            release_bdb_lock_shard(shard);
        die_lmdberr(
            "btsearch_inode_keyval cursor: %s",
            mdb_strerror(ret));
    }
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = val;
    mdata.mv_size = vallen;
    if (val == NULL || vallen == 0) {
        ret = mdb_cursor_get(cur, &mkey, &mdata,
                             MDB_SET_KEY);
    } else {
        ret = mdb_cursor_get(cur, &mkey, &mdata,
                             MDB_GET_BOTH_RANGE);
    }
    if (ret == 0) {
        if (vallen == 0
                || (mdata.mv_size >= (size_t)vallen
                && 0 == memcmp(mdata.mv_data,
                               val, vallen))) {
            result = s_malloc(sizeof(DAT));
            result->data =
                s_malloc(mdata.mv_size);
            memcpy(result->data, mdata.mv_data,
                   mdata.mv_size);
            result->size = mdata.mv_size;
        }
    }
    mdb_cursor_close(cur);
    if (lock)
        release_bdb_lock_shard(shard);
    EFUNC;
    return result;
}

int btdelete_inode_curkey(
    int database, unsigned long long inode,
    void *keydata, int keylen,
    void *value, int vallen, const char *msg)
{
    int shard = inode_to_shard(inode);
    MDB_dbi dbi;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    bdb_lock_shard(shard,
        (char *) __PRETTY_FUNCTION__);
    dbi = lmdb_set_db(shard, database);
    mkey.mv_data = keydata;
    mkey.mv_size = keylen;
    mdata.mv_data = value;
    mdata.mv_size = vallen;
    ret = mdb_del(write_txn[shard], dbi,
                  &mkey, &mdata);
    if (ret != 0 && ret != MDB_NOTFOUND) {
        if (msg)
            LFATAL(
                "btdelete_inode_curkey %s: %s",
                msg, mdb_strerror(ret));
    }
    release_bdb_lock_shard(shard);
    if (config->replication == 1
            && config->replication_role == 0) {
        write_repl_data(database,
                        REPLDELETECURKEY,
                        keydata, keylen,
                        value, vallen,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
    return (ret == 0) ? 0 : -1;
}

/* Return the inode shard for a DINOINO key
 * (used for DBL which is keyed by DINOINO). */
int count_dirlinks_inode(void *linkstr, int len,
                         unsigned long long inode)
{
    int shard = inode_to_shard(inode);
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int count = 0, ret;

    FUNC;
    bdb_lock_shard(shard,
        (char *) __PRETTY_FUNCTION__);
    ret = mdb_cursor_open(write_txn[shard],
                          dbi_dbl[shard], &cur);
    if (ret != 0) {
        release_bdb_lock_shard(shard);
        return 0;
    }
    mkey.mv_data = linkstr;
    mkey.mv_size = len;
    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret != 0)
        goto end_exit;
    do {
        count++;
        if (count > 1)
            break;
    } while (0 == mdb_cursor_get(cur, &mkey,
                         &mdata, MDB_NEXT_DUP));
end_exit:
    mdb_cursor_close(cur);
    release_bdb_lock_shard(shard);
    EFUNC;
    return count;
}


/* === Complex cursor functions === */

DDSTAT *dnode_bname_to_inode(void *dinode,
                             int dlen,
                             char *bname)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;
    DAT *statdata;
    DDSTAT *filestat = NULL, *lfilestat;
    DAT *fname;
    DINOINO dinoino;
    unsigned long long *valnode;
    unsigned long long inode;
    int shard;
#ifdef ENABLE_CRYPTO
    DAT *decrypted;
#endif

    FUNC;
    memcpy(&inode, dinode,
           sizeof(unsigned long long));
    shard = inode_to_shard(inode);
    bdb_lock_shard(shard,
        (char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn[shard],
                          dbi_dbdirent[shard],
                          &cur);
    if (ret != 0) {
        release_bdb_lock_shard(shard);
        return NULL;
    }

    mkey.mv_data = &inode;
    mkey.mv_size = sizeof(unsigned long long);
    mdata.mv_size = 0;
    mdata.mv_data = NULL;

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret != 0) {
        LDEBUG(
            "dnode_bname_to_inode: not found %s",
            mdb_strerror(ret));
        goto end;
    }
    do {
        valnode =
            (unsigned long long *) mdata.mv_data;
        if (inode == *valnode && inode != 1)
            continue;
        LDEBUG("Search for inode %llu", *valnode);
        statdata = dbp_get_within_lock(
                       *valnode, shard);
        if (NULL == statdata) {
            LINFO("Unable to find file in dbp.");
            continue;
        }
#ifdef ENABLE_CRYPTO
        if (config->encryptmeta
                && config->encryptdata) {
            decrypted = lfsdecrypt(statdata);
            DATfree(statdata);
            statdata = decrypted;
        }
#endif
        filestat = (DDSTAT *) statdata->data;
        if (0 != filestat->filename[0]) {
            s_free(statdata);
            LDEBUG("compare bname %s with %s",
                   bname, filestat->filename);
            if (0 == strcmp(bname,
                            filestat->filename))
            {
                break;
            }
        } else {
            memcpy(&dinoino.dirnode, dinode,
                   sizeof(unsigned long long));
            dinoino.inode =
                filestat->stbuf.st_ino;
            fname = dbl_keyval_within_lock(
                        dinoino.inode, shard,
                        &dinoino, sizeof(DINOINO),
                        bname, strlen(bname));
            if (fname) {
                lfilestat =
                    s_zmalloc(sizeof(DDSTAT));
                memcpy(lfilestat,
                       statdata->data,
                       statdata->size);
                s_free(statdata);
                ddstatfree(filestat);
                memcpy(&lfilestat->filename,
                       fname->data,
                       fname->size);
                lfilestat->filename[
                    fname->size] = 0;
                filestat = lfilestat;
                DATfree(fname);
                break;
            } else {
                s_free(statdata);
            }
        }
        ddstatfree(filestat);
        filestat = NULL;
    } while (0 == mdb_cursor_get(cur, &mkey,
                         &mdata, MDB_NEXT_DUP));
end:
    mdb_cursor_close(cur);
    release_bdb_lock_shard(shard);
    if (filestat) {
        LDEBUG(
            "dnode_bname_to_inode: %s inode %llu",
            filestat->filename,
            (unsigned long long)
            filestat->stbuf.st_ino);
    }
    return filestat;
}
unsigned long long has_nodes(
    unsigned long long inode)
{
    unsigned long long res = 0;
    unsigned long long *filenode;
    DAT *filedata;
    bool dotdir = 0;
    DDSTAT *ddstat;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret, shard;

    FUNC;
    shard = inode_to_shard(inode);
    bdb_lock_shard(shard,
        (char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn[shard],
                          dbi_dbdirent[shard],
                          &cur);
    if (ret != 0) {
        release_bdb_lock_shard(shard);
        return 0;
    }

    mkey.mv_data = &inode;
    mkey.mv_size = sizeof(inode);

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret != 0)
        goto end;
    do {
        filenode =
            (unsigned long long *) mdata.mv_data;
        filedata = dbp_get_within_lock(
                       *filenode, shard);
        if (filedata) {
            ddstat = value_to_ddstat(filedata);
            DATfree(filedata);
            if (*filenode == inode)
                dotdir = 1;
            if (ddstat->filename) {
                if (0 == strcmp(
                        ddstat->filename, "."))
                    dotdir = 1;
                if (0 == strcmp(
                        ddstat->filename, ".."))
                    dotdir = 1;
            }
            if (!dotdir) {
                res++;
            }
            ddstatfree(ddstat);
            dotdir = 0;
        }
    } while (0 == mdb_cursor_get(cur, &mkey,
                         &mdata, MDB_NEXT_DUP));
end:
    mdb_cursor_close(cur);
    release_bdb_lock_shard(shard);
    LDEBUG(
        "has_nodes inode %llu contains %llu files",
        inode, res);
    EFUNC;
    return res;
}
int fs_readdir(const char *path, void *buf,
               fuse_fill_dir_t filler,
               off_t offset,
               struct fuse_file_info *fi)
{
    int retcode = 0;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret, shard;
    struct stat stbuf;
    DAT *filedata;
    DDSTAT *ddstat;

    FUNC;
    (void) offset;
    (void) fi;

    ret = dbstat(path, &stbuf, 1);
    if (0 != ret)
        return -ENOENT;
    if (0 == strcmp(path, "/.lessfs/locks"))
        locks_to_dir(buf, filler, fi);

    shard = inode_to_shard(
                (unsigned long long)stbuf.st_ino);
    bdb_lock_shard(shard,
        (char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn[shard],
                          dbi_dbdirent[shard],
                          &cur);
    if (ret != 0) {
        release_bdb_lock_shard(shard);
        return -EIO;
    }

    mkey.mv_data = &stbuf.st_ino;
    mkey.mv_size = sizeof(unsigned long long);

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret == 0) {
        do {
            if (0 != memcmp(
                    mdata.mv_data,
                    &stbuf.st_ino,
                    sizeof(unsigned long long)))
            {
                unsigned long long child_ino;
                memcpy(&child_ino,
                       mdata.mv_data,
                       sizeof(unsigned long long));
                filedata = dbp_get_within_lock(
                               child_ino, shard);
                if (filedata) {
                    ddstat =
                        value_to_ddstat(filedata);
                    DATfree(filedata);
                    if (ddstat->filename[0] == 0) {
                        fs_read_hardlink(stbuf,
                            ddstat, buf, filler,
                            fi, shard);
                    } else {
                        fil_fuse_info(ddstat, buf,
                                      filler, fi);
                    }
                    ddstatfree(ddstat);
                }
            }
        } while (0 == mdb_cursor_get(cur, &mkey,
                         &mdata, MDB_NEXT_DUP));
    }
    mdb_cursor_close(cur);
    release_bdb_lock_shard(shard);
    LDEBUG("fs_readdir: return");
    return retcode;
}
void fs_read_hardlink(struct stat stbuf,
                      DDSTAT *ddstat,
                      void *buf,
                      fuse_fill_dir_t filler,
                      struct fuse_file_info *fi,
                      int held_shard)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    DINOINO dinoino;
    int ret;
    unsigned long long file_ino;
    int shard;
    MDB_txn *txn;
    MDB_txn *rtxn = NULL;

    FUNC;
    dinoino.dirnode = stbuf.st_ino;
    dinoino.inode = ddstat->stbuf.st_ino;
    file_ino = (unsigned long long)
                   ddstat->stbuf.st_ino;
    shard = inode_to_shard(file_ino);

    if (shard == held_shard) {
        txn = write_txn[shard];
    } else {
        ret = mdb_txn_begin(lmdb_env[shard],
                            NULL, MDB_RDONLY,
                            &rtxn);
        if (ret != 0)
            return;
        txn = rtxn;
    }

    ret = mdb_cursor_open(txn,
                          dbi_dbl[shard], &cur);
    if (ret != 0) {
        if (rtxn)
            mdb_txn_abort(rtxn);
        return;
    }

    mkey.mv_data = &dinoino;
    mkey.mv_size = sizeof(DINOINO);

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret == 0) {
        do {
            memcpy(&ddstat->filename,
                   mdata.mv_data,
                   mdata.mv_size);
            ddstat->filename[mdata.mv_size] = 0;
            LDEBUG(
                "fs_read_hardlink: %s size %zu",
                ddstat->filename,
                mdata.mv_size);
            fil_fuse_info(ddstat, buf, filler,
                          fi);
            ddstat->filename[0] = 0;
        } while (0 == mdb_cursor_get(cur, &mkey,
                         &mdata, MDB_NEXT_DUP));
    }
    mdb_cursor_close(cur);
    if (rtxn)
        mdb_txn_abort(rtxn);
    EFUNC;
}
/* count_dirlinks: shard-0 version kept for
 * compatibility. Use count_dirlinks_inode for
 * sharded DBL lookups. */
int count_dirlinks(void *linkstr, int len)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int count = 0, ret;

    FUNC;
    bdb_lock_shard(0,
        (char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn[0],
                          dbi_dbl[0], &cur);
    if (ret != 0) {
        release_bdb_lock_shard(0);
        return 0;
    }

    mkey.mv_data = linkstr;
    mkey.mv_size = len;
    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret != 0)
        goto end_exit;
    do {
        count++;
        if (count > 1)
            break;
    } while (0 == mdb_cursor_get(cur, &mkey,
                         &mdata, MDB_NEXT_DUP));
end_exit:
    mdb_cursor_close(cur);
    release_bdb_lock_shard(0);
    EFUNC;
    return count;
}

unsigned long long get_offset_fast(
    unsigned long long mbytes)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;
    bool found = 0;
    FREEBLOCK *freeblock = NULL;
    time_t thetime;
    unsigned long long offset = 0;
    unsigned long long fsize;

    FUNC;
    thetime = time(NULL);
    bdb_lock_shard(0,
        (char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn[0], dbi_freelist[0],
                          &cur);
    if (ret != 0) {
        release_bdb_lock();
        return 0;
    }

    mkey.mv_data = &mbytes;
    mkey.mv_size = sizeof(mbytes);

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret == 0) {
        do {
            if (mdata.mv_size < sizeof(FREEBLOCK)) {
                memcpy(&offset, mdata.mv_data,
                       mdata.mv_size);
            } else {
                freeblock =
                    s_malloc(sizeof(FREEBLOCK));
                memcpy(freeblock, mdata.mv_data,
                       mdata.mv_size);
                if (freeblock->reuseafter > thetime) {
                    s_free(freeblock);
                    freeblock = NULL;
                    continue;
                }
                offset = freeblock->offset;
            }
            memcpy(&fsize, mkey.mv_data, mkey.mv_size);
            mdb_cursor_del(cur, 0);
            found = 1;
            break;
        } while (0 == mdb_cursor_get(cur, &mkey,
                         &mdata, MDB_NEXT_DUP));
    }
    mdb_cursor_close(cur);
    release_bdb_lock_shard(0);
    if (found) {
        if (config->replication == 1
                && config->replication_role == 0) {
            if (freeblock) {
                write_repl_data(FREELIST,
                    REPLDELETECURKEY,
                    (char *) &fsize,
                    sizeof(unsigned long long),
                    (char *) freeblock,
                    sizeof(FREEBLOCK),
                    MAX_ALLOWED_THREADS - 2);
            } else {
                write_repl_data(FREELIST,
                    REPLDELETECURKEY,
                    (char *) &fsize,
                    sizeof(unsigned long long),
                    (char *) &offset,
                    sizeof(unsigned long long),
                    MAX_ALLOWED_THREADS - 2);
            }
        }
        if (freeblock)
            s_free(freeblock);
        EFUNC;
        return offset;
    }
    if (freeblock)
        s_free(freeblock);
    EFUNC;
    return 0;
}

INUSE *get_offset_reclaim(unsigned long long mbytes,
                          unsigned long long offset)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;
    FREEBLOCK *freeblock = NULL;
    time_t thetime;
    bool hasone = 0;
    unsigned long long asize;
    INUSE *inuse = NULL;

    FUNC;
    thetime = time(NULL);
    bdb_lock_shard(0,
        (char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn[0], dbi_freelist[0],
                          &cur);
    if (ret != 0) {
        release_bdb_lock();
        return NULL;
    }

    mkey.mv_data = &mbytes;
    mkey.mv_size = sizeof(mbytes);

    LDEBUG("get_offset_reclaim: search %llu blocks",
           mbytes);

    while (1) {
        ret = mdb_cursor_get(cur, &mkey, &mdata,
                             MDB_SET_RANGE);
        while (ret == 0) {
            if (mkey.mv_size == strlen("TRNCTD")) {
                ret = mdb_cursor_get(cur, &mkey,
                          &mdata, MDB_NEXT);
                continue;
            }
            memcpy(&asize, mkey.mv_data,
                   sizeof(unsigned long long));
            if (asize < mbytes) {
                ret = mdb_cursor_get(cur, &mkey,
                          &mdata, MDB_NEXT);
                continue;
            }
            if (mdata.mv_size < sizeof(FREEBLOCK)) {
                memcpy(&offset, mdata.mv_data,
                       mdata.mv_size);
            } else {
                freeblock =
                    s_malloc(sizeof(FREEBLOCK));
                memcpy(freeblock, mdata.mv_data,
                       mdata.mv_size);
                if (freeblock->reuseafter > thetime
                        && hasone == 0) {
                    s_free(freeblock);
                    freeblock = NULL;
                    hasone = 1;
                    ret = mdb_cursor_get(cur, &mkey,
                              &mdata, MDB_NEXT);
                    continue;
                }
                if (freeblock->reuseafter > thetime
                        && hasone == 1)
                    LINFO("get_offset_reclaim:"
                          " early space reclaim");
                offset = freeblock->offset;
            }
            inuse = s_zmalloc(sizeof(INUSE));
            inuse->allocated_size = asize * 512;
            inuse->offset = offset;
            mdb_cursor_del(cur, 0);
            break;
        }
        if (inuse)
            break;
        if (!hasone)
            break;
        /* Retry from beginning */
        mkey.mv_data = &mbytes;
        mkey.mv_size = sizeof(mbytes);
        hasone = 0;
    }
    mdb_cursor_close(cur);
    release_bdb_lock_shard(0);
    if (inuse) {
        if (config->replication == 1
                && config->replication_role == 0) {
            if (freeblock) {
                write_repl_data(FREELIST,
                    REPLDELETECURKEY,
                    (char *) &asize,
                    sizeof(unsigned long long),
                    (char *) freeblock,
                    sizeof(FREEBLOCK),
                    MAX_ALLOWED_THREADS - 2);
            } else {
                write_repl_data(FREELIST,
                    REPLDELETECURKEY,
                    (char *) &asize,
                    sizeof(unsigned long long),
                    (char *) &offset,
                    sizeof(unsigned long long),
                    MAX_ALLOWED_THREADS - 2);
            }
        }
    }
    if (freeblock)
        s_free(freeblock);
    return inuse;
}

void bdb_restart_truncation(void)
{
    char *keystr = "TRNCTD";
    pthread_t truncate_thread;
    struct truncate_thread_data *trunc_data;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;

    FUNC;
    if (!config->background_delete)
        return;

    bdb_lock_shard(0,
        (char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(write_txn[0],
                          dbi_freelist[0],
                          &cur);
    if (ret != 0) {
        release_bdb_lock_shard(0);
        return;
    }

    mkey.mv_data = keystr;
    mkey.mv_size = strlen(keystr);

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret != 0)
        goto end_exit;
    do {
        trunc_data = s_malloc(mdata.mv_size);
        memcpy(trunc_data, mdata.mv_data,
               mdata.mv_size);
        if (0 == trunc_data->inode) {
            s_free(trunc_data);
            break;
        }
        LINFO("Resume truncation of inode %llu",
              trunc_data->inode);
        create_inode_note(trunc_data->inode);
        if (0 != pthread_create(&truncate_thread,
                NULL, file_truncate_worker,
                (void *) trunc_data))
            die_syserr();
        if (0 != pthread_detach(truncate_thread))
            die_syserr();
    } while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                                 MDB_NEXT_DUP));
end_exit:
    mdb_cursor_close(cur);
    release_bdb_lock_shard(0);
    EFUNC;
}


/* === List / diagnostic functions === */

char *lessfs_stats()
{
    char *lfsmsg = NULL;
    char *line;
    DDSTAT *ddstat;
    DAT data;
    unsigned long long inode;
    char *nfi = "NFI";
    char *seq = "SEQ";
    CRYPTO *crypto;
    const char **lines = NULL;
    int count = 1;
    unsigned int lcount = 0;
    float ratio;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    MDB_stat mst;
    int ret, shard;

    /* Count total entries across all shards */
    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        bdb_lock_shard(shard,
            (char *) __PRETTY_FUNCTION__);
        mdb_stat(write_txn[shard],
                 dbi_dbp[shard], &mst);
        lcount += mst.ms_entries;
        release_bdb_lock_shard(shard);
    }
    lcount++;
    lines = s_malloc(lcount * sizeof(char *));
    lines[0] =
        as_sprintf(__FILE__, __LINE__,
            "  INODE             SIZE"
            "  COMPRESSED_SIZE"
            "            RATIO FILENAME\n");

    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        bdb_lock_shard(shard,
            (char *) __PRETTY_FUNCTION__);
        ret = mdb_cursor_open(write_txn[shard],
                              dbi_dbp[shard], &cur);
        if (ret != 0) {
            release_bdb_lock_shard(shard);
            continue;
        }
        while (0 == mdb_cursor_get(cur, &mkey,
                                   &mdata,
                                   MDB_NEXT)) {
            if (0 != memcmp(mkey.mv_data, nfi, 3)
                    && 0 != memcmp(mkey.mv_data,
                                   seq, 3)) {
                memcpy(&inode, mkey.mv_data,
                       sizeof(unsigned long long));
                data.data = mdata.mv_data;
                data.size = mdata.mv_size;
                if (inode == 0) {
                    crypto =
                        (CRYPTO *) mdata.mv_data;
                } else {
                    ddstat = value_to_ddstat(&data);
                    ratio = 0;
                    if (ddstat->stbuf.st_size
                            != 0) {
                        if (ddstat->real_size
                                == 0) {
                            ratio = 1000;
                        } else {
                            ratio = (float)
                                ddstat->stbuf
                                    .st_size
                                / (float)
                                ddstat->real_size;
                        }
                    }
                    if (S_ISREG(
                            ddstat->stbuf
                                .st_mode)) {
#ifdef x86_64
                        line = as_sprintf(
                            __FILE__, __LINE__,
                            "%7lu  %15lu"
                            "  %15llu"
                            "  %15.2f %s\n",
                            ddstat->stbuf.st_ino,
                            ddstat->stbuf.st_size,
                            ddstat->real_size,
                            ratio,
                            ddstat->filename);
#else
                        line = as_sprintf(
                            __FILE__, __LINE__,
                            "%7llu  %15llu"
                            "  %15llu"
                            "  %15.2f %s\n",
                            ddstat->stbuf.st_ino,
                            ddstat->stbuf.st_size,
                            ddstat->real_size,
                            ratio,
                            ddstat->filename);
#endif
                        lines[count++] = line;
                    } else {
                        lcount--;
                    }
                    ddstatfree(ddstat);
                }
            } else {
                lcount--;
            }
            if (count == lcount)
                break;
        }
        mdb_cursor_close(cur);
        release_bdb_lock_shard(shard);
    }
    lfsmsg = as_strarrcat(lines, count);
    while (count) {
        s_free((char *) lines[--count]);
    }
    s_free(lines);
    EFUNC;
    return lfsmsg;
}
void listdbp()
{
    DDSTAT *ddstat;
    unsigned long long *inode;
    char *nfi = "NFI";
    char *seq = "SEQ";
    CRYPTO *crypto;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    DAT tmp_data;
    int count = 0, ret, shard;

    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        bdb_lock_shard(shard,
            (char *) __PRETTY_FUNCTION__);
        ret = mdb_cursor_open(write_txn[shard],
                              dbi_dbp[shard], &cur);
        if (ret != 0) {
            release_bdb_lock_shard(shard);
            continue;
        }
        while (0 == mdb_cursor_get(cur, &mkey,
                                   &mdata,
                                   MDB_NEXT)) {
            count++;
            inode =
              (unsigned long long *)mkey.mv_data;
            if (0 == memcmp(mkey.mv_data, nfi, 3)
                    || 0 == memcmp(mkey.mv_data,
                                   seq, 3)) {
                inode = (unsigned long long *)
                            mdata.mv_data;
                printf("NFI : %llu\n", *inode);
            } else {
                tmp_data.data = mdata.mv_data;
                tmp_data.size = mdata.mv_size;
                if (*inode == 0) {
                    crypto = (CRYPTO *)
                                 mdata.mv_data;
                } else {
                    ddstat = value_to_ddstat(
                                 &tmp_data);
#ifdef x86_64
                    printf(
                      "ddstat->filename %s \n"
                      "      ->inode %lu  "
                      "-> size %lu  "
                      "-> real_size %llu "
                      "time %lu mode %u\n",
                      ddstat->filename,
                      ddstat->stbuf.st_ino,
                      ddstat->stbuf.st_size,
                      ddstat->real_size,
                      ddstat->stbuf.st_atim
                          .tv_sec,
                      ddstat->stbuf.st_mode);
#else
                    printf(
                      "ddstat->filename %s \n"
                      "      ->inode %llu "
                      "-> size %llu "
                      "-> real_size %llu "
                      "time %lu mode %u\n",
                      ddstat->filename,
                      ddstat->stbuf.st_ino,
                      ddstat->stbuf.st_size,
                      ddstat->real_size,
                      ddstat->stbuf.st_atim
                          .tv_sec,
                      ddstat->stbuf.st_mode);
#endif
                    if (S_ISDIR(
                            ddstat->stbuf
                                .st_mode)) {
                        printf(
                          "      ->filename %s"
                          " is a directory\n",
                          ddstat->filename);
                    }
                    ddstatfree(ddstat);
                }
            }
        }
        mdb_cursor_close(cur);
        release_bdb_lock_shard(shard);
    }
}
void listdirent()
{
    unsigned long long *dir;
    unsigned long long *ent;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret, shard;

    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        bdb_lock_shard(shard,
            (char *) __PRETTY_FUNCTION__);
        ret = mdb_cursor_open(
                  write_txn[shard],
                  dbi_dbdirent[shard], &cur);
        if (ret != 0) {
            release_bdb_lock_shard(shard);
            continue;
        }
        while (0 == mdb_cursor_get(cur, &mkey,
                                   &mdata,
                                   MDB_NEXT)) {
            dir =
              (unsigned long long *)mkey.mv_data;
            ent =
              (unsigned long long *)mdata.mv_data;
            printf("%llu:%llu\n", *dir, *ent);
        }
        mdb_cursor_close(cur);
        release_bdb_lock_shard(shard);
    }
}
void list_hardlinks()
{
    unsigned long long inode;
    DINOINO dinoino;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret, shard;

    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        bdb_lock_shard(shard,
            (char *) __PRETTY_FUNCTION__);
        ret = mdb_cursor_open(write_txn[shard],
                              dbi_dbl[shard],
                              &cur);
        if (ret != 0) {
            release_bdb_lock_shard(shard);
            continue;
        }
        while (0 == mdb_cursor_get(cur, &mkey,
                                   &mdata,
                                   MDB_NEXT)) {
            if (mkey.mv_size == sizeof(DINOINO)) {
                memcpy(&dinoino, mkey.mv_data,
                       sizeof(DINOINO));
                printf(
                    "dinoino %llu-%llu : "
                    "inode %s\n",
                    dinoino.dirnode,
                    dinoino.inode,
                    (char *) mdata.mv_data);
            } else {
                memcpy(&inode, mkey.mv_data,
                       sizeof(unsigned long long));
                memcpy(&dinoino, mdata.mv_data,
                       sizeof(DINOINO));
                printf(
                    "inode %llu : %llu-%llu "
                    "dinoino\n", inode,
                    dinoino.dirnode,
                    dinoino.inode);
            }
        }
        mdb_cursor_close(cur);
        release_bdb_lock_shard(shard);
    }
}
void listdbb()
{
    char *asc_hash = NULL;
    unsigned long long inode;
    unsigned long long blocknr;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret, shard;

    for (shard = 0;
         shard < config->lmdb_db_count; shard++) {
        bdb_lock_shard(shard,
            (char *) __PRETTY_FUNCTION__);
        ret = mdb_cursor_open(write_txn[shard],
                              dbi_dbb[shard],
                              &cur);
        if (ret != 0) {
            release_bdb_lock_shard(shard);
            continue;
        }
        while (0 == mdb_cursor_get(cur, &mkey,
                                   &mdata,
                                   MDB_NEXT)) {
            asc_hash = ascii_hash(
                (unsigned char *)mdata.mv_data);
            memcpy(&inode, mkey.mv_data,
                   sizeof(unsigned long long));
            memcpy(&blocknr,
                   mkey.mv_data
                       + sizeof(unsigned long long),
                   sizeof(unsigned long long));
            printf("%llu-%llu : %s\n",
                   inode, blocknr, asc_hash);
            s_free(asc_hash);
        }
        mdb_cursor_close(cur);
        release_bdb_lock_shard(shard);
    }
}
void listfree(int freespace_summary)
{
    unsigned long long mbytes;
    unsigned long long offset;
    unsigned long freespace = 0;
    struct truncate_thread_data *trunc_data;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    FREEBLOCK *freeblock;
    int ret;

    bdb_lock_shard(0,
        (char *) __PRETTY_FUNCTION__);
    ret = mdb_cursor_open(write_txn[0],
                          dbi_freelist[0], &cur);
    if (ret != 0) {
        release_bdb_lock_shard(0);
        return;
    }
    while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                               MDB_NEXT)) {
        if (mkey.mv_size == strlen("TRNCTD")) {
            trunc_data =
                (struct truncate_thread_data *)
                mdata.mv_data;
            printf("Truncation not finished for "
                   "inode %llu : start %llu "
                   "-> end %llu size %llu\n",
                   trunc_data->inode,
                   trunc_data->blocknr,
                   trunc_data->lastblocknr,
                   (unsigned long long)
                   trunc_data->stbuf.st_size);
        } else {
            memcpy(&mbytes, mkey.mv_data,
                   sizeof(unsigned long long));
            freespace += (mbytes * 512);
            if (mdata.mv_size
                    < sizeof(FREEBLOCK)) {
                memcpy(&offset, mdata.mv_data,
                   sizeof(unsigned long long));
                if (!freespace_summary) {
                    printf("offset = %llu :"
                           " blocks = %llu :"
                           " bytes = %llu\n",
                           offset, mbytes,
                           mbytes * 512);
                }
            } else {
                freeblock =
                    (FREEBLOCK *) mdata.mv_data;
                if (!freespace_summary) {
                    printf("offset = %llu :"
                           " blocks = %llu :"
                           " bytes = %llu"
                           " reuseafter %lu\n",
                           freeblock->offset,
                           mbytes, mbytes * 512,
                           freeblock->reuseafter);
                }
            }
        }
    }
    printf("Total available space in %s : %lu\n\n",
           config->blockdata, freespace);
    mdb_cursor_close(cur);
    release_bdb_lock_shard(0);
}

void listdbu()
{
}

void listdta()
{
}

void list_symlinks()
{
}

void flistdbu()
{
    char *asc_hash = NULL;
    INUSE *inuse;
    unsigned long long nexto;
    unsigned long rsize;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;

    bdb_lock_shard(0,
        (char *) __PRETTY_FUNCTION__);
    ret = mdb_cursor_open(write_txn[0],
                          dbi_dbu[0], &cur);
    if (ret != 0) {
        release_bdb_lock_shard(0);
        return;
    }
    while (0 == mdb_cursor_get(cur, &mkey, &mdata,
                               MDB_NEXT)) {
        if (config->nexthash && 0 == memcmp(config->nexthash,
                        mkey.mv_data,
                        config->hashlen)) {
            memcpy(&nexto, mdata.mv_data,
                   sizeof(unsigned long long));
            printf("\nnextoffset = %llu\n\n", nexto);
        } else {
            inuse = (INUSE *) mdata.mv_data;
            asc_hash = ascii_hash(
                (unsigned char *) mkey.mv_data);
            printf("%s   : %llu\n",
                   asc_hash, inuse->inuse);
            printf("offset"
                   "                 "
                   "              : %llu\n",
                   inuse->offset);
            printf("size"
                   "                 "
                   "        : %lu\n",
                   inuse->size);
            rsize = inuse->allocated_size;
            printf("allocated size"
                   "                 "
                   "  : %lu\n\n", rsize);
            s_free(asc_hash);
            if (inuse->allocated_size - 512
                    > inuse->size)
                printf("Reclaimed oversized"
                       " %lu - %llu\n",
                       inuse->size,
                       inuse->allocated_size);
        }
    }
    mdb_cursor_close(cur);
    release_bdb_lock_shard(0);
}


/*
 * inode_to_path: reconstruct the full path for an
 * inode by walking up the directory tree via DBDIRENT.
 * Returns a freshly allocated path string, or NULL
 * if not found.  Caller must s_free() the result.
 *
 * Works by: for each inode, look up its DDSTAT to get
 * the filename, then find which parent directory
 * contains it via DBDIRENT reverse lookup.
 */
char *inode_to_path(unsigned long long inode)
{
    char *components[256];
    int depth = 0;
    unsigned long long cur_ino = inode;
    DAT *dskdata;
    DDSTAT *ddstat;
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret, shard;
    unsigned long long parent_ino;
    int found;
    char *result;
    int total_len;
    int idx;

    FUNC;
    if (cur_ino == 1)
        return s_strdup("/");

    while (cur_ino != 1 && depth < 255) {
        dskdata = search_inode_dbdata(DBP,
            cur_ino, &cur_ino,
            sizeof(unsigned long long), true);
        if (dskdata == NULL)
            goto fail;
        ddstat = value_to_ddstat(dskdata);
        DATfree(dskdata);

        if (strcmp(ddstat->filename, "/") == 0) {
            ddstatfree(ddstat);
            break;
        }
        if (ddstat->filename[0] == 0) {
            DAT *lnk = btsearch_inode_keyval(DBL,
                cur_ino, &cur_ino,
                sizeof(unsigned long long),
                NULL, 0, true);
            if (lnk) {
                DINOINO dino;
                DAT *nm;
                memcpy(&dino, lnk->data,
                       lnk->size);
                DATfree(lnk);
                nm = btsearch_inode_keyval(DBL,
                    dino.inode, &dino,
                    sizeof(DINOINO),
                    NULL, 0, true);
                if (nm) {
                    components[depth] =
                        s_zmalloc(nm->size + 1);
                    memcpy(components[depth],
                           nm->data, nm->size);
                    depth++;
                    DATfree(nm);
                    ddstatfree(ddstat);
                } else {
                    ddstatfree(ddstat);
                    goto fail;
                }
            } else {
                ddstatfree(ddstat);
                goto fail;
            }
        } else {
            components[depth] =
                s_strdup(ddstat->filename);
            depth++;
            ddstatfree(ddstat);
        }

        /* Find parent: scan all shards' DBDIRENT */
        found = 0;
        for (shard = 0;
             shard < config->lmdb_db_count
             && !found; shard++) {
            bdb_lock_shard(shard,
                (char *) __PRETTY_FUNCTION__);
            ret = mdb_cursor_open(
                      write_txn[shard],
                      dbi_dbdirent[shard], &cur);
            if (ret != 0) {
                release_bdb_lock_shard(shard);
                goto fail;
            }
            ret = mdb_cursor_get(cur, &mkey,
                                 &mdata,
                                 MDB_FIRST);
            while (ret == 0) {
                if (mdata.mv_size
                    == sizeof(unsigned long long)){
                    unsigned long long child_ino;
                    memcpy(&child_ino,
                           mdata.mv_data,
                           sizeof(
                             unsigned long long));
                    if (child_ino == cur_ino) {
                        unsigned long long key_ino;
                        memcpy(&key_ino,
                               mkey.mv_data,
                               sizeof(
                                 unsigned
                                 long long));
                        if (key_ino != cur_ino) {
                            parent_ino = key_ino;
                            found = 1;
                            break;
                        }
                    }
                }
                ret = mdb_cursor_get(cur, &mkey,
                                     &mdata,
                                     MDB_NEXT);
            }
            mdb_cursor_close(cur);
            release_bdb_lock_shard(shard);
        }

        if (!found)
            break;
        cur_ino = parent_ino;
    }

    if (depth == 0)
        return s_strdup("/");

    total_len = 1;
    for (idx = depth - 1; idx >= 0; idx--)
        total_len +=
            strlen(components[idx]) + 1;

    result = s_malloc(total_len + 1);
    result[0] = 0;
    for (idx = depth - 1; idx >= 0; idx--) {
        strcat(result, "/");
        strcat(result, components[idx]);
        s_free(components[idx]);
    }

    EFUNC;
    return result;

fail:
    for (idx = 0; idx < depth; idx++)
        s_free(components[idx]);
    return NULL;
}

/*
 * fs_readdir_ll: low-level readdir for the LMDB
 * backend.  Fills buf using fuse_add_direntry().
 * Returns the number of bytes used in buf.
 */
size_t fs_readdir_ll(fuse_req_t req,
                     unsigned long long dir_ino,
                     char *buf, size_t bufsize,
                     off_t off)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret;
    DAT *filedata;
    DDSTAT *ddstat;
    struct stat stbuf;
    size_t buf_used = 0;
    size_t entsize;
    off_t idx = 0;
    char *bname;
    int dir_shard = inode_to_shard(dir_ino);

    FUNC;
    bdb_lock_shard(dir_shard,
        (char *) __PRETTY_FUNCTION__);

    ret = mdb_cursor_open(
              write_txn[dir_shard],
              dbi_dbdirent[dir_shard], &cur);
    if (ret != 0) {
        release_bdb_lock_shard(dir_shard);
        return 0;
    }

    mkey.mv_data = &dir_ino;
    mkey.mv_size = sizeof(unsigned long long);

    ret = mdb_cursor_get(cur, &mkey, &mdata,
                         MDB_SET_KEY);
    if (ret == 0) {
        do {
            unsigned long long child_ino;
            memcpy(&child_ino, mdata.mv_data,
                   sizeof(unsigned long long));

            if (child_ino == dir_ino)
                goto next_entry;

            /* Release dir_shard lock to
             * call search_inode_dbdata (which
             * acquires the child's shard lock).
             */
            mdb_cursor_close(cur);
            release_bdb_lock_shard(dir_shard);

            filedata = search_inode_dbdata(DBP,
                child_ino, &child_ino,
                sizeof(unsigned long long),
                true);

            /* Re-acquire dir_shard lock
             * and cursor position.
             */
            bdb_lock_shard(dir_shard,
                (char *) __PRETTY_FUNCTION__);
            ret = mdb_cursor_open(
                      write_txn[dir_shard],
                      dbi_dbdirent[dir_shard],
                      &cur);
            if (ret != 0) {
                release_bdb_lock_shard(
                    dir_shard);
                if (filedata) DATfree(filedata);
                break;
            }
            mkey.mv_data = &dir_ino;
            mkey.mv_size =
                sizeof(unsigned long long);
            /* Restore position */
            mdata.mv_data = &child_ino;
            mdata.mv_size =
                sizeof(unsigned long long);
            if (mdb_cursor_get(cur, &mkey,
                               &mdata,
                               MDB_GET_BOTH)
                    != 0) {
                if (filedata) DATfree(filedata);
                break;
            }

            if (filedata == NULL)
                goto next_entry;

            ddstat = value_to_ddstat(filedata);
            DATfree(filedata);

            bname = s_basename(ddstat->filename);
            if (bname == NULL
                || strcmp(bname, "/") == 0) {
                if (bname)
                    s_free(bname);
                ddstatfree(ddstat);
                goto next_entry;
            }

            if (idx < off) {
                idx++;
                s_free(bname);
                ddstatfree(ddstat);
                goto next_entry;
            }

            memcpy(&stbuf, &ddstat->stbuf,
                   sizeof(struct stat));
            entsize = fuse_add_direntry(
                req, buf + buf_used,
                bufsize - buf_used,
                bname, &stbuf, idx + 1);

            s_free(bname);

            if (ddstat->filename[0] == 0) {
                DINOINO dinoino;
                DAT *lnk;

                dinoino.dirnode = dir_ino;
                dinoino.inode =
                    ddstat->stbuf.st_ino;

                mdb_cursor_close(cur);
                release_bdb_lock_shard(
                    dir_shard);

                lnk = btsearch_inode_keyval(DBL,
                    dinoino.inode, &dinoino,
                    sizeof(DINOINO),
                    NULL, 0, true);

                bdb_lock_shard(dir_shard,
                    (char *) __PRETTY_FUNCTION__);
                ret = mdb_cursor_open(
                          write_txn[dir_shard],
                          dbi_dbdirent[dir_shard],
                          &cur);
                if (ret != 0) {
                    if (lnk) DATfree(lnk);
                    ddstatfree(ddstat);
                    release_bdb_lock_shard(
                        dir_shard);
                    break;
                }

                /* Reposition cursor at current
                 * entry so MDB_NEXT_DUP works
                 * after the hardlink lookup.
                 */
                mkey.mv_data = &dir_ino;
                mkey.mv_size =
                    sizeof(unsigned long long);
                mdata.mv_data = &child_ino;
                mdata.mv_size =
                    sizeof(unsigned long long);
                if (mdb_cursor_get(cur, &mkey,
                                   &mdata,
                                   MDB_GET_BOTH)
                        != 0) {
                    if (lnk) DATfree(lnk);
                    ddstatfree(ddstat);
                    release_bdb_lock_shard(
                        dir_shard);
                    break;
                }

                if (lnk) {
                    char linkname[257];
                    size_t nlen =
                        lnk->size < 256
                        ? lnk->size : 256;
                    memcpy(linkname,
                           lnk->data, nlen);
                    linkname[nlen] = 0;
                    DATfree(lnk);
                    buf_used -= entsize;
                    entsize = fuse_add_direntry(
                        req, buf + buf_used,
                        bufsize - buf_used,
                        linkname, &stbuf,
                        idx + 1);
                }
            }
            ddstatfree(ddstat);

            if (entsize > bufsize - buf_used)
                break;

            buf_used += entsize;
            idx++;

          next_entry:
            ;
        } while (0 == mdb_cursor_get(cur, &mkey,
                         &mdata, MDB_NEXT_DUP));
    }
    mdb_cursor_close(cur);
    release_bdb_lock_shard(dir_shard);
    EFUNC;
    return buf_used;
}

#endif /* LMDB */

/* ============================================================
 *  FLEX_COMP: load persisted per-directory policies
 * ============================================================ */
#ifdef LMDB
void load_policies_from_db(void)
{
    MDB_cursor *cur;
    MDB_val mkey, mdata;
    int ret, shard;
    unsigned long long dir_inode;
    DIR_POLICY pol;
    int count = 0;

    /*
     * Scan all shards for keys starting with
     * "DIRPOL" in the DBP database.  The key is
     * 14 bytes: "DIRPOL" (6) + inode (8).
     */
    for (shard = 0; shard < config->lmdb_db_count;
         shard++) {
        bdb_lock_shard(shard,
            (char *) __PRETTY_FUNCTION__);
        ret = mdb_cursor_open(
                  write_txn[shard],
                  dbi_dbp[shard], &cur);
        if (ret != 0) {
            release_bdb_lock_shard(shard);
            continue;
        }
        ret = mdb_cursor_get(cur, &mkey, &mdata,
                             MDB_FIRST);
        while (ret == 0) {
            if (mkey.mv_size == 14
                && 0 == memcmp(mkey.mv_data,
                               "DIRPOL", 6)
                && mdata.mv_size
                   >= sizeof(DIR_POLICY)) {
                memcpy(&dir_inode,
                       (char *) mkey.mv_data + 6,
                       sizeof(unsigned long long));
                memcpy(&pol, mdata.mv_data,
                       sizeof(DIR_POLICY));
                pthread_mutex_lock(&policy_mutex);
                tctreeput(dir_policy_tree,
                          &dir_inode,
                          sizeof(
                            unsigned long long),
                          &pol,
                          sizeof(DIR_POLICY));
                pthread_mutex_unlock(&policy_mutex);
                count++;
                LINFO("FLEX_COMP: loaded policy "
                      "for dir inode %llu "
                      "(comp=%s dedup=%u)",
                      dir_inode,
                      compression_char_to_name(
                          pol.compression),
                      (unsigned) pol.deduplication);
            }
            ret = mdb_cursor_get(cur, &mkey, &mdata,
                                 MDB_NEXT);
        }
        mdb_cursor_close(cur);
        release_bdb_lock_shard(shard);
    }
    LINFO("FLEX_COMP: loaded %d directory policies",
          count);
}
#else
void load_policies_from_db(void)
{
    LINFO("FLEX_COMP: policy loading not "
          "implemented for this backend");
}
#endif

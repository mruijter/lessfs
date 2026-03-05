/*
 *   Lessfs: A data deduplicating filesystem.
 *   Copyright (C) 2008 Mark Ruijter <mruijter@lessfs.com>
 *
 *   Low-level FUSE API implementation.
 *
 *   This program is s_free software.
 *   You can redistribute lessfs and/or modify it under
 *   the terms of either (1) the GNU General Public License;
 *   either version 3 of the License, or (at your option) any
 *   later version as published by the Free Software Foundation;
 *   or (2) obtain a commercial license by contacting the Author.
 *
 *   This program is distributed in the hope that it will be
 *   useful, but WITHOUT ANY WARRANTY; without even the implied
 *   warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 *   PURPOSE.  See the GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public
 *   License along with this program; if not, write to the
 *   Free Software Foundation, Inc., 59 Temple Place, Suite 330,
 *   Boston, MA 02111-1307 USA
 */

#define FUSE_USE_VERSION 29
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifndef LFATAL
#include "lib_log.h"
#endif

#define LFSVERSION "1.7.0"

#include <fuse.h>
#include <fuse/fuse_lowlevel.h>
#include <fuse/fuse_opt.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <malloc.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/file.h>
#include <sys/un.h>
#include <stdlib.h>
#include <pthread.h>
#include <signal.h>
#include <libgen.h>
#include <sys/utsname.h>
#include <sys/vfs.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "lib_tc_replacements.h"
#include <stdbool.h>
#include "lib_safe.h"
#include "lib_common.h"
#ifdef LZO
#include "lib_lzo.h"
#endif
#include "lib_qlz.h"
#include "lib_qlz15.h"
#ifdef BERKELEYDB
#include <db.h>
#include "lib_bdb.h"
#elif defined(LMDB)
#include <lmdb.h>
#include "lib_lmdb.h"
#endif
#include "lib_net.h"
#include "file_io.h"

#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include "lib_cfg.h"
#include "lib_str.h"
#include "lib_repl.h"
#include "retcodes.h"
#ifdef ENABLE_CRYPTO
#include "lib_crypto.h"
#endif

#include "commons.h"
#ifdef BERKELEYDB
extern char *bdb_lockedby;
#elif defined(LMDB)
extern char *bdb_lockedby;
#endif


/* Maximum filename component length (POSIX NAME_MAX) */
#define LESSFS_NAME_MAX 255
unsigned long working = 0;
unsigned long sequence = 0;

/* Tree for deferred deletion: inode -> struct stat
   When a file with nlink==1 is unlinked while still
   open, we remove the dirent but defer data deletion
   until the last fd is closed (ll_release). */
static TCTREE *deferred_tree;
static pthread_mutex_t deferred_mutex =
    PTHREAD_MUTEX_INITIALIZER;

/* Default attribute/entry timeouts in seconds */
#define ENTRY_TIMEOUT 1.0
#define ATTR_TIMEOUT  1.0

/* Forward declarations for threads reused from lessfs.c */
void *housekeeping_worker(void *arg);
void *ioctl_worker(void *arg);
void *init_worker(void *arg);
void *lessfs_flush(void *arg);

extern int max_threads;

void segvExit()
{
    LFATAL("Exit caused by segfault!, exitting\n");
    db_close(0);
    exit(EXIT_OK);
}

void normalExit()
{
    LFATAL("Exit signal received, exitting\n");
    db_close(0);
    exit(EXIT_OK);
}

void libSafeExit()
{
    db_close(0);
    LFATAL("Exit signal received from lib_safe, exitting\n");
    exit(EXIT_OK);
}

void dbsync()
{
    if (config->blockdata_io_type == FILE_IO) {
        fsync(fdbdta);
    }
    if (config->replication == 1
        && config->replication_role == 0) {
        fsync(frepl);
    }
}

unsigned long long get_real_size(unsigned long long inode)
{
    DAT *data;
    DDSTAT *ddstat;
    unsigned long long real_size = 0;

    data = search_inode_dbdata(DBP, inode, &inode,
                    sizeof(unsigned long long), LOCK);
    if (NULL == data) {
        return (real_size);
    }
    ddstat = value_to_ddstat(data);
    DATfree(data);
    real_size = ddstat->real_size;
    ddstatfree(ddstat);
    return (real_size);
}

CCACHEDTA *update_stored(unsigned char *hash, INOBNO * inobno,
                         off_t offsetblock)
{
    DAT *data;
    DAT *uncompdata;
    CCACHEDTA *ccachedta;

    ccachedta = s_zmalloc(sizeof(CCACHEDTA));
    ccachedta->creationtime = time(NULL);
    ccachedta->dirty = 1;
    ccachedta->pending = 0;
    ccachedta->newblock = 0;

    data = search_dbdata(DBDTA, hash, config->hashlen, LOCK);
    if (NULL == data) {
        s_free(ccachedta);
        return NULL;
    }
    uncompdata = lfsdecompress(data);
    LDEBUG("got uncompsize : %lu", uncompdata->size);
    memcpy(&ccachedta->data, uncompdata->data, uncompdata->size);
    ccachedta->datasize = uncompdata->size;
    ccachedta->updated = data->size;
    DATfree(uncompdata);
    DATfree(data);
    db_delete_stored(inobno);
    EFUNC;
    return ccachedta;
}


void add2cache(INOBNO * inobno, const char *buf, off_t offsetblock,
               size_t bsize)
{
    CCACHEDTA *ccachedta;
    uintptr_t p;
    int size;
    char *key;
    DAT *data;
    bool found = 0;


  pending:
    write_lock((char *) __PRETTY_FUNCTION__);
    key =
        (char *) tctreeget(readcachetree, (void *) inobno, sizeof(INOBNO),
                           &size);
    if (key) {
        memcpy(&p, key, size);
        ccachedta = get_ccachedta(p);
        while (ccachedta->pending == 1) {
            release_write_lock();
            goto pending;
        }
        ccachedta->creationtime = time(NULL);
        ccachedta->dirty = 1;
        memcpy((void *) &ccachedta->data[offsetblock], buf, bsize);
        if (ccachedta->datasize < offsetblock + bsize)
            ccachedta->datasize = offsetblock + bsize;
        update_filesize(inobno->inode, bsize, offsetblock,
                        inobno->blocknr);
        release_write_lock();
        return;
    }
    update_filesize(inobno->inode, bsize, offsetblock, inobno->blocknr);
    if (bsize < BLKSIZE) {
        data = check_block_exists(inobno);
        if (data) {
            create_hash_note(data->data);
            ccachedta =
                file_update_stored(data->data, inobno, offsetblock);
            delete_hash_note(data->data);
            if (ccachedta) {
                memcpy(&ccachedta->data[offsetblock], buf, bsize);
                if (ccachedta->datasize < offsetblock + bsize)
                    ccachedta->datasize = offsetblock + bsize;
                found = 1;
            }
            DATfree(data);
        }
    }

    if (!found) {
        ccachedta = s_zmalloc(sizeof(CCACHEDTA));
        memcpy(&ccachedta->data[offsetblock], buf, bsize);
        ccachedta->datasize = offsetblock + bsize;
        ccachedta->creationtime = time(NULL);
        ccachedta->dirty = 1;
        ccachedta->pending = 0;
        ccachedta->newblock = 1;
    }

    if (tctreernum(workqtree) * 2 > config->cachesize ||
        tctreernum(readcachetree) * 2 > config->cachesize) {
        flush_wait(0);
        purge_read_cache(0, 1, (char *) __PRETTY_FUNCTION__);
    }
    p = (uintptr_t) ccachedta;
// A full block can be processed right away.
    if (bsize == BLKSIZE) {
        ccachedta->pending = 1;
        tctreeput(workqtree, (void *) inobno, sizeof(INOBNO), (void *) &p,
                  sizeof(p));
    }
// When this is not a full block a separate thread is used.
    tctreeput(readcachetree, (void *) inobno, sizeof(INOBNO), (void *) &p,
              sizeof(p));
    release_write_lock();
    return;
}

void flush_rotate_replog()
{
   write_lock((char *) __PRETTY_FUNCTION__);
   flush_wait(0);
   purge_read_cache(0, 1, (char *) __PRETTY_FUNCTION__);
   if ( config->blockdata_io_type == CHUNK_IO ) sync();
   start_flush_commit();
   end_flush_commit();
   config->replication_last_rotated = time(NULL);
   rotate_replog();
   release_repl_lock();
   release_write_lock();
}

/*
 * Helper: fill a fuse_entry_param from a struct stat.
 */
static void fill_entry(struct fuse_entry_param *entry,
                       struct stat *stbuf)
{
    memset(entry, 0, sizeof(*entry));
    entry->ino = stbuf->st_ino;
    entry->generation = 1;
    memcpy(&entry->attr, stbuf, sizeof(struct stat));
    entry->attr_timeout = ATTR_TIMEOUT;
    entry->entry_timeout = ENTRY_TIMEOUT;
}

/*
 * Helper: fetch stat for an inode from metatree or
 * on-disk database.  Returns 0 on success, -errno on
 * failure.
 */
static int stat_inode(unsigned long long inode,
                      struct stat *stbuf)
{
    const char *data;
    int vsize;
    MEMDDSTAT *memddstat;
    DAT *dskdata;
    DDSTAT *ddstat;

    /* Try metatree cache first */
    meta_lock((char *) __PRETTY_FUNCTION__);
    data = tctreeget(metatree, &inode,
                     sizeof(unsigned long long), &vsize);
    if (data) {
        memddstat = (MEMDDSTAT *) data;
        memcpy(stbuf, &memddstat->stbuf,
               sizeof(struct stat));
        release_meta_lock();
        return 0;
    }
    release_meta_lock();

    /* Fall back to on-disk database */
    dskdata = search_inode_dbdata(DBP, inode,
                            &inode,
                            sizeof(unsigned long long),
                            LOCK);
    if (dskdata == NULL)
        return -ENOENT;
    ddstat = value_to_ddstat(dskdata);
    memcpy(stbuf, &ddstat->stbuf, sizeof(struct stat));
    DATfree(dskdata);
    ddstatfree(ddstat);
    return 0;
}

/* ============================================================
 *  FUSE low-level operations
 * ============================================================ */

static void ll_init(void *userdata,
                    struct fuse_conn_info *conn)
{
    unsigned int count = 0;
    unsigned int *cnt;
    int ret;
    unsigned char *stiger;
    char *hashstr;
    INUSE *finuse;
    struct tm *timeinfo = NULL;

    FUNC;
#ifdef LZO
    initlzo();
#endif
    if (getenv("MAX_THREADS"))
        max_threads = atoi(getenv("MAX_THREADS"));
    if (max_threads > MAX_ALLOWED_THREADS - 1)
        die_dataerr(
            "Configuration error : "
            "MAX_ALLOWED_THREADS should be less then %i",
            MAX_ALLOWED_THREADS - 1);
    if (max_threads < 1)
        max_threads = 1;
    pthread_t worker_thread[max_threads];
    pthread_t ioctl_thread;
    pthread_t replication_thread;
    pthread_t housekeeping_thread;
    pthread_t flush_thread;

    for (count = 0; count < max_threads; count++) {
        cnt = s_malloc(sizeof(int));
        memcpy(cnt, &count, sizeof(int));
        ret = pthread_create(&(worker_thread[count]),
                             NULL, init_worker,
                             (void *) cnt);
        if (ret != 0)
            die_syserr();
        if (0 != pthread_detach(worker_thread[count]))
            die_syserr();
    }
    ret = pthread_create(&ioctl_thread, NULL,
                         ioctl_worker, (void *) NULL);
    if (ret != 0)
        die_syserr();
    if (0 != pthread_detach(ioctl_thread))
        die_syserr();

    if (config->replication) {
        if (config->replication_role == 1) {
            ret = pthread_create(&replication_thread,
                                 NULL,
                                 replication_worker,
                                 (void *) NULL);
            if (ret != 0)
                die_syserr();
            if (0 != pthread_detach(replication_thread))
                die_syserr();
        }
    }

    ret = pthread_create(&housekeeping_thread, NULL,
                         housekeeping_worker,
                         (void *) NULL);
    if (ret != 0)
        die_syserr();
    if (0 != pthread_detach(housekeeping_thread))
        die_syserr();

    ret = pthread_create(&flush_thread, NULL,
                         lessfs_flush, (void *) NULL);
    if (ret != 0)
        die_syserr();
    if (0 != pthread_detach(flush_thread))
        die_syserr();
    check_blocksize();
    hashstr =
        as_sprintf(__FILE__, __LINE__, "%s%i",
                   config->hash, config->hashlen);
    stiger =
        thash((unsigned char *) hashstr,
              strlen(hashstr));
    if (NULL == (finuse = file_get_inuse(stiger))) {
        die_dataerr(
            "Invalid hashsize or hash found, "
            "do not change hash or hashsize after "
            "formatting lessfs.");
    } else
        s_free(finuse);
    s_free(hashstr);
    free(stiger);

    timeinfo = init_transactions();

    if (check_dirty()) {
        LINFO("Lessfs has not been unmounted cleanly.");
        if (config->transactions) {
            LINFO("Rollback to : %s",
                  asctime(timeinfo));
            write_repl_data(WRITTEN,
                            TRANSACTIONABORT,
                            " ", 1, NULL, 0,
                            MAX_ALLOWED_THREADS - 2);
        } else
            LINFO(
                "Lessfs has not been unmounted "
                "cleanly, you are advised to run "
                "lessfsck.");
    } else {
        LINFO("The filesystem is clean.");
        if (config->transactions) {
            LINFO("Last used at : %s",
                  asctime(timeinfo));
        }
        mark_dirty();
    }
    config->lfsstats = s_zmalloc(2);
    if (config->replication != 1
        || config->replication_role != 1) {
#ifdef BERKELEYDB
        bdb_restart_truncation();
#elif defined(LMDB)
        bdb_restart_truncation();
#endif
    }
    /* FLEX_COMP: load per-directory policies */
    load_all_policies();

    EFUNC;
}

static void ll_destroy(void *userdata)
{
    FUNC;
    LINFO("Lessfs is going down, please wait");
    config->shutdown = 1;
    if (config->replication_enabled) {
        LFATAL(
            "Wait for active replication threads "
            "to shutdown");
        while (!config->safe_down) {
            sleep(1);
        }
        LFATAL(
            "No active replication threads, "
            "going down");
        sleep(1);
    }
    get_global_lock((char *) __PRETTY_FUNCTION__);
    write_lock((char *) __PRETTY_FUNCTION__);
    purge_read_cache(0, 1,
                     (char *) __PRETTY_FUNCTION__);
    flush_wait(0);
    if (0 != tctreernum(workqtree)
        || 0 != tctreernum(readcachetree))
        LFATAL(
            "Going down with data in queue, "
            "this should never happen");
    release_write_lock();
    release_global_lock();
    config->replication = 0;
    if (config->transactions)
        lessfs_trans_stamp();
    clear_dirty();
    db_close(0);
    s_free(config->lfsstats);
#ifdef ENABLE_CRYPTO
    if (config->encryptdata) {
        s_free(config->passwd);
        s_free(config->iv);
    }
#endif
    if (config->transactions)
        free(config->commithash);
    if (config->replication_watchdir)
        s_free(config->replication_watchdir);
    s_free(config);
#ifdef MEMTRACE
    leak_report();
#endif
    LFATAL("Lessfs is down");
    EFUNC;
}

/*
 * lookup: resolve name in parent directory.
 */

/* ============================================================
 *  FLEX_COMP: synthetic .lessfs_policy helpers
 * ============================================================ */

/*
 * is_toplevel_dir_ll: lightweight check using
 * DBDIRENT — is this dir a direct child of root?
 */
static int is_toplevel_dir_ll(
    unsigned long long inode)
{
    return is_toplevel_dir(inode);
}

/*
 * fill_policy_stat: populate a stat structure for
 * a synthetic policy inode.
 */
static void fill_policy_stat(
    unsigned long long synth_ino,
    struct stat *stbuf)
{
    memset(stbuf, 0, sizeof(struct stat));
    stbuf->st_ino = synth_ino;
    stbuf->st_dev = 999988;
    stbuf->st_uid = 0;
    stbuf->st_gid = 0;
    stbuf->st_ctim.tv_sec = time(NULL);
    stbuf->st_mtim.tv_sec = stbuf->st_ctim.tv_sec;
    stbuf->st_atim.tv_sec = stbuf->st_ctim.tv_sec;

    if (POLICY_TYPE(synth_ino) == SYNTH_DIR) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        stbuf->st_size = 4096;
    } else {
        stbuf->st_mode = S_IFREG | 0644;
        stbuf->st_nlink = 1;
        stbuf->st_size = 16; /* approximate */
    }
}

static void ll_lookup(fuse_req_t req,
                      fuse_ino_t parent,
                      const char *name)
{
    DDSTAT *ddstat;
    struct fuse_entry_param entry;
    unsigned long long parent_ino = parent;

    FUNC;
    if (strlen(name) > LESSFS_NAME_MAX) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }

    /* FLEX_COMP: intercept .lessfs_policy lookups */
    if (IS_POLICY_INO(parent_ino)
        && POLICY_TYPE(parent_ino) == SYNTH_DIR) {
        /* Looking up a child of .lessfs_policy */
        unsigned long long real_dir =
            POLICY_PARENT_INO(parent_ino);
        struct stat stbuf;
        unsigned long long child_ino = 0;

        if (0 == strcmp(name, "compression")) {
            child_ino = MAKE_POLICY_INO(
                real_dir, SYNTH_COMP);
        } else if (0 == strcmp(name,
                               "deduplication")) {
            child_ino = MAKE_POLICY_INO(
                real_dir, SYNTH_DEDUP);
        } else if (0 == strcmp(name, ".")
                   || 0 == strcmp(name, "..")) {
            child_ino = parent_ino;
        }
        if (child_ino != 0) {
            memset(&entry, 0, sizeof(entry));
            fill_policy_stat(child_ino,
                             &entry.attr);
            entry.ino = child_ino;
            entry.attr_timeout = 1.0;
            entry.entry_timeout = 1.0;
            fuse_reply_entry(req, &entry);
            EFUNC;
            return;
        }
        fuse_reply_err(req, ENOENT);
        return;
    }

    if (0 == strcmp(name, LESSFS_POLICY_DIR)
        && !IS_POLICY_INO(parent_ino)) {
        /* Check if parent is a top-level dir */
        get_global_lock(
            (char *) __PRETTY_FUNCTION__);
        if (is_toplevel_dir_ll(parent_ino)) {
            unsigned long long synth_ino =
                MAKE_POLICY_INO(parent_ino,
                                SYNTH_DIR);
            release_global_lock();
            memset(&entry, 0, sizeof(entry));
            fill_policy_stat(synth_ino,
                             &entry.attr);
            entry.ino = synth_ino;
            entry.attr_timeout = 1.0;
            entry.entry_timeout = 1.0;
            fuse_reply_entry(req, &entry);
            EFUNC;
            return;
        }
        release_global_lock();
    }

    get_global_lock((char *) __PRETTY_FUNCTION__);

    ddstat = dnode_bname_to_inode(
        &parent_ino,
        sizeof(unsigned long long),
        (char *) name);
    if (ddstat == NULL) {
        release_global_lock();
        fuse_reply_err(req, ENOENT);
        return;
    }

    fill_entry(&entry, &ddstat->stbuf);
    ddstatfree(ddstat);
    release_global_lock();
    fuse_reply_entry(req, &entry);
    EFUNC;
}

/*
 * forget: kernel dropping inode reference.
 * We currently don't track lookup counts since
 * lessfs manages inode lifetime via the DB.
 */
static void ll_forget(fuse_req_t req,
                      fuse_ino_t ino,
                      unsigned long nlookup)
{
    (void) ino;
    (void) nlookup;
    fuse_reply_none(req);
}

/*
 * getattr
 */
static void ll_getattr(fuse_req_t req,
                       fuse_ino_t ino,
                       struct fuse_file_info *fi)
{
    struct stat stbuf;
    int res;
    unsigned long long inode;

    FUNC;
    memset(&stbuf, 0, sizeof(stbuf));

    /* FLEX_COMP: synthetic policy inodes */
    if (IS_POLICY_INO(ino)) {
        fill_policy_stat(ino, &stbuf);
        fuse_reply_attr(req, &stbuf, 1.0);
        EFUNC;
        return;
    }

    /*
     * If fi is set and fi->fh is valid, use it.
     * This covers the unlinked-but-open case
     * that was broken in the high-level API.
     */
    if (fi != NULL && fi->fh != 0)
        inode = fi->fh;
    else
        inode = ino;

    get_global_lock((char *) __PRETTY_FUNCTION__);
    res = stat_inode(inode, &stbuf);
    if (!inode_isnot_locked(stbuf.st_ino))
        stbuf.st_mode ^= S_ISVTX;
    release_global_lock();

    if (res != 0) {
        fuse_reply_err(req, -res);
        return;
    }
    fuse_reply_attr(req, &stbuf, ATTR_TIMEOUT);
    EFUNC;
}

/*
 * setattr: replaces chmod, chown, truncate,
 *          utimens from the high-level API.
 */
static void ll_setattr(fuse_req_t req,
                       fuse_ino_t ino,
                       struct stat *attr,
                       int to_set,
                       struct fuse_file_info *fi)
{
    struct stat stbuf;
    int res = 0;
    unsigned long long inode;
    time_t thetime;
    const char *data;
    int vsize;
    MEMDDSTAT *memddstat;
    DAT *ddbuf;
    DAT *dskdata;
    DDSTAT *ddstat;

    FUNC;

    /* FLEX_COMP: setattr on synthetic policy inodes
       is a no-op — just return the synthetic stat. */
    if (IS_POLICY_INO(ino)) {
        fill_policy_stat(ino, &stbuf);
        fuse_reply_attr(req, &stbuf, 1.0);
        EFUNC;
        return;
    }

    if (config->replication == 1
        && config->replication_role == 1) {
        fuse_reply_err(req, EPERM);
        return;
    }

    if (fi != NULL && fi->fh != 0)
        inode = fi->fh;
    else
        inode = ino;

    get_global_lock((char *) __PRETTY_FUNCTION__);
    thetime = time(NULL);

    res = stat_inode(inode, &stbuf);
    if (res != 0) {
        release_global_lock();
        fuse_reply_err(req, -res);
        return;
    }

    /* Handle truncate first — special path */
    if (to_set & FUSE_SET_ATTR_SIZE) {
        char *bname;
        struct stat *stptr;

        if (S_ISDIR(stbuf.st_mode)) {
            release_global_lock();
            fuse_reply_err(req, EISDIR);
            return;
        }
        stptr = s_malloc(sizeof(struct stat));
        memcpy(stptr, &stbuf, sizeof(struct stat));
        bname = as_sprintf(
            __FILE__, __LINE__, "%llu", inode);
        create_inode_note(stptr->st_ino);
        write_lock((char *) __PRETTY_FUNCTION__);
        flush_wait(stptr->st_ino);
        purge_read_cache(stptr->st_ino, 1,
            (char *) __PRETTY_FUNCTION__);
        if (attr->st_size < stptr->st_size) {
            res = file_fs_truncate(stptr,
                                   attr->st_size,
                                   bname, 0);
        } else {
            update_filesize_cache(stptr,
                                  attr->st_size);
            delete_inode_note(stptr->st_ino);
        }
        release_write_lock();
        s_free(stptr);
        s_free(bname);
        /* Re-fetch stat after truncate */
        stat_inode(inode, &stbuf);
    }

    /* Apply mode/uid/gid/time changes */
    meta_lock((char *) __PRETTY_FUNCTION__);
    data = tctreeget(metatree, &inode,
                     sizeof(unsigned long long),
                     &vsize);
    if (data) {
        memddstat = (MEMDDSTAT *) data;
        if (to_set & FUSE_SET_ATTR_MODE) {
            memddstat->stbuf.st_mode = attr->st_mode;
        }
        if (to_set & FUSE_SET_ATTR_UID)
            memddstat->stbuf.st_uid = attr->st_uid;
        if (to_set & FUSE_SET_ATTR_GID)
            memddstat->stbuf.st_gid = attr->st_gid;
        if (to_set & FUSE_SET_ATTR_ATIME)
            memddstat->stbuf.st_atim =
                attr->st_atim;
        if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
            memddstat->stbuf.st_atim.tv_sec =
                thetime;
            memddstat->stbuf.st_atim.tv_nsec = 0;
        }
        if (to_set & FUSE_SET_ATTR_MTIME)
            memddstat->stbuf.st_mtim =
                attr->st_mtim;
        if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
            memddstat->stbuf.st_mtim.tv_sec =
                thetime;
            memddstat->stbuf.st_mtim.tv_nsec = 0;
        }
        memddstat->stbuf.st_ctim.tv_sec = thetime;
        memddstat->stbuf.st_ctim.tv_nsec = 0;
        memddstat->updated = 1;
        /* Copy stbuf BEFORE tctreeput invalidates
           the memddstat pointer. */
        memcpy(&stbuf, &memddstat->stbuf,
               sizeof(struct stat));
        ddbuf = create_mem_ddbuf(memddstat);
        tctreeput(metatree, &inode,
                  sizeof(unsigned long long),
                  (void *) ddbuf->data,
                  ddbuf->size);
        release_meta_lock();
        DATfree(ddbuf);
    } else {
        release_meta_lock();
        /* No metatree entry: update on-disk */
        dskdata = search_inode_dbdata(DBP, inode,
                    &inode,
                    sizeof(unsigned long long),
                    LOCK);
        if (dskdata == NULL) {
            release_global_lock();
            fuse_reply_err(req, ENOENT);
            return;
        }
        ddstat = value_to_ddstat(dskdata);
        if (to_set & FUSE_SET_ATTR_MODE)
            ddstat->stbuf.st_mode = attr->st_mode;
        if (to_set & FUSE_SET_ATTR_UID)
            ddstat->stbuf.st_uid = attr->st_uid;
        if (to_set & FUSE_SET_ATTR_GID)
            ddstat->stbuf.st_gid = attr->st_gid;
        if (to_set & FUSE_SET_ATTR_ATIME)
            ddstat->stbuf.st_atim = attr->st_atim;
        if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
            ddstat->stbuf.st_atim.tv_sec = thetime;
            ddstat->stbuf.st_atim.tv_nsec = 0;
        }
        if (to_set & FUSE_SET_ATTR_MTIME)
            ddstat->stbuf.st_mtim = attr->st_mtim;
        if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
            ddstat->stbuf.st_mtim.tv_sec = thetime;
            ddstat->stbuf.st_mtim.tv_nsec = 0;
        }
        ddstat->stbuf.st_ctim.tv_sec = thetime;
        ddstat->stbuf.st_ctim.tv_nsec = 0;
        ddbuf = create_ddbuf(ddstat->stbuf,
                             ddstat->filename,
                             ddstat->real_size);
        bin_write_inode_dbdata(DBP, inode, &inode,
                         sizeof(unsigned long long),
                         (void *) ddbuf->data,
                         ddbuf->size);
        memcpy(&stbuf, &ddstat->stbuf,
               sizeof(struct stat));
        DATfree(dskdata);
        DATfree(ddbuf);
        ddstatfree(ddstat);
    }
    release_global_lock();
    fuse_reply_attr(req, &stbuf, ATTR_TIMEOUT);
    EFUNC;
}

/*
 * readlink
 */
static void ll_readlink(fuse_req_t req,
                        fuse_ino_t ino)
{
    DAT *data;
    unsigned long long inode = ino;

    FUNC;
    get_global_lock((char *) __PRETTY_FUNCTION__);
    data = search_dbdata(DBS, &inode,
                         sizeof(unsigned long long),
                         LOCK);
    release_global_lock();

    if (data == NULL) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    fuse_reply_readlink(req, (const char *) data->data);
    DATfree(data);
    EFUNC;
}

/*
 * mknod
 */
static void ll_mknod(fuse_req_t req,
                     fuse_ino_t parent,
                     const char *name,
                     mode_t mode, dev_t rdev)
{
    struct fuse_entry_param entry;
    struct stat stbuf;
    unsigned long long parent_ino = parent;
    DDSTAT *ddstat;
    char *fullpath;
    char *parentpath;
    time_t thetime;
    int res;

    FUNC;
    if (strlen(name) > LESSFS_NAME_MAX) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    if (config->replication == 1
        && config->replication_role == 1) {
        fuse_reply_err(req, EPERM);
        return;
    }

    get_global_lock((char *) __PRETTY_FUNCTION__);
    thetime = time(NULL);

    /*
     * We need a full path for dbmknod / write_file_ent.
     * Reconstruct it from parent inode + name.
     */
    parentpath = inode_to_path(parent_ino);
    if (parentpath == NULL) {
        release_global_lock();
        fuse_reply_err(req, ENOENT);
        return;
    }

    if (strcmp(parentpath, "/") == 0)
        fullpath = as_sprintf(__FILE__, __LINE__,
                              "/%s", name);
    else
        fullpath = as_sprintf(__FILE__, __LINE__,
                              "%s/%s", parentpath,
                              name);

    /* Reject if name already exists in parent */
    ddstat = dnode_bname_to_inode(
        &parent_ino,
        sizeof(unsigned long long),
        (char *) name);
    if (ddstat != NULL) {
        ddstatfree(ddstat);
        s_free(fullpath);
        s_free(parentpath);
        release_global_lock();
        fuse_reply_err(req, EEXIST);
        return;
    }

    { const struct fuse_ctx *rctx = fuse_req_ctx(req);
      ll_caller_uid = rctx->uid;
      ll_caller_gid = rctx->gid; }
    dbmknod(fullpath, mode, NULL, rdev);

    /* Update parent mtime/ctime */
    res = stat_inode(parent_ino, &stbuf);
    if (res == 0) {
        stbuf.st_ctim.tv_sec = thetime;
        stbuf.st_ctim.tv_nsec = 0;
        stbuf.st_mtim.tv_sec = thetime;
        stbuf.st_mtim.tv_nsec = 0;
        update_stat(parentpath, &stbuf);
    }

    /* Lookup the newly created file to reply */
    ddstat = dnode_bname_to_inode(
        &parent_ino,
        sizeof(unsigned long long),
        (char *) name);
    s_free(fullpath);
    s_free(parentpath);

    if (ddstat == NULL) {
        release_global_lock();
        fuse_reply_err(req, EIO);
        return;
    }

    fill_entry(&entry, &ddstat->stbuf);
    ddstatfree(ddstat);
    release_global_lock();
    fuse_reply_entry(req, &entry);
    EFUNC;
}


/*
 * create — atomic mknod + open for regular files.
 * Avoids the kernel fallback (mknod+lookup+open)
 * and prevents EIO when stale DBDIRENT entries
 * cause the post-mknod lookup to fail.
 */
static void ll_create(fuse_req_t req,
                      fuse_ino_t parent,
                      const char *name,
                      mode_t mode,
                      struct fuse_file_info *fi)
{
    struct fuse_entry_param entry;
    struct stat stbuf;
    unsigned long long parent_ino = parent;
    unsigned long long inode;
    DDSTAT *ddstat;
    char *fullpath;
    char *parentpath;
    char *bname;
    DAT *ddbuf;
    const char *dataptr;
    MEMDDSTAT *memddstat;
    int vsize;
    unsigned long long blocknr = 0;
    time_t thetime;
    int res;

    FUNC;
    if (strlen(name) > LESSFS_NAME_MAX) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }

    /* FLEX_COMP: intercept create inside a
       .lessfs_policy dir — treat as open since
       these virtual files always exist. */
    if (IS_POLICY_INO(parent_ino)
        && POLICY_TYPE(parent_ino) == SYNTH_DIR) {
        unsigned long long real_dir =
            POLICY_PARENT_INO(parent_ino);
        unsigned long long child_ino = 0;
        if (0 == strcmp(name, "compression"))
            child_ino = MAKE_POLICY_INO(
                real_dir, SYNTH_COMP);
        else if (0 == strcmp(name,
                             "deduplication"))
            child_ino = MAKE_POLICY_INO(
                real_dir, SYNTH_DEDUP);
        if (child_ino != 0) {
            memset(&entry, 0, sizeof(entry));
            fill_policy_stat(child_ino,
                             &entry.attr);
            entry.ino = child_ino;
            entry.attr_timeout = 1.0;
            entry.entry_timeout = 1.0;
            fi->fh = child_ino;
            fi->direct_io = 1;
            fuse_reply_create(req, &entry, fi);
            EFUNC;
            return;
        }
        fuse_reply_err(req, ENOENT);
        return;
    }

    if (config->replication == 1
        && config->replication_role == 1) {
        fuse_reply_err(req, EPERM);
        return;
    }

    get_global_lock(
        (char *) __PRETTY_FUNCTION__);
    thetime = time(NULL);

    parentpath = inode_to_path(parent_ino);
    if (parentpath == NULL) {
        release_global_lock();
        fuse_reply_err(req, ENOENT);
        return;
    }

    if (strcmp(parentpath, "/") == 0)
        fullpath = as_sprintf(
            __FILE__, __LINE__,
            "/%s", name);
    else
        fullpath = as_sprintf(
            __FILE__, __LINE__,
            "%s/%s", parentpath, name);

    { const struct fuse_ctx *rctx =
          fuse_req_ctx(req);
      ll_caller_uid = rctx->uid;
      ll_caller_gid = rctx->gid; }

    /* Check if file already exists */
    ddstat = dnode_bname_to_inode(
        &parent_ino,
        sizeof(unsigned long long),
        (char *) name);
    if (ddstat != NULL) {
        /* File exists */
        if (fi->flags & O_EXCL) {
            ddstatfree(ddstat);
            s_free(fullpath);
            s_free(parentpath);
            release_global_lock();
            fuse_reply_err(req, EEXIST);
            return;
        }
        /* O_TRUNC: reset size to 0 */
        if (fi->flags & O_TRUNC) {
            ddstat->stbuf.st_size = 0;
            update_stat(fullpath,
                        &ddstat->stbuf);
        }
        s_free(fullpath);
        s_free(parentpath);
    } else {
        /* File does not exist — create it */
        dbmknod(fullpath, mode, NULL, 0);

        /* Update parent mtime/ctime */
        res = stat_inode(parent_ino, &stbuf);
        if (res == 0) {
            stbuf.st_ctim.tv_sec = thetime;
            stbuf.st_ctim.tv_nsec = 0;
            stbuf.st_mtim.tv_sec = thetime;
            stbuf.st_mtim.tv_nsec = 0;
            update_stat(parentpath, &stbuf);
        }

        /* Lookup the newly created file */
        ddstat = dnode_bname_to_inode(
            &parent_ino,
            sizeof(unsigned long long),
            (char *) name);
        s_free(fullpath);
        s_free(parentpath);

        if (ddstat == NULL) {
            release_global_lock();
            fuse_reply_err(req, EIO);
            return;
        }
    }

    inode = ddstat->stbuf.st_ino;
    fill_entry(&entry, &ddstat->stbuf);

    /* Open: populate metatree cache */
    fi->fh = inode;
    bname = s_strdup(ddstat->filename);
    ddstatfree(ddstat);

    meta_lock(
        (char *) __PRETTY_FUNCTION__);
    blocknr--;
    memddstat = s_malloc(sizeof(MEMDDSTAT));
    memcpy(&memddstat->stbuf, &entry.attr,
           sizeof(struct stat));
    memcpy(&memddstat->filename, bname,
           strlen(bname) + 1);
    memddstat->blocknr = blocknr;
    memddstat->updated = 0;
    memddstat->real_size = 0;
    memddstat->opened = 1;
    memddstat->stbuf.st_atim.tv_sec =
        time(NULL);
    memddstat->stbuf.st_atim.tv_nsec = 0;
    ddbuf = create_mem_ddbuf(memddstat);
    tctreeput(metatree, &inode,
              sizeof(unsigned long long),
              (void *) ddbuf->data,
              ddbuf->size);
    DATfree(ddbuf);
    memddstatfree(memddstat);
    release_meta_lock();

    s_free(bname);
    release_global_lock();
    fuse_reply_create(req, &entry, fi);
    EFUNC;
}

/*
 * mkdir
 */
static void ll_mkdir(fuse_req_t req,
                     fuse_ino_t parent,
                     const char *name,
                     mode_t mode)
{
    struct fuse_entry_param entry;
    unsigned long long parent_ino = parent;
    DDSTAT *ddstat;
    char *fullpath;
    char *parentpath;
    int ret;

    FUNC;
    if (strlen(name) > LESSFS_NAME_MAX) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    if (config->replication == 1
        && config->replication_role == 1) {
        fuse_reply_err(req, EPERM);
        return;
    }

    get_global_lock((char *) __PRETTY_FUNCTION__);

    parentpath = inode_to_path(parent_ino);
    if (parentpath == NULL) {
        release_global_lock();
        fuse_reply_err(req, ENOENT);
        return;
    }

    if (strcmp(parentpath, "/") == 0)
        fullpath = as_sprintf(__FILE__, __LINE__,
                              "/%s", name);
    else
        fullpath = as_sprintf(__FILE__, __LINE__,
                              "%s/%s", parentpath,
                              name);

    { const struct fuse_ctx *rctx = fuse_req_ctx(req);
      ll_caller_uid = rctx->uid;
      ll_caller_gid = rctx->gid; }
    ret = fs_mkdir(fullpath, mode);
    if (ret != 0) {
        s_free(fullpath);
        s_free(parentpath);
        release_global_lock();
        fuse_reply_err(req, -ret);
        return;
    }

    ddstat = dnode_bname_to_inode(
        &parent_ino,
        sizeof(unsigned long long),
        (char *) name);
    s_free(fullpath);
    s_free(parentpath);

    if (ddstat == NULL) {
        release_global_lock();
        fuse_reply_err(req, EIO);
        return;
    }

    fill_entry(&entry, &ddstat->stbuf);
    ddstatfree(ddstat);
    release_global_lock();
    fuse_reply_entry(req, &entry);
    EFUNC;
}

/*
 * unlink
 */
/*
 * Complete deferred deletion for an inode
 * whose last link was removed while it was
 * still open.  Called when the last fd closes.
 * Caller MUST hold global_lock and write_lock.
 */
static void complete_deferred_delete(
    unsigned long long inode)
{
    int vsize;
    const char *data;
    struct stat saved_st;
    DAT *dskdata;
    DDSTAT *ddstat;

    FUNC;
    pthread_mutex_lock(&deferred_mutex);
    data = tctreeget(deferred_tree, &inode,
                     sizeof(unsigned long long),
                     &vsize);
    if (data == NULL) {
        pthread_mutex_unlock(&deferred_mutex);
        EFUNC;
        return;
    }
    memcpy(&saved_st, data,
           sizeof(struct stat));
    tctreeout(deferred_tree, &inode,
              sizeof(unsigned long long));
    pthread_mutex_unlock(&deferred_mutex);

    LINFO("complete_deferred_delete inode %llu",
          inode);

    /* Delete symlink target if applicable */
    if (S_ISLNK(saved_st.st_mode)) {
        delete_inode_key(DBS, inode, &inode,
                   sizeof(unsigned long long),
                   (char *) __PRETTY_FUNCTION__);
    }

    /* Truncate all data blocks */
    {
        char bname[32];
        snprintf(bname, sizeof(bname),
                 "%llu", inode);
        (void) file_fs_truncate(
            &saved_st, 0, bname, 1);
    }

    /* Remove metadata from DBP */
    delete_inode_key(DBP, inode, &inode,
               sizeof(unsigned long long),
               (char *) __PRETTY_FUNCTION__);
    EFUNC;
}

static void ll_unlink(fuse_req_t req,
                      fuse_ino_t parent,
                      const char *name)
{
    int res;
    struct stat stbuf;
    unsigned long long parent_ino = parent;
    unsigned long long inode;
    DDSTAT *ddstat;
    char *fullpath;
    char *parentpath;
    const char *dataptr;
    int vsize;
    int file_is_open = 0;

    FUNC;

    /* FLEX_COMP: virtual policy files — pretend
       removal succeeds so rm -rf works. */
    if (IS_POLICY_INO(parent_ino)) {
        fuse_reply_err(req, 0);
        return;
    }

    if (strlen(name) > LESSFS_NAME_MAX) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    if (config->replication == 1
        && config->replication_role == 1) {
        fuse_reply_err(req, EPERM);
        return;
    }

    get_global_lock(
        (char *) __PRETTY_FUNCTION__);

    /* Resolve child inode */
    ddstat = dnode_bname_to_inode(
        &parent_ino,
        sizeof(unsigned long long),
        (char *) name);
    if (ddstat == NULL) {
        release_global_lock();
        fuse_reply_err(req, ENOENT);
        return;
    }
    inode = ddstat->stbuf.st_ino;
    memcpy(&stbuf, &ddstat->stbuf,
           sizeof(struct stat));
    ddstatfree(ddstat);

    /* Check if file is still open */
    meta_lock((char *) __PRETTY_FUNCTION__);
    dataptr = tctreeget(metatree, &inode,
        sizeof(unsigned long long), &vsize);
    if (dataptr != NULL) {
        MEMDDSTAT *mds = (MEMDDSTAT *) dataptr;
        if (mds->opened > 0)
            file_is_open = 1;
    }
    release_meta_lock();

    if (stbuf.st_nlink == 1 && file_is_open) {
        /* Defer data deletion: remove dirent
           and update metadata but keep data. */
        struct stat dirst;
        char *dname;
        time_t thetime = time(NULL);
        DAT *dskdata;
        DDSTAT *ondisk;
        DAT *ddbuf;

        write_lock(
            (char *) __PRETTY_FUNCTION__);
        flush_wait(inode);
        purge_read_cache(inode, 1,
            (char *) __PRETTY_FUNCTION__);

        /* Update parent dir times */
        parentpath = inode_to_path(parent_ino);
        if (parentpath) {
            update_parent_time(parentpath, 0);
            s_free(parentpath);
        }

        /* Remove from directory listing */
        {
            struct stat pst;
            stat_inode(parent_ino, &pst);
            btdelete_inode_curkey(DBDIRENT,
                (unsigned long long) pst.st_ino,
                &pst.st_ino,
                sizeof(unsigned long long),
                &inode,
                sizeof(unsigned long long),
                (char *) __PRETTY_FUNCTION__);
        }

        /* Set nlink=0 in on-disk metadata */
        dskdata = search_inode_dbdata(DBP,
            inode, &inode,
            sizeof(unsigned long long), LOCK);
        if (dskdata) {
            ondisk = value_to_ddstat(dskdata);
            ondisk->stbuf.st_nlink = 0;
            ondisk->stbuf.st_ctim.tv_sec =
                thetime;
            ondisk->stbuf.st_ctim.tv_nsec = 0;
            ddbuf = create_ddbuf(
                ondisk->stbuf,
                ondisk->filename,
                ondisk->real_size);
            bin_write_inode_dbdata(DBP, inode, &inode,
                sizeof(unsigned long long),
                (void *) ddbuf->data,
                ddbuf->size);
            DATfree(ddbuf);
            DATfree(dskdata);
            ddstatfree(ondisk);
        }

        /* Update metatree cache: set nlink=0 */
        meta_lock(
            (char *) __PRETTY_FUNCTION__);
        dataptr = tctreeget(metatree, &inode,
            sizeof(unsigned long long), &vsize);
        if (dataptr) {
            MEMDDSTAT *mds =
                (MEMDDSTAT *) dataptr;
            mds->stbuf.st_nlink = 0;
            mds->stbuf.st_ctim.tv_sec =
                thetime;
            mds->stbuf.st_ctim.tv_nsec = 0;
            {
                DAT *mddbuf =
                    create_mem_ddbuf(mds);
                tctreeput(metatree, &inode,
                    sizeof(unsigned long long),
                    (void *) mddbuf->data,
                    mddbuf->size);
                DATfree(mddbuf);
            }
        }
        release_meta_lock();

        /* Add to deferred deletion tree */
        pthread_mutex_lock(&deferred_mutex);
        tctreeput(deferred_tree, &inode,
            sizeof(unsigned long long),
            &stbuf, sizeof(struct stat));
        pthread_mutex_unlock(&deferred_mutex);

        if (config->relax == 0)
            dbsync();
        release_write_lock();
        release_global_lock();

        fuse_reply_err(req, 0);
        EFUNC;
        return;
    }

    /* Normal path: file not open or nlink > 1 */
    parentpath = inode_to_path(parent_ino);
    if (parentpath == NULL) {
        release_global_lock();
        fuse_reply_err(req, ENOENT);
        return;
    }
    if (strcmp(parentpath, "/") == 0)
        fullpath = as_sprintf(__FILE__, __LINE__,
                              "/%s", name);
    else
        fullpath = as_sprintf(__FILE__, __LINE__,
                              "%s/%s", parentpath,
                              name);

    write_lock((char *) __PRETTY_FUNCTION__);
    flush_wait(inode);
    purge_read_cache(inode, 1,
        (char *) __PRETTY_FUNCTION__);
    meta_lock((char *) __PRETTY_FUNCTION__);
    update_filesize_onclose(inode);
    tctreeout(metatree, &inode,
              sizeof(unsigned long long));
    release_meta_lock();

    res = file_unlink_file(fullpath);
    if (config->relax == 0)
        dbsync();
    release_write_lock();

    s_free(fullpath);
    s_free(parentpath);
    release_global_lock();

    if (config->nospace == ENOSPC)
        config->nospace = -ENOSPC;

    fuse_reply_err(req, res < 0 ? -res : 0);
    EFUNC;
}

/*
 * rmdir
 */
static void ll_rmdir(fuse_req_t req,
                     fuse_ino_t parent,
                     const char *name)
{
    /* FLEX_COMP: virtual policy dir — pretend
       removal succeeds so rm -rf works. */
    if (IS_POLICY_INO(parent)
        || 0 == strcmp(name, LESSFS_POLICY_DIR)) {
        fuse_reply_err(req, 0);
        return;
    }

    int res;
    unsigned long long parent_ino = parent;
    char *fullpath;
    char *parentpath;

    FUNC;
    if (strlen(name) > LESSFS_NAME_MAX) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    if (config->replication == 1
        && config->replication_role == 1) {
        fuse_reply_err(req, EPERM);
        return;
    }

    get_global_lock((char *) __PRETTY_FUNCTION__);

    parentpath = inode_to_path(parent_ino);
    if (parentpath == NULL) {
        release_global_lock();
        fuse_reply_err(req, ENOENT);
        return;
    }
    if (strcmp(parentpath, "/") == 0)
        fullpath = as_sprintf(__FILE__, __LINE__,
                              "/%s", name);
    else
        fullpath = as_sprintf(__FILE__, __LINE__,
                              "%s/%s", parentpath,
                              name);

    res = fs_rmdir(fullpath);
    s_free(fullpath);
    s_free(parentpath);

    /* FLEX_COMP: if we just removed a top-level
       directory, delete its persisted policy. */
    if (res == 0) {
        DDSTAT *rm_dd = dnode_bname_to_inode(
            &parent_ino,
            sizeof(unsigned long long),
            (char *) name);
        if (rm_dd == NULL) {
            /* Already gone — look up by name
               is too late.  Use invalidate. */
            invalidate_policy_cache(0);
        } else {
            unsigned long long rm_ino =
                rm_dd->stbuf.st_ino;
            ddstatfree(rm_dd);
            { DIR_POLICY zero_pol = {0, 0}; set_dir_policy(rm_ino, &zero_pol); }
            invalidate_policy_cache(0);
        }
    }

    release_global_lock();

    fuse_reply_err(req, res < 0 ? -res : 0);
    EFUNC;
}

/*
 * symlink
 */
static void ll_symlink(fuse_req_t req,
                       const char *link,
                       fuse_ino_t parent,
                       const char *name)
{
    struct fuse_entry_param entry;
    unsigned long long parent_ino = parent;
    DDSTAT *ddstat;
    char *fullpath;
    char *parentpath;
    int res;

    FUNC;
    if (strlen(name) > LESSFS_NAME_MAX) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    get_global_lock((char *) __PRETTY_FUNCTION__);

    parentpath = inode_to_path(parent_ino);
    if (parentpath == NULL) {
        release_global_lock();
        fuse_reply_err(req, ENOENT);
        return;
    }
    if (strcmp(parentpath, "/") == 0)
        fullpath = as_sprintf(__FILE__, __LINE__,
                              "/%s", name);
    else
        fullpath = as_sprintf(__FILE__, __LINE__,
                              "%s/%s", parentpath,
                              name);

    { const struct fuse_ctx *rctx = fuse_req_ctx(req);
      ll_caller_uid = rctx->uid;
      ll_caller_gid = rctx->gid; }
    res = fs_symlink((char *) link, fullpath);
    if (res != 0) {
        s_free(fullpath);
        s_free(parentpath);
        release_global_lock();
        fuse_reply_err(req, -res);
        return;
    }

    ddstat = dnode_bname_to_inode(
        &parent_ino,
        sizeof(unsigned long long),
        (char *) name);
    s_free(fullpath);
    s_free(parentpath);

    if (ddstat == NULL) {
        release_global_lock();
        fuse_reply_err(req, EIO);
        return;
    }

    fill_entry(&entry, &ddstat->stbuf);
    ddstatfree(ddstat);
    release_global_lock();
    fuse_reply_entry(req, &entry);
    EFUNC;
}

/*
 * rename
 */
static void ll_rename(fuse_req_t req,
                      fuse_ino_t parent,
                      const char *name,
                      fuse_ino_t newparent,
                      const char *newname)
{
    int res;
    struct stat stbuf;
    unsigned long long pino = parent;
    unsigned long long npino = newparent;
    char *from_path, *to_path;
    char *from_parent, *to_parent;
    DDSTAT *ddstat;

    FUNC;
    if (strlen(name) > LESSFS_NAME_MAX) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    if (strlen(newname) > LESSFS_NAME_MAX) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    if (config->replication == 1
        && config->replication_role == 1) {
        fuse_reply_err(req, EPERM);
        return;
    }

    get_global_lock((char *) __PRETTY_FUNCTION__);

    /* Build full paths */
    from_parent = inode_to_path(pino);
    to_parent = inode_to_path(npino);
    if (from_parent == NULL || to_parent == NULL) {
        if (from_parent) s_free(from_parent);
        if (to_parent) s_free(to_parent);
        release_global_lock();
        fuse_reply_err(req, ENOENT);
        return;
    }

    if (strcmp(from_parent, "/") == 0)
        from_path = as_sprintf(__FILE__, __LINE__,
                               "/%s", name);
    else
        from_path = as_sprintf(__FILE__, __LINE__,
                               "%s/%s",
                               from_parent, name);

    if (strcmp(to_parent, "/") == 0)
        to_path = as_sprintf(__FILE__, __LINE__,
                             "/%s", newname);
    else
        to_path = as_sprintf(__FILE__, __LINE__,
                             "%s/%s",
                             to_parent, newname);

    /* Resolve the source */
    ddstat = dnode_bname_to_inode(
        &pino,
        sizeof(unsigned long long),
        (char *) name);
    if (ddstat == NULL) {
        s_free(from_path);
        s_free(to_path);
        s_free(from_parent);
        s_free(to_parent);
        release_global_lock();
        fuse_reply_err(req, ENOENT);
        return;
    }
    memcpy(&stbuf, &ddstat->stbuf,
           sizeof(struct stat));
    ddstatfree(ddstat);

    update_filesize_onclose(stbuf.st_ino);

    if (stbuf.st_nlink > 1
        && !S_ISDIR(stbuf.st_mode)) {
        res = fs_rename_link(from_path, to_path,
                             stbuf);
    } else {
        res = fs_rename(from_path, to_path, stbuf);
    }

    s_free(from_path);
    s_free(to_path);
    s_free(from_parent);
    s_free(to_parent);
    release_global_lock();

    fuse_reply_err(req, res < 0 ? -res : 0);
    EFUNC;
}

/*
 * link
 */
static void ll_link(fuse_req_t req,
                    fuse_ino_t ino,
                    fuse_ino_t newparent,
                    const char *newname)
{
    struct fuse_entry_param entry;
    unsigned long long src_ino = ino;
    unsigned long long np_ino = newparent;
    DDSTAT *ddstat;
    char *src_path, *dst_path;
    char *dst_parent;
    int res;

    FUNC;
    if (strlen(newname) > LESSFS_NAME_MAX) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    if (config->replication == 1
        && config->replication_role == 1) {
        fuse_reply_err(req, EPERM);
        return;
    }

    get_global_lock((char *) __PRETTY_FUNCTION__);

    src_path = inode_to_path(src_ino);
    dst_parent = inode_to_path(np_ino);
    if (src_path == NULL || dst_parent == NULL) {
        if (src_path) s_free(src_path);
        if (dst_parent) s_free(dst_parent);
        release_global_lock();
        fuse_reply_err(req, ENOENT);
        return;
    }

    if (strcmp(dst_parent, "/") == 0)
        dst_path = as_sprintf(__FILE__, __LINE__,
                              "/%s", newname);
    else
        dst_path = as_sprintf(__FILE__, __LINE__,
                              "%s/%s", dst_parent,
                              newname);

    res = fs_link(src_path, dst_path);

    if (res != 0) {
        s_free(src_path);
        s_free(dst_path);
        s_free(dst_parent);
        release_global_lock();
        fuse_reply_err(req, -res);
        return;
    }

    /* Re-read inode to get updated nlink */
    ddstat = dnode_bname_to_inode(
        &np_ino,
        sizeof(unsigned long long),
        (char *) newname);
    s_free(src_path);
    s_free(dst_path);
    s_free(dst_parent);

    if (ddstat == NULL) {
        release_global_lock();
        fuse_reply_err(req, EIO);
        return;
    }

    fill_entry(&entry, &ddstat->stbuf);
    ddstatfree(ddstat);
    release_global_lock();
    fuse_reply_entry(req, &entry);
    EFUNC;
}

/*
 * open
 */
static void ll_open(fuse_req_t req,
                    fuse_ino_t ino,
                    struct fuse_file_info *fi)
{
    struct stat stbuf;
    int res;
    unsigned long long inode = ino;

    /* FLEX_COMP: open on policy control files */
    if (IS_POLICY_INO(ino)) {
        fi->fh = ino;
        fi->direct_io = 1;
        fuse_reply_open(req, fi);
        return;
    }
    char *bname;
    DAT *ddbuf;
    const char *dataptr;
    MEMDDSTAT *memddstat;
    int vsize;
    unsigned long long blocknr = 0;

    FUNC;
    get_global_lock((char *) __PRETTY_FUNCTION__);
    res = stat_inode(inode, &stbuf);
    if (res != 0) {
        release_global_lock();
        fuse_reply_err(req, -res);
        return;
    }

    fi->fh = inode;

    /* Build bname from on-disk stat */
    {
        DAT *dsk = search_inode_dbdata(
            DBP, inode, &inode,
            sizeof(unsigned long long), LOCK);
        if (dsk) {
            DDSTAT *dd = value_to_ddstat(dsk);
            bname = s_strdup(dd->filename);
            DATfree(dsk);
            ddstatfree(dd);
        } else {
            bname = as_sprintf(__FILE__, __LINE__,
                               "%llu", inode);
        }
    }

    meta_lock((char *) __PRETTY_FUNCTION__);
    dataptr = tctreeget(metatree, &inode,
                        sizeof(unsigned long long),
                        &vsize);
    if (dataptr == NULL) {
        blocknr--;
        memddstat = s_malloc(sizeof(MEMDDSTAT));
        memcpy(&memddstat->stbuf, &stbuf,
               sizeof(struct stat));
        memcpy(&memddstat->filename, bname,
               strlen(bname) + 1);
        memddstat->blocknr = blocknr;
        memddstat->updated = 0;
        memddstat->real_size =
            get_real_size(stbuf.st_ino);
        memddstat->opened = 1;
        memddstat->stbuf.st_atim.tv_sec = time(NULL);
        memddstat->stbuf.st_atim.tv_nsec = 0;
        ddbuf = create_mem_ddbuf(memddstat);
        tctreeput(metatree, &inode,
                  sizeof(unsigned long long),
                  (void *) ddbuf->data,
                  ddbuf->size);
        DATfree(ddbuf);
        memddstatfree(memddstat);
    } else {
        memddstat = (MEMDDSTAT *) dataptr;
        memddstat->opened++;
        memddstat->stbuf.st_atim.tv_sec = time(NULL);
        memddstat->stbuf.st_atim.tv_nsec = 0;
        ddbuf = create_mem_ddbuf(memddstat);
        tctreeput(metatree, &inode,
                  sizeof(unsigned long long),
                  (void *) ddbuf->data,
                  ddbuf->size);
        DATfree(ddbuf);
    }
    release_meta_lock();
    s_free(bname);
    release_global_lock();
    fuse_reply_open(req, fi);
    EFUNC;
}

/*
 * read
 */
static void ll_read(fuse_req_t req,
                    fuse_ino_t ino,
                    size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
    unsigned long long blocknr;
    size_t done = 0;
    size_t got = 0;
    size_t block_offset = 0;
    struct stat stbuf;
    char *buf;

    FUNC;

    /* FLEX_COMP: read from policy control files */
    if (IS_POLICY_INO(fi->fh)) {
        unsigned long long real_dir =
            POLICY_PARENT_INO(fi->fh);
        unsigned int ptype =
            POLICY_TYPE(fi->fh);
        DIR_POLICY *pol;
        DIR_POLICY defpol;
        char valbuf[64];
        size_t vlen;

        defpol.compression =
            config->compression;
        defpol.deduplication =
            config->deduplication;
        pol = get_dir_policy(real_dir);
        if (NULL == pol)
            pol = &defpol;

        if (ptype == SYNTH_COMP) {
            snprintf(valbuf, sizeof(valbuf),
                     "%s\n",
                     compression_char_to_name(
                         pol->compression));
        } else if (ptype == SYNTH_DEDUP) {
            snprintf(valbuf, sizeof(valbuf),
                     "%u\n",
                     (unsigned) pol->deduplication);
        } else {
            fuse_reply_err(req, EINVAL);
            return;
        }
        vlen = strlen(valbuf);
        if ((size_t) offset >= vlen) {
            fuse_reply_buf(req, NULL, 0);
        } else {
            if (offset + size > vlen)
                size = vlen - offset;
            fuse_reply_buf(req,
                           valbuf + offset, size);
        }
        EFUNC;
        return;
    }

    get_global_lock((char *) __PRETTY_FUNCTION__);
    write_lock((char *) __PRETTY_FUNCTION__);

    buf = s_zmalloc(size);

    if (get_realsize_fromcache(fi->fh, &stbuf)) {
        if (offset + size > stbuf.st_size) {
            if (offset > stbuf.st_size) {
                release_write_lock();
                release_global_lock();
                fuse_reply_buf(req, buf, 0);
                s_free(buf);
                return;
            }
            size = stbuf.st_size - offset;
        }
    }
    while (done < size) {
        blocknr = (offset + done) / BLKSIZE;
        block_offset =
            (done + offset) - (blocknr * BLKSIZE);
        got = file_read_block(blocknr, buf + done,
                              size - done,
                              block_offset, fi->fh);
        done = done + BLKSIZE - block_offset;
        if (done > size)
            done = size;
    }
    release_write_lock();
    release_global_lock();
    fuse_reply_buf(req, buf, done);
    s_free(buf);
    EFUNC;
}

/*
 * write
 */
static void ll_write(fuse_req_t req,
                     fuse_ino_t ino,
                     const char *buf,
                     size_t size, off_t offset,
                     struct fuse_file_info *fi)
{
    off_t offsetblock;
    size_t bsize;
    size_t done = 0;
    INOBNO inobno;

    FUNC;

    /* FLEX_COMP: write to policy control files */
    if (IS_POLICY_INO(fi->fh)) {
        unsigned long long real_dir =
            POLICY_PARENT_INO(fi->fh);
        unsigned int ptype =
            POLICY_TYPE(fi->fh);
        DIR_POLICY pol;
        DIR_POLICY *existing;
        char valbuf[64];
        size_t cplen;

        /* Get current or default policy */
        existing = get_dir_policy(real_dir);
        if (existing) {
            memcpy(&pol, existing,
                   sizeof(DIR_POLICY));
        } else {
            pol.compression =
                config->compression;
            pol.deduplication =
                config->deduplication;
        }

        /* Copy and NUL-terminate the input */
        cplen = size;
        if (cplen >= sizeof(valbuf))
            cplen = sizeof(valbuf) - 1;
        memcpy(valbuf, buf, cplen);
        valbuf[cplen] = '\0';
        /* Strip trailing newline */
        if (cplen > 0
            && valbuf[cplen - 1] == '\n')
            valbuf[cplen - 1] = '\0';

        if (ptype == SYNTH_COMP) {
            unsigned char new_comp =
                compression_name_to_char(valbuf);
            /* Accept '1' as "use default" */
            if (0 == strcmp(valbuf, "1"))
                new_comp = config->compression;
            pol.compression = new_comp;
            LINFO("FLEX_COMP: dir inode %llu "
                  "compression -> %s",
                  real_dir,
                  compression_char_to_name(
                      new_comp));
        } else if (ptype == SYNTH_DEDUP) {
            if (valbuf[0] == '0')
                pol.deduplication = 0;
            else if (valbuf[0] == '1')
                pol.deduplication = 1;
            else {
                fuse_reply_err(req, EINVAL);
                return;
            }
            LINFO("FLEX_COMP: dir inode %llu "
                  "deduplication -> %u",
                  real_dir,
                  (unsigned) pol.deduplication);
        } else {
            fuse_reply_err(req, EINVAL);
            return;
        }

        set_dir_policy(real_dir, &pol);
        fuse_reply_write(req, size);
        EFUNC;
        return;
    }

    if (config->nospace == ENOSPC) {
        fuse_reply_err(req, ENOSPC);
        return;
    }
    wait_inode_pending2(fi->fh);
    get_global_lock((char *) __PRETTY_FUNCTION__);

    if (config->replication == 1
        && config->replication_role == 1) {
        release_global_lock();
        fuse_reply_err(req, EPERM);
        return;
    }

    inobno.inode = fi->fh;
    while (1) {
        inobno.blocknr =
            (offset + done) / BLKSIZE;
        offsetblock =
            offset + done -
            (inobno.blocknr * BLKSIZE);
        bsize = size - done;
        if (bsize + offsetblock > BLKSIZE)
            bsize = BLKSIZE - offsetblock;
        add2cache(&inobno, buf + done,
                  offsetblock, bsize);
        done = done + bsize;
        bsize = size - done;
        if (done >= size)
            break;
    }
    release_global_lock();
    fuse_reply_write(req, size);
    EFUNC;
}

/*
 * release
 */
static void ll_release(fuse_req_t req,
                       fuse_ino_t ino,
                       struct fuse_file_info *fi)
{
    const char *dataptr;
    int vsize;
    MEMDDSTAT *memddstat;
    DAT *ddbuf;
    int last_close = 0;
    unsigned long long inode_fh = fi->fh;

    FUNC;
    get_global_lock(
        (char *) __PRETTY_FUNCTION__);
    meta_lock((char *) __PRETTY_FUNCTION__);
    dataptr = tctreeget(metatree, &inode_fh,
                        sizeof(unsigned long long),
                        &vsize);
    if (dataptr) {
        memddstat = (MEMDDSTAT *) dataptr;
        if (memddstat->opened == 1) {
            last_close = 1;
            update_filesize_onclose(inode_fh);
            tctreeout(metatree, &inode_fh,
                sizeof(unsigned long long));
        } else {
            memddstat->opened--;
            ddbuf = create_mem_ddbuf(memddstat);
            tctreeput(metatree, &inode_fh,
                sizeof(unsigned long long),
                (void *) ddbuf->data,
                ddbuf->size);
            DATfree(ddbuf);
        }
    }
    release_meta_lock();

    /* If this was the last close, check for
       deferred deletion */
    if (last_close) {
        int deferred_vsize;
        const char *defdata;
        pthread_mutex_lock(&deferred_mutex);
        defdata = tctreeget(deferred_tree,
            &inode_fh,
            sizeof(unsigned long long),
            &deferred_vsize);
        pthread_mutex_unlock(&deferred_mutex);
        if (defdata) {
            write_lock(
                (char *) __PRETTY_FUNCTION__);
            complete_deferred_delete(inode_fh);
            release_write_lock();
        }
    }

    release_global_lock();
    fuse_reply_err(req, 0);
    EFUNC;
}

/*
 * fsync
 */
static void ll_fsync(fuse_req_t req,
                     fuse_ino_t ino,
                     int datasync,
                     struct fuse_file_info *fi)
{
    FUNC;
    (void) datasync;
    get_global_lock((char *) __PRETTY_FUNCTION__);
    write_lock((char *) __PRETTY_FUNCTION__);
    flush_wait(fi->fh);
    purge_read_cache(fi->fh, 0,
                     (char *) __PRETTY_FUNCTION__);
    release_write_lock();
    if (config->relax < 2)
        update_filesize_onclose(fi->fh);
    if (config->relax == 0)
        dbsync();
    release_global_lock();
    fuse_reply_err(req, 0);
    EFUNC;
}

/*
 * opendir
 */
static void ll_opendir(fuse_req_t req,
                       fuse_ino_t ino,
                       struct fuse_file_info *fi)
{
    /* FLEX_COMP: allow opening policy dirs */
    if (IS_POLICY_INO(ino)) {
        fi->fh = ino;
        fuse_reply_open(req, fi);
        return;
    }

    fi->fh = ino;
    fuse_reply_open(req, fi);
}

/*
 * readdir: low-level version using offset cookies.
 *
 * We use the existing DBDIRENT cursor to iterate.
 * The offset is the 0-based index into the entry list
 * for the directory.
 */
static void ll_readdir(fuse_req_t req,
                       fuse_ino_t ino,
                       size_t size, off_t off,
                       struct fuse_file_info *fi)
{
    unsigned long long dir_ino = ino;
    char *buf;
    size_t buf_used = 0;
    size_t entsize;
    int idx = 0;

    FUNC;

    /* FLEX_COMP: readdir on a .lessfs_policy dir */
    if (IS_POLICY_INO(dir_ino)
        && POLICY_TYPE(dir_ino) == SYNTH_DIR) {
        unsigned long long real_dir =
            POLICY_PARENT_INO(dir_ino);
        struct stat stbuf;
        off_t cur_idx = 0;

        buf = s_zmalloc(size);

        /* . */
        if (cur_idx >= off) {
            fill_policy_stat(dir_ino, &stbuf);
            entsize = fuse_add_direntry(
                req, buf + buf_used,
                size - buf_used,
                ".", &stbuf, cur_idx + 1);
            if (entsize <= size - buf_used)
                buf_used += entsize;
        }
        cur_idx++;

        /* .. */
        if (cur_idx >= off) {
            memset(&stbuf, 0, sizeof(stbuf));
            stbuf.st_ino = real_dir;
            stbuf.st_mode = S_IFDIR | 0755;
            entsize = fuse_add_direntry(
                req, buf + buf_used,
                size - buf_used,
                "..", &stbuf, cur_idx + 1);
            if (entsize <= size - buf_used)
                buf_used += entsize;
        }
        cur_idx++;

        /* compression */
        if (cur_idx >= off) {
            fill_policy_stat(
                MAKE_POLICY_INO(real_dir,
                                SYNTH_COMP),
                &stbuf);
            entsize = fuse_add_direntry(
                req, buf + buf_used,
                size - buf_used,
                "compression", &stbuf,
                cur_idx + 1);
            if (entsize <= size - buf_used)
                buf_used += entsize;
        }
        cur_idx++;

        /* deduplication */
        if (cur_idx >= off) {
            fill_policy_stat(
                MAKE_POLICY_INO(real_dir,
                                SYNTH_DEDUP),
                &stbuf);
            entsize = fuse_add_direntry(
                req, buf + buf_used,
                size - buf_used,
                "deduplication", &stbuf,
                cur_idx + 1);
            if (entsize <= size - buf_used)
                buf_used += entsize;
        }

        fuse_reply_buf(req, buf, buf_used);
        s_free(buf);
        EFUNC;
        return;
    }

    buf = s_zmalloc(size);

    get_global_lock((char *) __PRETTY_FUNCTION__);

    /*
     * Call the backend-specific readdir_ll that fills
     * entries using fuse_add_direntry.
     */
    buf_used = fs_readdir_ll(req, dir_ino, buf, size,
                             off);

    /* FLEX_COMP: inject .lessfs_policy for top-level
       directories.  Use POLICY_DONE_COOKIE as the
       offset cookie so the kernel won't re-request
       this entry on the next readdir call. */
    if (off < POLICY_DONE_COOKIE
        && is_toplevel_dir_ll(dir_ino)
        && buf_used < size) {
        struct stat pol_stbuf;
        unsigned long long synth_ino =
            MAKE_POLICY_INO(dir_ino, SYNTH_DIR);
        fill_policy_stat(synth_ino, &pol_stbuf);
        entsize = fuse_add_direntry(
            req, buf + buf_used,
            size - buf_used,
            LESSFS_POLICY_DIR,
            &pol_stbuf, POLICY_DONE_COOKIE);
        if (entsize <= size - buf_used)
            buf_used += entsize;
    }

    release_global_lock();

    fuse_reply_buf(req, buf, buf_used);
    s_free(buf);
    EFUNC;
}

/*
 * releasedir
 */
static void ll_releasedir(fuse_req_t req,
                          fuse_ino_t ino,
                          struct fuse_file_info *fi)
{
    (void) ino;
    (void) fi;
    fuse_reply_err(req, 0);
}

/*
 * statfs
 */
static void ll_statfs(fuse_req_t req,
                      fuse_ino_t ino)
{
    struct statvfs stbuf;
    char *blockdatadir;
    int res;

    blockdatadir = s_dirname(config->blockdata);
    res = statvfs(blockdatadir, &stbuf);
    s_free(blockdatadir);

    if (res == -1) {
        fuse_reply_err(req, errno);
        return;
    }
    fuse_reply_statfs(req, &stbuf);
}

/*
 * access: let the kernel handle permissions via
 * default_permissions mount option.
 */
static void ll_access(fuse_req_t req,
                      fuse_ino_t ino, int mask)
{
    (void) ino;
    (void) mask;
    fuse_reply_err(req, 0);
}

/* ============================================================
 *  Operation table
 * ============================================================ */

static struct fuse_lowlevel_ops ll_oper = {
    .init       = ll_init,
    .destroy    = ll_destroy,
    .lookup     = ll_lookup,
    .forget     = ll_forget,
    .getattr    = ll_getattr,
    .setattr    = ll_setattr,
    .readlink   = ll_readlink,
    .mknod      = ll_mknod,
    .create     = ll_create,
    .mkdir      = ll_mkdir,
    .unlink     = ll_unlink,
    .rmdir      = ll_rmdir,
    .symlink    = ll_symlink,
    .rename     = ll_rename,
    .link       = ll_link,
    .open       = ll_open,
    .read       = ll_read,
    .write      = ll_write,
    .release    = ll_release,
    .fsync      = ll_fsync,
    .opendir    = ll_opendir,
    .readdir    = ll_readdir,
    .releasedir = ll_releasedir,
    .statfs     = ll_statfs,
    .access     = ll_access,
};

/* ============================================================
 *  Threads reused from lessfs.c — only the ones that don't
 *  reference high-level fuse API types.
 * ============================================================ */

/* Return 0 when enough space s_free
   Return 1 at MIN_SPACE_FREE
   Return 2 at MIN_SPACE_CLEAN
   Return 3 at MIN_SPACE_FREE+3% */
int check_free_space(char *dbpath)
{
    float dfree;
    int mf, mc, sr;
    char *minfree;
    char *minclean;
    char *startreclaim;
    struct statfs sb;

    if (-1 == statfs(dbpath, &sb))
        die_dataerr("Failed to stat : %s", dbpath);
    dfree =
        (float) sb.f_blocks / (float) sb.f_bfree;
    dfree = 100 / dfree;
    minfree = getenv("MIN_SPACE_FREE");
    minclean = getenv("MIN_SPACE_CLEAN");
    if (minfree) {
        mf = atoi(minfree);
        if (mf > 100 || mf < 1)
            mf = 10;
    } else {
        mf = 10;
    }
    startreclaim = getenv("START_RECLAIM");
    if (startreclaim) {
        sr = atoi(startreclaim);
        if (sr > 100 || sr < 1)
            sr = 15;
    } else
        sr = 1;
    if (100 - dfree >= sr) {
        config->reclaim = 1;
    } else
        config->reclaim = 0;
    if (dfree <= mf)
        return (1);
    if (minclean) {
        mc = atoi(minclean);
        if (mc > 100 || mc < 1)
            mc = 15;
    } else {
        mc = 0;
    }
    if (dfree <= mc)
        return (2);
    if (dfree <= mf + 1)
        return (3);
    return (0);
}

void freeze_nospace(char *dbpath)
{
    config->frozen = 1;
    get_global_lock((char *) __PRETTY_FUNCTION__);
    write_lock((char *) __PRETTY_FUNCTION__);
    flush_wait(0);
    purge_read_cache(0, 1,
                     (char *) __PRETTY_FUNCTION__);
    if (config->nospace != 0) {
        release_write_lock();
        release_global_lock();
        if (config->nospace == 1) {
            config->nospace = -ENOSPC;
            LFATAL(
                "Filesystem for database %s has "
                "insufficient space to continue, "
                "ENOSPC", dbpath);
        }
        return;
    } else {
        LFATAL(
            "Filesystem for database %s has "
            "insufficient space to continue, "
            "freezing I/O", dbpath);
    }
    while (1) {
        if (0 == check_free_space(dbpath)) {
            if (config->nospace == 0) {
                config->frozen = 0;
                release_write_lock();
                release_global_lock();
            }
            break;
        }
        sleep(1);
    }
    if (config->nospace == 0)
        LFATAL("Resuming IO : sufficient space.");
}

void *housekeeping_worker(void *arg)
{
    char *dbpath = NULL;
    int result;
    int count = 0;

    while (1) {
        while (count <= 6) {
            switch (count) {
#ifdef BERKELEYDB
            case 0:
                dbpath = as_sprintf(
                    __FILE__, __LINE__,
                    "%s/metadata.db", config->meta);
                count = 7;
                break;
            case 1:
                if (config->blockdata_io_type
                    == FILE_IO) {
                    dbpath =
                        s_strdup(config->blockdata);
                }
                count = 7;
                break;
#elif defined(LMDB)
            case 0:
                dbpath = as_sprintf(
                    __FILE__, __LINE__,
                    "%s", config->meta);
                count = 7;
                break;
            case 1:
                if (config->blockdata_io_type
                    == FILE_IO) {
                    dbpath =
                        s_strdup(config->blockdata);
                }
                count = 7;
                break;
#endif
            default:
                break;
            }
            result = check_free_space(dbpath);
            switch (result) {
            case 1:
                freeze_nospace(dbpath);
                break;
            case 2:
                /* exec_clean_program(); */
                break;
            case 3:
                config->nospace = RECLAIM_AGRESSIVE;
                break;
            default:
                break;
            }
            s_free(dbpath);
            count++;
            sleep(config->inspectdiskinterval);
        }
        count = 0;
        repl_lock((char *) __PRETTY_FUNCTION__);
        release_repl_lock();
    }
    return NULL;
}

void *lessfs_flush(void *arg)
{
    struct stat stbuf;
    time_t curtime;

    while (1) {
        curtime = time(NULL);
        if (config->replication) {
            sleep(config->flushtime / 2);
        } else
            sleep(config->flushtime);
        if (config->replication != 1
            || config->replication_role != 1) {
            get_global_lock(
                (char *) __PRETTY_FUNCTION__);
            write_lock(
                (char *) __PRETTY_FUNCTION__);
            flush_wait(0);
            purge_read_cache(0, 1,
                (char *) __PRETTY_FUNCTION__);
            if (config->blockdata_io_type
                == CHUNK_IO)
                sync();
            start_flush_commit();
            end_flush_commit();
            if (0 == strcmp(
                    config->replication_partner_ip,
                    "-1")
                && config->replication
                && config->replication_role == 0) {
                repl_lock(
                    (char *) __PRETTY_FUNCTION__);
                if (-1 == fstat(frepl, &stbuf))
                    die_syserr();
                if (stbuf.st_size
                    > config->rotate_replog_size
                    || (curtime
                        - config->
                          replication_last_rotated)
                       > REPLOG_DELAY) {
                    config->
                        replication_last_rotated =
                        time(NULL);
                    rotate_replog();
                } else
                    fsync(frepl);
                release_repl_lock();
            }
            release_write_lock();
            release_global_lock();
            if (config->replication == 1
                && config->replication_role == 0) {
                if (-1 == fstat(frepl, &stbuf))
                    die_syserr();
                if (0
                    != config->max_backlog_size
                    && stbuf.st_size
                       > config->max_backlog_size) {
                    LINFO(
                        "Waiting for replication "
                        "log to drain");
                    trunc_lock(
                        (char *)
                        __PRETTY_FUNCTION__);
                    while (1) {
                        if (-1 == fstat(frepl,
                                        &stbuf))
                            die_syserr();
                        if (stbuf.st_size
                            < config->
                              max_backlog_size)
                            break;
                        usleep(10000);
                    }
                    release_trunc_lock();
                }
                write_repl_data(
                    WRITTEN, TRANSACTIONCOMMIT,
                    " ", 1, NULL, 0,
                    MAX_ALLOWED_THREADS - 2);
            }
        }
    }
    pthread_exit(NULL);
}

void *init_worker(void *arg)
{
    int count;
    char *aptr;
    uintptr_t ptr;
    CCACHEDTA *ccachedta;
    char *key;
    int size;
    int vsize;
    int found;
    char *dupkey;
    int index;
    TCLIST *keylist;

    memcpy(&count, arg, sizeof(int));
    s_free(arg);
    found = 0;
    while (1) {
        if (found == 0) {
            usleep(10000);
            if (config->replication
                && config->replication_role == 0
                && 0 != strcmp(
                       config->
                       replication_partner_ip,
                       "-1")) {
                if (0 == try_replbl_lock(
                       (char *)
                       __PRETTY_FUNCTION__)) {
                    send_backlog();
                    release_replbl_lock();
                }
            } else if (config->shutdown
                       || config->
                          replication_enabled == 0)
                config->safe_down = 1;
        }
        found = 0;
        write_lock((char *) __PRETTY_FUNCTION__);
        keylist = tctreekeys(workqtree);
        index = 0;
        if (0 != tclistnum(keylist)) {
            inobnolistsort(keylist);
            key = (char *) tclistval(keylist, 0,
                                     &size);
            aptr = (char *) tctreeget(
                workqtree, (void *) key,
                size, &vsize);
            if (aptr) {
                memcpy(&ptr, aptr, vsize);
                ccachedta = get_ccachedta(ptr);
                dupkey = s_malloc(size);
                memcpy(dupkey, key, size);
                tctreeout(workqtree, key, size);
                found++;
                tclistdel(keylist);
                release_write_lock();
                worker_lock(
                    (char *)
                    __PRETTY_FUNCTION__);
                working++;
                sequence++;
                release_worker_lock();
                cook_cache(dupkey, size,
                           ccachedta, sequence);
                s_free(dupkey);
                worker_lock(
                    (char *)
                    __PRETTY_FUNCTION__);
                working--;
                release_worker_lock();
            } else
                die_dataerr(
                    "Key without value in "
                    "workers");
        } else
            tclistdel(keylist);
        if (0 == found)
            release_write_lock();
    }
    LFATAL("Thread %u exits", count);
    pthread_exit(NULL);
}

void show_lock_status(int csocket)
{
    char *msg;
    timeoutWrite(3, csocket,
        "---------------------\n",
        strlen("---------------------\n"));
    timeoutWrite(3, csocket,
        "normally unset\n\n",
        strlen("normally unset\n\n"));
    if (0 != try_global_lock()) {
        msg = as_sprintf(__FILE__, __LINE__,
            "global_lock : 1 (set) by %s\n",
            global_lockedby);
        timeoutWrite(3, csocket, msg, strlen(msg));
        s_free(msg);
    } else {
        release_global_lock();
        timeoutWrite(3, csocket,
            "global_lock : 0 (not set)\n",
            strlen("global_lock : 0 (not set)\n"));
    }
    if (0 != try_meta_lock()) {
        msg = as_sprintf(__FILE__, __LINE__,
            "meta_lock : 1 (set) by %s\n",
            meta_lockedby);
        timeoutWrite(3, csocket, msg, strlen(msg));
        s_free(msg);
    } else {
        release_meta_lock();
        timeoutWrite(3, csocket,
            "meta_lock : 0 (not set)\n",
            strlen("meta_lock : 0 (not set)\n"));
    }
    if (0 != try_write_lock()) {
        msg = as_sprintf(__FILE__, __LINE__,
            "write_lock : 1 (set) by %s\n",
            write_lockedby);
        timeoutWrite(3, csocket, msg, strlen(msg));
        s_free(msg);
    } else {
        release_write_lock();
        timeoutWrite(3, csocket,
            "write_lock : 0 (not set)\n",
            strlen("write_lock : 0 (not set)\n"));
    }
    timeoutWrite(3, csocket,
        "---------------------\n",
        strlen("---------------------\n"));
}

void *ioctl_worker(void *arg)
{
    int msocket;
    int csocket;
    const char *port;
    const char *proto = "tcp";
    struct sockaddr_un client_address;
    socklen_t client_len;
    char buf[1028];
    char *addr;
    int err = 0;
    char *result;
    char *message = NULL;
    bool isfrozen = 0;

    msocket = -1;
    while (1) {
        addr = getenv("LISTEN_IP");
        port = getenv("LISTEN_PORT");
        if (NULL == port)
            port = "100";
        if (NULL == addr)
            LWARNING(
                "The administration session is "
                "not limited to localhost.");
        msocket = serverinit(addr, port, proto);
        if (msocket != -1)
            break;
        sleep(1);
        close(msocket);
    }

    client_len = sizeof(client_address);
    while (1) {
        csocket = accept(msocket,
            (struct sockaddr *) &client_address,
            &client_len);
        while (1) {
            result = NULL;
            if (-1 == timeoutWrite(3, csocket,
                                   ">", 1))
                break;
            if (-1 == readnlstring(10, csocket,
                                   buf, 1024)) {
                result = "timeout";
                err = 1;
                break;
            }
            if (0 == strncmp(buf, "\r",
                             strlen("\r")))
                continue;
            if (0 == strncmp(buf, "quit\r",
                             strlen("quit\r"))
                || 0 == strncmp(buf, "exit\r",
                                strlen("exit\r"))) {
                err = 0;
                result = "bye";
                break;
            }
            if (0 == strncmp(buf, "freeze\r",
                             strlen("freeze\r"))) {
                if (0 == isfrozen) {
                    result =
                        "All i/o is now suspended.";
                    err = 0;
                    get_global_lock(
                        (char *)
                        __PRETTY_FUNCTION__);
                    start_flush_commit();
                    isfrozen = 1;
                } else {
                    result =
                        "i/o is already suspended.";
                    err = 1;
                }
            }
            if (0 == strncmp(buf, "defrost\r",
                             strlen("defrost\r"))) {
                if (1 == isfrozen) {
                    result = "Resuming i/o.";
                    err = 0;
                    end_flush_commit();
                    release_global_lock();
                    isfrozen = 0;
                } else {
                    result =
                        "i/o is not suspended.";
                    err = 1;
                }
            }
            if (0 == strncmp(buf, "help\r",
                             strlen("help\r"))
                || 0 == strncmp(buf, "h\r",
                                strlen("h\r"))) {
                result =
                    "valid commands: defrost "
                    "freeze help lockstatus "
                    "quit|exit";
                err = 0;
            }
            if (0 == strncmp(buf, "lockstatus\r",
                         strlen("lockstatus\r"))) {
                show_lock_status(csocket);
                result = "lockstatus listed";
                err = 0;
            }
            if (NULL == result) {
                err = -1;
                result = "unknown command";
            }
            if (err == 0) {
                message = as_sprintf(
                    __FILE__, __LINE__,
                    "+OK %s\n", result);
                if (-1 == timeoutWrite(3, csocket,
                    message, strlen(message)))
                    break;
            } else {
                message = as_sprintf(
                    __FILE__, __LINE__,
                    "-ERR %s\n", result);
                if (-1 == timeoutWrite(3, csocket,
                    message, strlen(message)))
                    break;
            }
            s_free(message);
            message = NULL;
        }
        if (message)
            s_free(message);
        if (err == 0) {
            message = as_sprintf(
                __FILE__, __LINE__,
                "+OK %s\n", result);
            timeoutWrite(3, csocket,
                         message, strlen(message));
        } else {
            message = as_sprintf(
                __FILE__, __LINE__,
                "-ERR %s\n", result);
            timeoutWrite(3, csocket,
                         message, strlen(message));
        }
        s_free(message);
        message = NULL;
        close(csocket);
    }
    return NULL;
}

/* Dirty/blocksize check helpers from lessfs.c */
void mark_dirty()
{
    unsigned char *stiger;
    char *brand;
    INUSE finuse;
    time_t thetime;
    unsigned long long inuse;

    FUNC;
    brand = as_sprintf(__FILE__, __LINE__,
                       "LESSFS_DIRTY");
    stiger = thash((unsigned char *) brand,
                   strlen(brand));
    thetime = time(NULL);
    inuse = thetime;
    finuse.inuse = BLKSIZE;
    finuse.size = inuse;
    finuse.offset = 0;
    file_update_inuse(stiger, &finuse);
    s_free(brand);
    free(stiger);
    EFUNC;
}

int check_dirty()
{
    unsigned char *stiger;
    char *brand;
    INUSE *finuse;
    int dirty = 0;
    brand = as_sprintf(__FILE__, __LINE__,
                       "LESSFS_DIRTY");
    stiger = thash((unsigned char *) brand,
                   strlen(brand));
    finuse = file_get_inuse(stiger);
    if (finuse) {
        s_free(finuse);
        dirty = 1;
    }
    free(stiger);
    s_free(brand);
    return (dirty);
}

void check_blocksize()
{
    int blksize;
    blksize = get_blocksize();
    if (blksize != BLKSIZE)
        die_dataerr(
            "Not allowed to mount lessfs with "
            "blocksize %u when previously used "
            "with blocksize %i",
            BLKSIZE, blksize);
}

int verify_kernel_version()
{
    struct utsname un;
    char *begin;
    char *end;
    int count;

    uname(&un);
    begin = un.release;
    for (count = 0; count < 3; count++) {
        end = strchr(begin, '.');
        if (end) {
            end[0] = 0;
            end++;
        }
        if (count == 0 && atoi(begin) < 2)
            return (-1);
        if (count == 0 && atoi(begin) > 2)
            break;
        if (count == 1 && atoi(begin) < 6)
            return (-2);
        if (count == 1 && atoi(begin) > 6)
            break;
        if (count == 2 && atoi(begin) < 26)
            return (-3);
        begin = end;
    }
    return (0);
}

void usage(char *appName)
{
    printf(
        "\n"
        "-------------------------------------------\n"
        "lessfs %s (low-level FUSE API)\n\n"
        "Usage: %s [/path_to_config.cfg] "
        "[mount_point] <FUSE OPTIONS>\n\n"
        "Example:\n"
        "mklessfs /etc/lessfs.cfg\n"
        "lessfs   /etc/lessfs.cfg /mnt\n\n"
        "-------------------------------------------\n",
        LFSVERSION, appName);
    exit(EXIT_USAGE);
}

int main(int argc, char *argv[])
{
    int res;
    char *pstr = NULL;
    struct rlimit lessfslimit;
    struct fuse_args args;
    struct fuse_chan *chan;
    struct fuse_session *sess;
    char *mountpoint;

    FUNC;

    if ((argc > 1) && (strcmp(argv[1], "-h") == 0))
        usage(argv[0]);
    if (argc < 3)
        usage(argv[0]);
    if (-1 == r_env_cfg(argv[1]))
        usage(argv[0]);

    /* Build FUSE args from argv[0] + argv[2..] */
    int argc_new = argc - 1;
    char **argv_new =
        (char **) s_malloc(argc * sizeof(char *));
    argv_new[0] = argv[0];
    int idx;
    for (idx = 1; idx < argc - 1; idx++)
        argv_new[idx] = argv[idx + 1];

    if (getenv("COREDUMPSIZE")) {
        lessfslimit.rlim_cur =
            lessfslimit.rlim_max =
            atoi(getenv("COREDUMPSIZE"));
        if (0 != setrlimit(RLIMIT_CORE,
                           &lessfslimit)) {
            fprintf(stderr,
                "Failed to set COREDUMPSIZE\n");
            exit(EXIT_SYSTEM);
        }
    } else {
        // signal(SIGSEGV, segvExit); // disabled for debug
    }

    signal(SIGHUP, normalExit);
    signal(SIGTERM, normalExit);
    signal(SIGALRM, normalExit);
    signal(SIGINT, normalExit);
    signal(SIGUSR1, libSafeExit);
    if (getenv("DEBUG"))
        debug = atoi(getenv("DEBUG"));
    parseconfig(0, 0);

    deferred_tree = tctreenew();
    args = (struct fuse_args)
        FUSE_ARGS_INIT(argc_new, argv_new);
    fuse_opt_parse(&args, NULL, NULL, NULL);
    pstr = as_sprintf(__FILE__, __LINE__,
        "-omax_readahead=128,max_write=%u,"
        "max_read=%u", BLKSIZE, BLKSIZE);
    fuse_opt_add_arg(&args, pstr);
    s_free(pstr);
    if (BLKSIZE > 4096) {
        if (0 != verify_kernel_version()) {
            LFATAL(
                "Kernel too old for >4k blocks.");
            exit(EXIT_SYSTEM);
        }
        fuse_opt_add_arg(&args, "-obig_writes");
    }
    fuse_opt_add_arg(&args,
        "-odefault_permissions,allow_other");

    /* Extract mountpoint from args */
    mountpoint = NULL;
    {
        int multithreaded;
        int foreground_flag = 0;
        if (fuse_parse_cmdline(&args, &mountpoint,
                               &multithreaded,
                               &foreground_flag) == -1) {
            fprintf(stderr,
                "Failed to parse command line\n");
            exit(EXIT_SYSTEM);
        }
        /* Daemonize unless -f was given */
        fuse_daemonize(foreground_flag);
    }

    if (mountpoint == NULL) {
        fprintf(stderr,
            "No mountpoint specified\n");
        exit(EXIT_SYSTEM);
    }

    chan = fuse_mount(mountpoint, &args);
    if (chan == NULL) {
        fprintf(stderr,
            "fuse_mount failed\n");
        exit(EXIT_SYSTEM);
    }

    sess = fuse_lowlevel_new(&args, &ll_oper,
                             sizeof(ll_oper), NULL);
    if (sess == NULL) {
        fuse_unmount(mountpoint, chan);
        fprintf(stderr,
            "fuse_lowlevel_new failed\n");
        exit(EXIT_SYSTEM);
    }

    fuse_session_add_chan(sess, chan);

    /* Run multi-threaded event loop */
    res = fuse_session_loop_mt(sess);

    fuse_session_remove_chan(chan);
    fuse_session_destroy(sess);
    fuse_unmount(mountpoint, chan);
    fuse_opt_free_args(&args);
    s_free(argv_new);

    return res ? 1 : 0;
}

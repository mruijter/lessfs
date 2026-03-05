#define die_dberr(f...) { LFATAL(f); exit(EXIT_DBERR); }
#define die_dataerr(f...) { LFATAL(f); exit(EXIT_DATAERR); }
#define die_syserr() { LFATAL("Fatal system error : %s",strerror(errno)); exit(EXIT_SYSTEM); }

#define LOCK 1
#define NOLOCK 0
#define MAX_THREADS 1
#define MAX_POSIX_FILENAME_LEN 256
#define MAX_FUSE_BLKSIZE 131072
#define METAQSIZE 2048
#define MAX_HASH_LEN 64
#define CACHE_MAX_AGE 3
#define MAX_ALLOWED_THREADS 1024
/* Used for debugging locking */
#define GLOBAL_LOCK_TIMEOUT 7200        //Since deleting or truncating can take a long time...
#define LOCK_TIMEOUT 3600
#define MAX_META_CACHE 100000   //This consumes approx 1.5 MB memory
#define ROTATE_REPLOG_SIZE 1073741824
#define RECLAIM_AGRESSIVE 128
#define HIGH_PRIO 0
#define MEDIUM_PRIO 1
#define LOW_PRIO 2
#define MAX_CHUNK_DEPTH 5
#define LFSSMALL 0                 // Filesystem <=1TB & BDB
#define LFSMEDIUM 1                // Filesystem <=10TB & BDB
#define LFSHUGE 2                  // Filesystem >10TB & BDB

typedef unsigned long long int word64;
typedef unsigned long word32;
typedef unsigned char byte;

enum IOTYPE {FILE_IO,MULTIFILE_IO,CHUNK_IO};

typedef struct {
    unsigned long long offset;
    unsigned long size;
    unsigned long long allocated_size;
    unsigned long long inuse;
} INUSE;

typedef struct {
    unsigned long long offset;
    unsigned long size;
    unsigned long long inuse;
} OLDINUSE;

typedef struct {
    unsigned long long inode;
    unsigned long long blocknr;
} INOBNO;

typedef struct {
    //unsigned int snapshotnr;
    unsigned long long dirnode;
    unsigned long long inode;
} DINOINO;

typedef struct {
    unsigned long size;
    unsigned char *data;
} DAT;

typedef struct {
    struct stat stbuf;
    unsigned long long real_size;
    char filename[MAX_POSIX_FILENAME_LEN + 1];
} DDSTAT;

typedef struct {
    char passwd[64];
    char iv[8];
} CRYPTO;

typedef struct {
    struct stat stbuf;
    unsigned int updated;
    unsigned long long blocknr;
    unsigned int opened;
    unsigned long long real_size;
    char filename[MAX_POSIX_FILENAME_LEN + 1];
} MEMDDSTAT;

struct truncate_thread_data {
    unsigned long long blocknr;
    unsigned long long lastblocknr;
    unsigned long long inode;
    unsigned int offsetblock;
    struct stat stbuf;
    bool unlink;
};

typedef struct {
    unsigned char data[MAX_FUSE_BLKSIZE];
    unsigned long datasize;
    unsigned char hash[MAX_HASH_LEN];
    int dirty;
    time_t creationtime;
    int pending;
    char newblock;
    unsigned long updated;
} CCACHEDTA;

static inline CCACHEDTA *get_ccachedta(uintptr_t p)
{
    return (CCACHEDTA *) p;
}

unsigned char *thash(unsigned char *, int);
void check_datafile_sanity();
int fs_readlink(const char *, char *, size_t);
int fs_symlink(char *, char *);
/* Thread-local caller credentials (LL FUSE) */
extern __thread uid_t ll_caller_uid;
extern __thread gid_t ll_caller_gid;

void invalidate_p2i(char *);
void erase_p2i();
void cache_p2i(char *, struct stat *);
void logiv(char *, unsigned char *);
void loghash(char *, unsigned char *);
void log_fatal_hash(char *, unsigned char *);
void create_hash_note(unsigned char *);
void wait_hash_pending(unsigned char *);
void delete_hash_note(unsigned char *);
void create_inode_note(unsigned long long);
void wait_inode_pending(unsigned long long);
int inode_isnot_locked(unsigned long long);
void wait_inode_pending2(unsigned long long);
void delete_inode_note(unsigned long long);
void die_lock_report(const char *, const char *);
void get_global_lock(const char *);
void get_global_lock(const char *);
int try_global_lock();
void release_global_lock();
void write_lock(const char *);
void release_write_lock();
int try_write_lock();
void get_hash_lock(const char *);
void release_hash_lock();
void get_inode_lock(const char *);
void release_inode_lock();
void get_offset_lock(const char *);
void release_offset_lock();
void meta_lock(const char *);
void repl_lock(const char *);
void release_repl_lock();
void release_replbl_lock();
void release_meta_lock();
int try_meta_lock();
void trunc_lock();
void release_trunc_lock();
void truncation_wait();
DAT *create_mem_ddbuf(MEMDDSTAT *);
DDSTAT *value_to_ddstat(DAT *);
DAT *create_ddbuf(struct stat, char *, unsigned long long);
void dbmknod(const char *, mode_t, char *, dev_t);
int get_realsize_fromcache(unsigned long long, struct stat *);
int dbstat(const char *, struct stat *, bool);
void comprfree(compr *);
void memddstatfree(MEMDDSTAT *);
MEMDDSTAT *value_tomem_ddstat(char *, int);
void ddstatfree(DDSTAT *);
int path_from_cache(char *, struct stat *);
int get_dir_inode(char *, struct stat *, bool);
void formatfs();
void write_file_ent(const char *, unsigned long long, mode_t mode, char *,
                    dev_t);
void write_nfi(unsigned long long);
void write_seq(unsigned long);
DAT *lfsdecompress(DAT *);
void auto_repair(INOBNO *);
unsigned long long readBlock(unsigned long long,
                             char *, size_t, size_t, unsigned long long);
void delete_inuse(unsigned char *);
void delete_dbb(INOBNO *);
unsigned long long getInUse(unsigned char *);
void DATfree(DAT *);
void update_inuse(unsigned char *, unsigned long long);
unsigned long long get_next_inode();
MEMDDSTAT *inode_meta_from_cache(unsigned long long);
void update_filesize_onclose(unsigned long long);
int update_filesize_cache(struct stat *, off_t);
void update_filesize(unsigned long long, unsigned long long,
                     unsigned int, unsigned long long);
void hash_update_filesize(MEMDDSTAT *, unsigned long long);
DAT *lfscompress(unsigned char *, unsigned long);
DAT *check_block_exists(INOBNO *);
int fs_mkdir(const char *, mode_t);
int db_unlink_file(const char *);
void partial_truncate_block(unsigned long long, unsigned long long,
                            unsigned int);
void *tc_truncate_worker(void *);
DAT *search_dbdata(int, void *key, int, bool);
DDSTAT *dnode_bname_to_inode(void *, int, char *);
char *lessfs_stats();
int count_dirlinks(void *, int);
struct tm *init_transactions();
int update_parent_time(char *, int);
int update_stat(char *, struct stat *);
void flush_wait(unsigned long long);
void db_close(bool);
void cook_cache(char *, int, CCACHEDTA *, unsigned long);
unsigned long long get_inode(const char *);
void purge_read_cache(unsigned long long, bool, char *);
void update_meta(unsigned long long, unsigned long, int);
void lessfs_trans_stamp();
int db_fs_truncate(struct stat *, off_t, char *, bool);
void write_trunc_todolist(struct truncate_thread_data *);
void tc_write_cache(CCACHEDTA *, INOBNO *,
                    unsigned char);
void db_delete_stored(INOBNO *);
void fil_fuse_info(DDSTAT *, void *, fuse_fill_dir_t,
                   struct fuse_file_info *);
void locks_to_dir(void *, fuse_fill_dir_t, struct fuse_file_info *);
int lckname_to_stat(char *, struct stat *);
int fs_link(char *, char *);
int fs_rename_link(const char *, const char *, struct stat);
int fs_rename(const char *, const char *, struct stat);
int fs_rmdir(const char *);
void clear_dirty();
void parseconfig(int, bool);
void sync_all_filesizes();
int get_blocksize();
void brand_blocksize();
void btbin_write_dup(int database, void *, int, void *, int, bool);
void checkpasswd(char *);
int try_offset_lock();
int try_hash_lock();
int try_repl_lock();
int try_replbl_lock();
void worker_lock();
void release_worker_lock();
void cachep2i_lock(const char *);
void release_cachep2i_lock();
int try_cachep2i_lock();
unsigned long get_sequence();
void next_sequence();
char *ascii_hash(unsigned char *);
void open_trees();
void close_trees();
INUSE *get_offset(unsigned long long);
void inobnolistsort(TCLIST *list);
void mkchunk_dir(char *);
unsigned char *hash_to_ascii(unsigned char *);
/* Low-level FUSE API support functions */
char *inode_to_path(unsigned long long);
#ifdef FUSE_USE_VERSION
#if FUSE_USE_VERSION >= 29
size_t fs_readdir_ll(fuse_req_t, unsigned long long,
                     char *, size_t, off_t);
#endif
#endif
void mark_dirty(void);
int check_dirty(void);
void check_blocksize(void);
int verify_kernel_version(void);
unsigned long long get_real_size(unsigned long long);
void add2cache(INOBNO *, const char *, off_t, size_t);
int check_free_space(char *);
void freeze_nospace(char *);

/* ============================================================
 *  FLEX_COMP: Per-directory compression & deduplication policy
 * ============================================================ */
typedef struct {
    unsigned char compression;   /* codec char or 0 */
    unsigned char deduplication;  /* 0 or 1 */
} DIR_POLICY;

/* Synthetic inode scheme for .lessfs_policy virtual dirs */
#define SYNTH_BASE   0x4000000000000000ULL
#define SYNTH_DIR    0  /* .lessfs_policy dir       */
#define SYNTH_COMP   1  /* compression control file  */
#define SYNTH_DEDUP  2  /* deduplication control file */

#define MAKE_POLICY_INO(dir_ino, type) \
    (SYNTH_BASE | ((unsigned long long)(dir_ino) << 2) \
     | (unsigned long long)(type))
#define IS_POLICY_INO(ino) \
    (((unsigned long long)(ino) & SYNTH_BASE) != 0)
#define POLICY_PARENT_INO(ino) \
    (((unsigned long long)(ino) & ~SYNTH_BASE) >> 2)
#define POLICY_TYPE(ino) \
    ((unsigned int)((unsigned long long)(ino) & 3))

#define LESSFS_POLICY_DIR ".lessfs_policy"

#define POLICY_DONE_COOKIE 0x7FFFFFFFFFFFFFFELL
/* Policy tree and cache (defined in lib_common.c) */
extern TCTREE *dir_policy_tree;
extern TCTREE *inode_policy_cache;
extern pthread_mutex_t policy_mutex;

DIR_POLICY *get_dir_policy(
    unsigned long long top_dir_inode);
DIR_POLICY resolve_inode_policy(
    unsigned long long file_inode);
void set_dir_policy(
    unsigned long long top_dir_inode,
    DIR_POLICY *policy);
void invalidate_policy_cache(
    unsigned long long file_inode);
void load_all_policies(void);
int is_toplevel_dir(unsigned long long inode);
unsigned long long toplevel_dir_for_inode(
    unsigned long long file_inode);
unsigned char compression_name_to_char(
    const char *name);
const char *compression_char_to_name(
    unsigned char comp);

/* Updated lfscompress with explicit compression type */
DAT *lfscompress_policy(unsigned char *dbdata,
    unsigned long dsize, unsigned char comp);

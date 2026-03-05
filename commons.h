#define MAX_THREADS 1

extern struct configdata *config;

char *logname;
char *function = __FILE__;
int debug = 5;
int BLKSIZE = 4096;
int max_threads = MAX_THREADS;


extern TCTREE *workqtree;
extern TCTREE *readcachetree;
extern TCTREE *inodetree;

extern TCTREE *metatree;
extern TCTREE *path2inotree;
extern TCTREE *hashtree;
extern int fdbdta;
extern int frepl;

extern unsigned long long nextoffset;
extern const char *write_lockedby;
extern const char *global_lockedby;
extern const char *hash_lockedby;
extern const char *meta_lockedby;
extern const char *offset_lockedby;
extern const char *cachep2i_lockedby;

/* FLEX_COMP policy trees */
extern TCTREE *dir_policy_tree;
extern TCTREE *inode_policy_cache;
extern pthread_mutex_t policy_mutex;

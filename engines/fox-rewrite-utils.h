#ifndef FOX_REWRITE_UTILS_H
#define FOX_REWRITE_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>
#include <liblightnvm.h>
#include "../fox.h"

#define READ_MODE 1
#define WRITE_MODE 2

#define PAGE_CLEAN 0
#define PAGE_DIRTY 1
#define PAGE_ABANDONED 2

#define BLOCK_CLEAN 0
#define BLOCK_DIRTY 1

struct nodegeoaddr {
    uint64_t offset_in_page; // parts not aligned to page (in ocssd)
    uint64_t ch_i;
    uint64_t lun_i;
    uint64_t blk_i;
    uint64_t pg_i;
};

struct fox_iounit {
    char iotype; // 'r' for read and 'w' for write
    uint64_t offset;
    uint64_t size;
    uint64_t exetime;
    double gc_becost; // benefit / cost factor for gc
    uint64_t nabandoned;
    uint64_t ndirty;
    uint64_t nblock;
    uint64_t map_change_count;
    uint64_t map_set_count;
    uint64_t gc_count;
    uint64_t gc_time;
    uint64_t gc_map_change_count;
    uint64_t pgs_r;
    uint64_t bread;
    uint64_t pgs_w;
    uint64_t bwritten;
    uint64_t erased_blks;
    uint64_t erase_t;
    uint64_t read_t;
    uint64_t write_t;
};

struct fox_heatmap_unit {
    uint64_t readt;
    uint64_t writet;
    uint64_t eraset;
};

struct rewrite_meta {
    struct fox_node* node;
    uint64_t total_pagenum;
    size_t vpg_sz;
    uint8_t* page_state; // state of all pages in node
    uint8_t* blk_state; // state of all blocks in node
    uint8_t* temp_page_state_inblk; // temporarily store page state in one block, used for collecting dirty pages in one block before erase it, useful for rewriting dirty pages
    uint8_t* begin_pagebuf;
    uint8_t* end_pagebuf;
    uint8_t* pagebuf;
    struct fox_iounit* ioseq;
    uint64_t ioseqlen;
    struct fox_heatmap_unit* heatmap;
};

uint64_t geoaddr2vpg(struct fox_node* node, struct nodegeoaddr* geoaddr);

uint64_t geoaddr2vblk(struct fox_node* node, struct nodegeoaddr* geoaddr);

struct nodegeoaddr vpg2geoaddr(struct fox_node* node, uint64_t vpg_i);

struct nodegeoaddr vblk2geoaddr(struct fox_node* node, uint64_t vblk_i);

uint64_t vpg2vblk(struct fox_node* node, uint64_t vpg_i);

uint64_t vblk2vpg(struct fox_node* node, uint64_t vblk_i);

uint8_t* get_p_page_state(struct rewrite_meta* meta, struct nodegeoaddr* geoaddr);

uint8_t* get_p_blk_state(struct rewrite_meta* meta, struct nodegeoaddr* geoaddr);

int init_rewrite_meta(struct fox_node* node, struct rewrite_meta* meta);

int free_rewrite_meta(struct rewrite_meta* meta);

int set_nodegeoaddr(struct fox_node* node, struct nodegeoaddr* baddr, uint64_t lbyte_addr);

int rw_inside_page(struct fox_node* node, struct fox_blkbuf* blockbuf, uint8_t* databuf, struct rewrite_meta* meta, struct nodegeoaddr* geoaddr, uint64_t size, int mode);

int erase_block(struct fox_node* node, struct rewrite_meta* meta, struct nodegeoaddr* geoaddr);

int write_meta_stats(struct rewrite_meta* meta);

#endif

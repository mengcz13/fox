#ifndef FOX_REWRITE_H
#define FOX_REWRITE_H

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

struct rewrite_meta {
    struct fox_node* node;
    uint8_t* page_state; // state of all pages in node
    uint8_t* blk_state; // state of all blocks in node
    uint8_t* temp_page_state_inblk; // temporarily store page state in one block, used for collecting dirty pages in one block before erase it, useful for rewriting dirty pages
    uint8_t* begin_pagebuf;
    uint8_t* end_pagebuf;
    uint8_t* pagebuf;
};

uint64_t geoaddr2vpg(struct fox_node* node, struct nodegeoaddr* geoaddr);

uint64_t geoaddr2vblk(struct fox_node* node, struct nodegeoaddr* geoaddr);

struct nodegeoaddr vpg2geoaddr(struct fox_node* node, uint64_t vpg_i);

struct nodegeoaddr vblk2geoaddr(struct fox_node* node, uint64_t vblk_i);

uint8_t* get_p_page_state(struct rewrite_meta* meta, struct nodegeoaddr* geoaddr);

uint8_t* get_p_blk_state(struct rewrite_meta* meta, struct nodegeoaddr* geoaddr);

int init_rewrite_meta(struct fox_node* node, struct rewrite_meta* meta);

int free_rewrite_meta(struct rewrite_meta* meta);

int set_nodegeoaddr(struct fox_node* node, struct nodegeoaddr* baddr, uint64_t lbyte_addr);

int iterate_io(struct fox_node* node, struct fox_blkbuf* buf, struct rewrite_meta* meta, uint8_t* resbuf, uint64_t offset, uint64_t size, int mode);

int rw_inside_page(struct fox_node* node, struct fox_blkbuf* blockbuf, uint8_t* databuf, struct rewrite_meta* meta, struct nodegeoaddr* geoaddr, uint64_t size, int mode);

int read_block(struct fox_node* node, struct fox_blkbuf* buf, struct nodegeoaddr* geoaddr);

int read_page(struct fox_node* node, struct fox_blkbuf* buf, struct nodegeoaddr* geoaddr);

int erase_block(struct fox_node* node, struct rewrite_meta* meta, struct nodegeoaddr* geoaddr);

#endif
